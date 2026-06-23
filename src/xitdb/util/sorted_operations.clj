(ns xitdb.util.sorted-operations
  "Bridges the XITDBSorted* wrapper types to the Java Read/WriteSortedMap.
  Keys are encoded/decoded through `xitdb.util.sorted-key` (order-preserving,
  reversible) rather than hashed, so the real key is recoverable on read."
  (:require
    [xitdb.util.conversion :as conversion]
    [xitdb.util.sorted-key :as sorted-key])
  (:import
    [io.github.radarroark.xitdb ReadCursor WriteCursor ReadSortedMap WriteSortedMap]))

(defn smap-item-count
  "O(1) entry count, delegating to the rank-augmented B-tree."
  [^ReadSortedMap rsm]
  (.count rsm))

(defn- decode-key-cursor [^ReadCursor key-cursor]
  (sorted-key/decode-key (.readBytes key-cursor nil)))

(defn smap-read-cursor
  "Read cursor for `key`, or nil if absent."
  [^ReadSortedMap rsm key]
  (.getCursor rsm (sorted-key/encode-key key)))

(defn smap-contains-key?
  [^ReadSortedMap rsm key]
  (some? (smap-read-cursor rsm key)))

(defn smap-assoc-value!
  "Encodes `k`, writes value `v` at its cursor. Returns the WriteSortedMap."
  [^WriteSortedMap wsm k v]
  (let [value-cursor (.putCursor wsm (sorted-key/encode-key k))]
    (.write value-cursor (conversion/v->slot! value-cursor v))
    wsm))

(defn smap-dissoc-key!
  [^WriteSortedMap wsm k]
  (.remove wsm (sorted-key/encode-key k))
  wsm)

(defn smap-empty!
  "Replaces contents with an empty sorted map, in place."
  [^WriteSortedMap wsm]
  (let [^WriteCursor cursor (.-cursor wsm)]
    (.write cursor nil)
    ;; re-init an empty sorted map at the same cursor the wsm holds
    (WriteSortedMap. cursor))
  wsm)

(defn smap-seq
  "Lazy ascending seq of key-value MapEntry pairs, or nil if empty.
  `read-from-cursor` converts a value cursor to a Clojure value."
  [^ReadSortedMap rsm read-from-cursor]
  (let [it (.iterator rsm)]
    (when (.hasNext it)
      (letfn [(step []
                (lazy-seq
                  (when (.hasNext it)
                    (let [cursor (.next it)
                          kv     (.readKeyValuePair cursor)
                          k      (decode-key-cursor (.-keyCursor kv))
                          v      (read-from-cursor (.-valueCursor kv))]
                      (cons (clojure.lang.MapEntry. k v) (step))))))]
        (step)))))

(defn- kvpair->entry
  "Turns a Java KeyValuePair (with .-keyCursor/.-valueCursor) into a Clojure
  MapEntry (decoded key, read value)."
  [kv read-from-cursor]
  (clojure.lang.MapEntry.
    (decode-key-cursor (.-keyCursor kv))
    (read-from-cursor (.-valueCursor kv))))

(defn smap-seq-from
  "Lazy ascending seq of MapEntry pairs starting at the first key >= `key`,
  using the engine's native O(log n) lower-bound seek. nil if none."
  [^ReadSortedMap rsm read-from-cursor key]
  (let [it (.iteratorFrom rsm (sorted-key/encode-key key))]
    (when (.hasNext it)
      (letfn [(step []
                (lazy-seq
                  (when (.hasNext it)
                    (cons (kvpair->entry (.readKeyValuePair (.next it))
                                         read-from-cursor)
                          (step)))))]
        (step)))))

(defn smap-nth
  "MapEntry at rank `index` (negative counts from the end), or `not-found` when
  out of range. O(log n) via the rank-augmented B-tree."
  [^ReadSortedMap rsm read-from-cursor index not-found]
  (let [kv (.getIndexKeyValuePair rsm (long index))]
    (if (nil? kv)
      not-found
      (kvpair->entry kv read-from-cursor))))

(defn smap-rank
  "Number of keys strictly less than `key`. O(log n)."
  [^ReadSortedMap rsm key]
  (.rank rsm (sorted-key/encode-key key)))

(defn smap-rseq
  "Lazy descending seq of MapEntry pairs, walking `getIndexKeyValuePair` from
  index `start` down to 0. Stays low-memory (one entry materialised at a time)."
  ([^ReadSortedMap rsm read-from-cursor]
   (smap-rseq rsm read-from-cursor (dec (.count rsm))))
  ([^ReadSortedMap rsm read-from-cursor start]
   (when (>= start 0)
     (letfn [(step [i]
               (lazy-seq
                 (when (>= i 0)
                   (let [kv (.getIndexKeyValuePair rsm (long i))]
                     (when kv
                       (cons (kvpair->entry kv read-from-cursor) (step (dec i))))))))]
       (step start)))))

(defn smap-kv-reduce
  [^ReadSortedMap rsm read-from-cursor f init]
  (let [it (.iterator rsm)]
    (loop [result init]
      (if (.hasNext it)
        (let [cursor     (.next it)
              kv         (.readKeyValuePair cursor)
              k          (decode-key-cursor (.-keyCursor kv))
              v          (read-from-cursor (.-valueCursor kv))
              new-result (f result k v)]
          (if (reduced? new-result)
            @new-result
            (recur new-result)))
        result))))
