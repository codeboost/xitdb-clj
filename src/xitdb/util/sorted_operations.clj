(ns xitdb.util.sorted-operations
  "Bridges the XITDBSorted* wrapper types to the Java Read/WriteSortedMap.
  Keys are encoded/decoded for the file's mode (`conversion/db-sorted-key-encode`
  and friends) rather than hashed, so the real key is recoverable on read."
  (:require
    [xitdb.util.conversion :as conversion]
    [xitdb.util.sorted-key :as sorted-key])
  (:import
    [io.github.radarroark.xitdb Database ReadCursor ReadCursor$KeyValuePairCursor WriteCursor ReadSortedMap WriteSortedMap ReadSortedSet WriteSortedSet]))

(defn smap-item-count
  "O(1) entry count, delegating to the rank-augmented B-tree."
  [^ReadSortedMap rsm]
  (.count rsm))

(defn- decode-key-cursor
  "The key stored at `key-cursor`, decoded for the mode of its database."
  [^ReadCursor key-cursor]
  (conversion/db-sorted-key-decode (.-db key-cursor) (.readBytes key-cursor nil)))

(defn- ^Database smap-db [^ReadSortedMap rsm]
  (.-db (.-cursor rsm)))

(defn- ^Database sset-db [^ReadSortedSet rss]
  (.-db (.-cursor rss)))

(defn- smap-lookup-key
  "Bytes to look `key` up by, or nil when it can never be present: nil itself,
  and non-string keys in a compat file (see `conversion/db-sorted-key-lookup`)."
  ^bytes [^ReadSortedMap rsm key]
  (when (some? key)
    (conversion/db-sorted-key-lookup (smap-db rsm) key)))

(defn- sset-lookup-member
  "Like `smap-lookup-key`, for a sorted set member."
  ^bytes [^ReadSortedSet rss member]
  (when (some? member)
    (conversion/db-sorted-key-lookup (sset-db rss) member)))

(defn smap-comparator
  "The comparator for keys of `rsm`, matching its file's mode."
  ^java.util.Comparator [^ReadSortedMap rsm]
  (conversion/db-sorted-comparator (smap-db rsm)))

(defn sset-comparator
  "The comparator for members of `rss`, matching its file's mode."
  ^java.util.Comparator [^ReadSortedSet rss]
  (conversion/db-sorted-comparator (sset-db rss)))

(defn smap-read-cursor
  "Read cursor for `key`, or nil if absent. nil (not storable as a sorted key)
  is never present, so lookups with it return nil instead of throwing."
  [^ReadSortedMap rsm key]
  (when-let [encoded (smap-lookup-key rsm key)]
    (.getCursor rsm encoded)))

(defn smap-contains-key?
  "Unsupported keys are absent, including when native/Java maps compare
  themselves with a stored map. Storage failures are not encoding failures."
  [^ReadSortedMap rsm key]
  (let [^bytes encoded (try
                         (smap-lookup-key rsm key)
                         (catch IllegalArgumentException _ nil))]
    (and (some? encoded) (some? (.getCursor rsm encoded)))))

(defn ^WriteCursor smap-write-cursor
  "Write cursor for `key`. Creates the key if it doesn't exist, so callers that
  only want to read an existing entry must check `smap-read-cursor` first."
  [^WriteSortedMap wsm key]
  (.putCursor wsm (conversion/db-sorted-key-encode (smap-db wsm) key)))

(defn smap-assoc-value!
  "Encodes `k`, writes value `v` at its cursor. Returns the WriteSortedMap."
  [^WriteSortedMap wsm k v]
  (let [value-cursor (smap-write-cursor wsm k)]
    (.write value-cursor (conversion/v->slot! value-cursor v))
    wsm))

(defn smap-dissoc-key!
  "Removing nil (never present) is a no-op, like Clojure's sorted-map."
  [^WriteSortedMap wsm k]
  (when-let [encoded (smap-lookup-key wsm k)]
    (.remove wsm encoded))
  wsm)

(defn smap-empty!
  "Replaces contents with an empty sorted map, in place. Returns the
  WriteSortedMap. Mirrors `operations/map-empty!`: writes a fresh empty
  SORTED_MAP slot at the cursor so the value stays a sorted map (the
  `key-comparator` makes `v->slot!` accept it as a default-ordered tree)."
  [^WriteSortedMap wsm]
  (let [^WriteCursor cursor (.-cursor wsm)]
    (.write cursor (conversion/v->slot! cursor (sorted-map-by sorted-key/key-comparator))))
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
  [^ReadCursor$KeyValuePairCursor kv read-from-cursor]
  (clojure.lang.MapEntry.
    (decode-key-cursor (.-keyCursor kv))
    (read-from-cursor (.-valueCursor kv))))

(declare smap-seq-from-index)

(defn smap-seq-from
  "Lazy ascending seq of MapEntry pairs starting at the first key >= `key`,
  using the engine's native O(log n) lower-bound seek. nil if none. A key the
  file cannot hold (non-string, in a compat file) sorts before every key, so the
  seq starts at the first entry."
  [^ReadSortedMap rsm read-from-cursor key]
  (if-let [encoded (conversion/db-sorted-key-lookup (smap-db rsm) key)]
    (let [it (.iteratorFrom rsm encoded)]
      (when (.hasNext it)
        (letfn [(step []
                  (lazy-seq
                    (when (.hasNext it)
                      (cons (kvpair->entry (.readKeyValuePair (.next it))
                                           read-from-cursor)
                            (step)))))]
          (step))))
    (smap-seq-from-index rsm read-from-cursor 0)))

(defn smap-nth
  "MapEntry at rank `index` (negative counts from the end), or `not-found` when
  out of range. O(log n) via the rank-augmented B-tree."
  [^ReadSortedMap rsm read-from-cursor index not-found]
  (let [kv (.getIndexKeyValuePair rsm (long index))]
    (if (nil? kv)
      not-found
      (kvpair->entry kv read-from-cursor))))

(defn smap-rank
  "Number of keys strictly less than `key`. O(log n). A key the file cannot hold
  (non-string, in a compat file) sorts before every key, so its rank is 0."
  [^ReadSortedMap rsm key]
  (if-let [encoded (conversion/db-sorted-key-lookup (smap-db rsm) key)]
    (.rank rsm encoded)
    0))

(defn smap-seq-from-index
  "Lazy ascending seq of MapEntry pairs starting at rank `index` (0-based),
  using the engine's native O(log n) `iteratorFromIndex` seek. nil if none.
  Streams one entry at a time; does not materialise the whole collection."
  [^ReadSortedMap rsm read-from-cursor index]
  (let [it (.iteratorFromIndex rsm (long index))]
    (when (.hasNext it)
      (letfn [(step []
                (lazy-seq
                  (when (.hasNext it)
                    (cons (kvpair->entry (.readKeyValuePair (.next it))
                                         read-from-cursor)
                          (step)))))]
        (step)))))

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

;; ---------------------------------------------------------------------------
;; Sorted SET helpers. A SORTED_SET is a SortedMap with no values: the MEMBER
;; is the key. We decode the member from the key cursor of each entry.
;; ---------------------------------------------------------------------------

(defn sset-item-count
  "O(1) member count."
  [^ReadSortedSet rss]
  (.count rss))

(defn sset-contains?
  "nil (not storable as a sorted member) is never present, so checking for it
  returns false instead of throwing."
  [^ReadSortedSet rss member]
  (if-let [encoded (sset-lookup-member rss member)]
    (.contains rss encoded)
    false))

(defn sset-assoc-value!
  "Adds `member` to the set (no-op if already present). Returns the WriteSortedSet."
  [^WriteSortedSet wss member]
  (.put wss (conversion/db-sorted-key-encode (sset-db wss) member))
  wss)

(defn sset-disj-value!
  "Removes `member` from the set (no-op if absent, including nil, which is
  never present). Returns the WriteSortedSet."
  [^WriteSortedSet wss member]
  (when-let [encoded (sset-lookup-member wss member)]
    (.remove wss encoded))
  wss)

(defn sset-empty!
  "Replaces contents with an empty sorted set, in place. Returns the
  WriteSortedSet. Mirrors `operations/set-empty!`: writes a fresh empty
  SORTED_SET slot at the cursor so the value stays a sorted set."
  [^WriteSortedSet wss]
  (let [^WriteCursor cursor (.-cursor wss)]
    (.write cursor (conversion/v->slot! cursor (sorted-set-by sorted-key/key-comparator))))
  wss)

(defn- member-from-cursor
  "Decodes the member from a set entry cursor (its key cursor)."
  [^ReadCursor cursor]
  (decode-key-cursor (.-keyCursor (.readKeyValuePair cursor))))

(defn- kvpair->member
  "Decodes the member from a Java KeyValuePair (its key cursor)."
  [^ReadCursor$KeyValuePairCursor kv]
  (decode-key-cursor (.-keyCursor kv)))

(defn sset-seq
  "Lazy ascending seq of members, or nil if empty."
  [^ReadSortedSet rss]
  (let [it (.iterator rss)]
    (when (.hasNext it)
      (letfn [(step []
                (lazy-seq
                  (when (.hasNext it)
                    (cons (member-from-cursor (.next it)) (step)))))]
        (step)))))

(declare sset-seq-from-index)

(defn sset-seq-from
  "Lazy ascending seq of members starting at the first member >= `member`,
  using the engine's native O(log n) lower-bound seek. nil if none. A member the
  file cannot hold (non-string, in a compat file) sorts before every member, so
  the seq starts at the first member."
  [^ReadSortedSet rss member]
  (if-let [encoded (conversion/db-sorted-key-lookup (sset-db rss) member)]
    (let [it (.iteratorFrom rss encoded)]
      (when (.hasNext it)
        (letfn [(step []
                  (lazy-seq
                    (when (.hasNext it)
                      (cons (member-from-cursor (.next it)) (step)))))]
          (step))))
    (sset-seq-from-index rss 0)))

(defn sset-nth
  "Member at rank `index` (negative counts from the end), or `not-found` when
  out of range. O(log n)."
  [^ReadSortedSet rss index not-found]
  (let [kv (.getIndexKeyValuePair rss (long index))]
    (if (nil? kv)
      not-found
      (kvpair->member kv))))

(defn sset-rank
  "Number of members strictly less than `member`. O(log n). A member the file
  cannot hold (non-string, in a compat file) sorts before every member, so its
  rank is 0."
  [^ReadSortedSet rss member]
  (if-let [encoded (conversion/db-sorted-key-lookup (sset-db rss) member)]
    (.rank rss encoded)
    0))

(defn sset-seq-from-index
  "Lazy ascending seq of members starting at rank `index` (0-based), using the
  engine's native O(log n) `iteratorFromIndex` seek. nil if none. Streams one
  member at a time; does not materialise the whole collection."
  [^ReadSortedSet rss index]
  (let [it (.iteratorFromIndex rss (long index))]
    (when (.hasNext it)
      (letfn [(step []
                (lazy-seq
                  (when (.hasNext it)
                    (cons (member-from-cursor (.next it)) (step)))))]
        (step)))))

(defn sset-rseq
  "Lazy descending seq of members, walking `getIndexKeyValuePair` from index
  `start` down to 0. Low-memory (one member at a time)."
  ([^ReadSortedSet rss]
   (sset-rseq rss (dec (.count rss))))
  ([^ReadSortedSet rss start]
   (when (>= start 0)
     (letfn [(step [i]
               (lazy-seq
                 (when (>= i 0)
                   (let [kv (.getIndexKeyValuePair rss (long i))]
                     (when kv
                       (cons (kvpair->member kv) (step (dec i))))))))]
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
