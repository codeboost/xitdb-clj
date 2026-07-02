(ns xitdb.sorted-map
  "On-disk sorted map wrapper types, modelled on `xitdb.hash-map`.

  `XITDBSortedMap` is the read view; `XITDBWriteSortedMap` is the mutable view
  used inside a transaction. Ordering is by the engine's unsigned byte
  comparison over order-preserving encoded keys (see `xitdb.util.sorted-key`).

  Both views implement `clojure.lang.Sorted`/`Indexed`/`Reversible` (subseq,
  nth, rseq) on top of the rank-augmented B-tree, in addition to ascending
  ordered `seq`."
  (:require
    [xitdb.common :as common]
    [xitdb.util.sorted-key :as sorted-key]
    [xitdb.util.sorted-operations :as sorted-ops])
  (:import
    [io.github.radarroark.xitdb
     ReadCursor ReadSortedMap WriteCursor WriteSortedMap]))

(defn smap-seq [rsm]
  (sorted-ops/smap-seq rsm common/-read-from-cursor))

(defn- descending-start-index
  "Index to begin a descending walk for `seqFrom(key, false)`: the largest rank
  whose key is <= `key`. Uses `rank` (count of keys strictly < key); if `key`
  itself is present, include it."
  [^ReadSortedMap rsm key]
  (let [r (sorted-ops/smap-rank rsm key)]
    (if (sorted-ops/smap-contains-key? rsm key)
      r
      (dec r))))

(deftype XITDBSortedMap [^ReadSortedMap rsm]

  clojure.lang.ILookup
  (valAt [this key]
    (.valAt this key nil))

  (valAt [this key not-found]
    (let [cursor (sorted-ops/smap-read-cursor rsm key)]
      (if (nil? cursor)
        not-found
        (common/-read-from-cursor cursor))))

  clojure.lang.Associative
  (containsKey [this key]
    (sorted-ops/smap-contains-key? rsm key))

  (entryAt [this key]
    (when (.containsKey this key)
      (clojure.lang.MapEntry. key (.valAt this key nil))))

  (assoc [this k v]
    (assoc (common/-materialize-shallow this) k v))

  clojure.lang.IPersistentMap
  (without [this k]
    (dissoc (common/-materialize-shallow this) k))

  (count [this]
    (sorted-ops/smap-item-count rsm))

  clojure.lang.IPersistentCollection
  (cons [this o]
    (. clojure.lang.RT (conj (common/-materialize-shallow this) o)))

  (empty [this]
    (sorted-map-by sorted-key/key-comparator))

  (equiv [this other]
    (and (instance? clojure.lang.IPersistentMap other)
         (= (into {} this) (into {} other))))

  clojure.lang.Seqable
  (seq [_]
    (smap-seq rsm))

  clojure.lang.Sorted
  (comparator [_]
    sorted-key/key-comparator)

  (entryKey [_ entry]
    (key entry))

  (seq [_ ascending?]
    (if ascending?
      (smap-seq rsm)
      (sorted-ops/smap-rseq rsm common/-read-from-cursor)))

  (seqFrom [_ key ascending?]
    (if ascending?
      (sorted-ops/smap-seq-from rsm common/-read-from-cursor key)
      (sorted-ops/smap-rseq rsm common/-read-from-cursor
                            (descending-start-index rsm key))))

  clojure.lang.Indexed
  (nth [this i]
    (let [e (.nth this i ::not-found)]
      (if (identical? e ::not-found)
        (throw (IndexOutOfBoundsException.))
        e)))

  (nth [_ i not-found]
    (sorted-ops/smap-nth rsm common/-read-from-cursor i not-found))

  clojure.lang.Reversible
  (rseq [_]
    (sorted-ops/smap-rseq rsm common/-read-from-cursor))

  clojure.lang.IFn
  (invoke [this k]
    (.valAt this k))

  (invoke [this k not-found]
    (.valAt this k not-found))

  java.lang.Iterable
  (iterator [this]
    (let [iter (clojure.lang.SeqIterator. (seq this))]
      (reify java.util.Iterator
        (hasNext [_]
          (.hasNext iter))
        (next [_]
          (.next iter))
        (remove [_]
          (throw (UnsupportedOperationException. "XITDBSortedMap iterator is read-only"))))))

  clojure.core.protocols/IKVReduce
  (kv-reduce [this f init]
    (sorted-ops/smap-kv-reduce rsm common/-read-from-cursor f init))

  common/ISlot
  (-slot [this]
    (-> rsm .cursor .slot))

  common/IUnwrap
  (-unwrap [this]
    rsm)

  common/IMaterialize
  (-materialize [this]
    (reduce (fn [m [k v]]
              (assoc m k (common/materialize v))) (sorted-map-by sorted-key/key-comparator) (seq this)))

  common/IMaterializeShallow
  (-materialize-shallow [this]
    (reduce (fn [m [k v]]
              (assoc m k v)) (sorted-map-by sorted-key/key-comparator) (seq this)))

  Object
  (toString [this]
    (str (into (sorted-map-by sorted-key/key-comparator) this))))

(defmethod print-method XITDBSortedMap [o ^java.io.Writer w]
  (.write w "#XITDBSortedMap")
  (print-method (into (sorted-map-by sorted-key/key-comparator) o) w))

;---------------------------------------------------

(deftype XITDBWriteSortedMap [^WriteSortedMap wsm]
  clojure.lang.IPersistentCollection
  (cons [this o]
    (cond
      (instance? clojure.lang.MapEntry o)
      (.assoc this (key o) (val o))

      (map? o)
      (doseq [[k v] (seq o)]
        (.assoc this k v))

      (and (sequential? o) (= 2 (count o)))
      (.assoc this (first o) (second o))

      :else
      (throw (IllegalArgumentException. "Can only cons MapEntries or key-value pairs onto maps")))
    this)

  (empty [this]
    (sorted-ops/smap-empty! wsm)
    this)

  (equiv [this other]
    (and (= (count this) (count other))
         (every? (fn [[k v]] (= v (get other k ::not-found)))
                 (seq this))))

  clojure.lang.Associative
  (assoc [this k v]
    (sorted-ops/smap-assoc-value! wsm k (common/unwrap v))
    this)

  (containsKey [this key]
    (sorted-ops/smap-contains-key? wsm key))

  (entryAt [this key]
    (when (.containsKey this key)
      (clojure.lang.MapEntry. key (.valAt this key nil))))

  clojure.lang.IPersistentMap
  (without [this key]
    (sorted-ops/smap-dissoc-key! wsm key)
    this)

  (count [this]
    (sorted-ops/smap-item-count wsm))

  clojure.lang.ILookup
  (valAt [this key]
    (.valAt this key nil))

  (valAt [this key not-found]
    ;; Read through a write cursor (like XITDBWriteHashMap) so nested
    ;; collections come back as writable views; the read-cursor probe keeps
    ;; `putCursor` from creating absent keys.
    (let [cursor (sorted-ops/smap-read-cursor wsm key)]
      (if (nil? cursor)
        not-found
        (common/-read-from-cursor (sorted-ops/smap-write-cursor wsm key)))))

  clojure.lang.Seqable
  (seq [_]
    (smap-seq wsm))

  ;; The same ordered read machinery as XITDBSortedMap, reading the
  ;; in-transaction (uncommitted) state. `WriteSortedMap` is a `ReadSortedMap`
  ;; subclass, so the rank/index-based ops apply directly to `wsm`.
  clojure.lang.Sorted
  (comparator [_]
    sorted-key/key-comparator)

  (entryKey [_ entry]
    (key entry))

  (seq [_ ascending?]
    (if ascending?
      (smap-seq wsm)
      (sorted-ops/smap-rseq wsm common/-read-from-cursor)))

  (seqFrom [_ key ascending?]
    (if ascending?
      (sorted-ops/smap-seq-from wsm common/-read-from-cursor key)
      (sorted-ops/smap-rseq wsm common/-read-from-cursor
                            (descending-start-index wsm key))))

  clojure.lang.Indexed
  (nth [this i]
    (let [e (.nth this i ::not-found)]
      (if (identical? e ::not-found)
        (throw (IndexOutOfBoundsException.))
        e)))

  (nth [_ i not-found]
    (sorted-ops/smap-nth wsm common/-read-from-cursor i not-found))

  clojure.lang.Reversible
  (rseq [_]
    (sorted-ops/smap-rseq wsm common/-read-from-cursor))

  clojure.core.protocols/IKVReduce
  (kv-reduce [this f init]
    (sorted-ops/smap-kv-reduce wsm common/-read-from-cursor f init))

  common/ISlot
  (-slot [this]
    (-> wsm .cursor .slot))

  common/IUnwrap
  (-unwrap [this]
    wsm)

  common/IReadOnly
  (-read-only [this]
    (XITDBSortedMap. wsm))

  Object
  (toString [this]
    (str "XITDBWriteSortedMap")))

(defmethod print-method XITDBWriteSortedMap [o ^java.io.Writer w]
  (.write w "#XITDBWriteSortedMap")
  (print-method (into (sorted-map-by sorted-key/key-comparator) (common/-read-only o)) w))

(defn xwrite-sorted-map [^WriteCursor write-cursor]
  (->XITDBWriteSortedMap (WriteSortedMap. write-cursor)))

(defn xsorted-map [^ReadCursor read-cursor]
  (->XITDBSortedMap (ReadSortedMap. read-cursor)))
