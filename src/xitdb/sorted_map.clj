(ns xitdb.sorted-map
  "On-disk sorted map wrapper types, modelled on `xitdb.hash-map`.

  `XITDBSortedMap` is the read view; `XITDBWriteSortedMap` is the mutable view
  used inside a transaction. Ordering is by the engine's unsigned byte
  comparison over order-preserving encoded keys (see `xitdb.util.sorted-key`).

  The `clojure.lang.Sorted`/`Indexed`/`Reversible` protocols (subseq, nth, rseq)
  are added in a later slice; this slice provides ascending ordered `seq` only."
  (:require
    [xitdb.common :as common]
    [xitdb.util.conversion :as conversion]
    [xitdb.util.sorted-operations :as sorted-ops])
  (:import
    [io.github.radarroark.xitdb
     ReadCursor ReadSortedMap WriteCursor WriteSortedMap]))

(defn smap-seq [rsm]
  (sorted-ops/smap-seq rsm common/-read-from-cursor))

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
    (sorted-map))

  (equiv [this other]
    (and (instance? clojure.lang.IPersistentMap other)
         (= (into {} this) (into {} other))))

  clojure.lang.Seqable
  (seq [_]
    (smap-seq rsm))

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
              (assoc m k (common/materialize v))) (sorted-map) (seq this)))

  common/IMaterializeShallow
  (-materialize-shallow [this]
    (reduce (fn [m [k v]]
              (assoc m k v)) (sorted-map) (seq this)))

  Object
  (toString [this]
    (str (into (sorted-map) this))))

(defmethod print-method XITDBSortedMap [o ^java.io.Writer w]
  (.write w "#XITDBSortedMap")
  (print-method (into (sorted-map) o) w))

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
    (let [cursor (sorted-ops/smap-read-cursor wsm key)]
      (if (nil? cursor)
        not-found
        (common/-read-from-cursor cursor))))

  clojure.lang.Seqable
  (seq [_]
    (smap-seq wsm))

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
  (print-method (into (sorted-map) (common/-read-only o)) w))

(defn xwrite-sorted-map [^WriteCursor write-cursor]
  (->XITDBWriteSortedMap (WriteSortedMap. write-cursor)))

(defn xsorted-map [^ReadCursor read-cursor]
  (->XITDBSortedMap (ReadSortedMap. read-cursor)))
