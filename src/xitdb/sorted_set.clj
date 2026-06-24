(ns xitdb.sorted-set
  "On-disk sorted set wrapper types, modelled on `xitdb.hash-set` (set shape) and
  `xitdb.sorted-map` (the `Sorted`/`Indexed`/`Reversible` machinery).

  A `SORTED_SET` is a `SORTED_MAP` with no values: each member is its own key.
  `XITDBSortedSet` is the read view; `XITDBWriteSortedSet` is the mutable view
  used inside a transaction. Ordering is by the engine's unsigned byte
  comparison over order-preserving encoded members (see `xitdb.util.sorted-key`)."
  (:require
    [xitdb.common :as common]
    [xitdb.util.sorted-key :as sorted-key]
    [xitdb.util.sorted-operations :as sorted-ops])
  (:import
    [io.github.radarroark.xitdb
     ReadCursor ReadSortedSet WriteCursor WriteSortedSet]))

(defn- descending-start-index
  "Index to begin a descending walk for `seqFrom(member, false)`: the largest
  rank whose member is <= `member`. Uses `rank` (count of members strictly <
  member); if `member` itself is present, include it."
  [^ReadSortedSet rss member]
  (let [r (sorted-ops/sset-rank rss member)]
    (if (sorted-ops/sset-contains? rss member)
      r
      (dec r))))

(deftype XITDBSortedSet [^ReadSortedSet rss]

  clojure.lang.IPersistentSet
  (disjoin [this k]
    (disj (common/-materialize-shallow this) k))

  (contains [this k]
    (sorted-ops/sset-contains? rss k))

  (get [this k]
    (when (.contains this k)
      k))

  clojure.lang.IPersistentCollection
  (cons [this o]
    (. clojure.lang.RT (conj (common/-materialize-shallow this) o)))

  (empty [this]
    (sorted-set-by sorted-key/key-comparator))

  (equiv [this other]
    (and (instance? clojure.lang.IPersistentSet other)
         (= (count this) (count other))
         (every? #(.contains this %) other)))

  (count [_]
    (sorted-ops/sset-item-count rss))

  clojure.lang.Seqable
  (seq [_]
    (sorted-ops/sset-seq rss))

  clojure.lang.Sorted
  (comparator [_]
    sorted-key/key-comparator)

  (entryKey [_ entry]
    entry)

  (seq [_ ascending?]
    (if ascending?
      (sorted-ops/sset-seq rss)
      (sorted-ops/sset-rseq rss)))

  (seqFrom [_ member ascending?]
    (if ascending?
      (sorted-ops/sset-seq-from rss member)
      (sorted-ops/sset-rseq rss (descending-start-index rss member))))

  clojure.lang.Indexed
  (nth [this i]
    (let [e (.nth this i ::not-found)]
      (if (identical? e ::not-found)
        (throw (IndexOutOfBoundsException.))
        e)))

  (nth [_ i not-found]
    (sorted-ops/sset-nth rss i not-found))

  clojure.lang.Reversible
  (rseq [_]
    (sorted-ops/sset-rseq rss))

  clojure.lang.ILookup
  (valAt [this k]
    (.valAt this k nil))

  (valAt [this k not-found]
    (if (.contains this k)
      k
      not-found))

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
          (throw (UnsupportedOperationException. "XITDBSortedSet iterator is read-only"))))))

  common/ISlot
  (-slot [this]
    (-> rss .cursor .slot))

  common/IUnwrap
  (-unwrap [this]
    rss)

  common/IMaterialize
  (-materialize [this]
    (into (sorted-set-by sorted-key/key-comparator) (map common/materialize (seq this))))

  common/IMaterializeShallow
  (-materialize-shallow [this]
    (into (sorted-set-by sorted-key/key-comparator) (seq this)))

  Object
  (toString [this]
    (str (into (sorted-set-by sorted-key/key-comparator) this))))

(defmethod print-method XITDBSortedSet [o ^java.io.Writer w]
  (.write w "#XITDBSortedSet")
  (print-method (into (sorted-set-by sorted-key/key-comparator) o) w))

;---------------------------------------------------

(deftype XITDBWriteSortedSet [^WriteSortedSet wss]
  clojure.lang.IPersistentSet
  (disjoin [this v]
    (sorted-ops/sset-disj-value! wss (common/unwrap v))
    this)

  (contains [this v]
    (sorted-ops/sset-contains? wss (common/unwrap v)))

  (get [this k]
    (when (.contains this (common/unwrap k))
      k))

  clojure.lang.IPersistentCollection
  (cons [this o]
    (sorted-ops/sset-assoc-value! wss (common/unwrap o))
    this)

  (empty [this]
    (sorted-ops/sset-empty! wss)
    this)

  (equiv [this other]
    (and (instance? clojure.lang.IPersistentSet other)
         (= (count this) (count other))
         (every? #(.contains this %) other)))

  (count [_]
    (sorted-ops/sset-item-count wss))

  clojure.lang.Seqable
  (seq [_]
    (sorted-ops/sset-seq wss))

  clojure.lang.ILookup
  (valAt [this k]
    (.valAt this k nil))

  (valAt [this k not-found]
    (if (.contains this k)
      k
      not-found))

  common/ISlot
  (-slot [_]
    (-> wss .cursor .slot))

  common/IUnwrap
  (-unwrap [_]
    wss)

  common/IReadOnly
  (-read-only [this]
    (XITDBSortedSet. wss))

  Object
  (toString [_]
    (str "XITDBWriteSortedSet")))

(defmethod print-method XITDBWriteSortedSet [o ^java.io.Writer w]
  (.write w "#XITDBWriteSortedSet")
  (print-method (into (sorted-set-by sorted-key/key-comparator) (common/-read-only o)) w))

;; Constructor functions
(defn xwrite-sorted-set [^WriteCursor write-cursor]
  (->XITDBWriteSortedSet (WriteSortedSet. write-cursor)))

(defn xsorted-set [^ReadCursor read-cursor]
  (->XITDBSortedSet (ReadSortedSet. read-cursor)))
