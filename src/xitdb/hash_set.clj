(ns xitdb.hash-set
  (:require
    [xitdb.common :as common]
    [xitdb.util.conversion :as conversion]
    [xitdb.util.operations :as operations])
  (:import
    [io.github.radarroark.xitdb ReadCountedHashSet ReadCursor ReadHashMap ReadHashSet WriteCountedHashSet WriteCursor WriteHashMap WriteHashSet]))

(defn set-seq
  [rhm]
  "The cursors used must implement the IReadFromCursor protocol."
  (operations/set-seq rhm common/-read-from-cursor))

(deftype XITDBHashSet [^ReadHashSet rhs]
  clojure.lang.IPersistentSet
  (disjoin [_ k]
    (throw (UnsupportedOperationException. "XITDBHashSet is read-only")))

  (contains [this k]
    (not (nil? (.getCursor rhs (conversion/db-key-hash (-> rhs .cursor .db) key)))))

  (get [this k]
    (when (.contains this k)
      k))

  clojure.lang.IPersistentCollection
  (cons [_ o]
    (throw (UnsupportedOperationException. "XITDBHashSet is read-only")))

  (empty [_]
    (throw (UnsupportedOperationException. "XITDBHashSet is read-only")))

  (equiv [this other]
    (and (instance? clojure.lang.IPersistentSet other)
         (= (count this) (count other))
         (every? #(.contains this %) other)))

  (count [_]
    (operations/set-item-count rhs))

  clojure.lang.Seqable
  (seq [_]
    (set-seq rhs))

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

  java.lang.Iterable
  (iterator [this]
    (let [iter (clojure.lang.SeqIterator. (seq this))]
      (reify java.util.Iterator
        (hasNext [_]
          (.hasNext iter))
        (next [_]
          (.next iter))
        (remove [_]
          (throw (UnsupportedOperationException. "XITDBHashSet iterator is read-only"))))))

  common/IUnwrap
  (-unwrap [_]
    rhs)

  Object
  (toString [this]
    (str (into #{} this))))

(defmethod print-method XITDBHashSet [o ^java.io.Writer w]
  (.write w "#XITDBHashSet")
  (print-method (into #{} o) w))

(extend-protocol common/IMaterialize
  XITDBHashSet
  (-materialize [this]
    (into #{} (map common/materialize (seq this)))))

;; Writable version of the set
(deftype XITDBWriteHashSet [^WriteHashSet whs]
  clojure.lang.IPersistentSet
  (disjoin [this v]
    (operations/set-disj-value! whs (common/unwrap v))
    this)

  (contains [this v]
    (operations/set-contains? whs (common/unwrap v)))

  (get [this k]
    (when (.contains this (common/unwrap k))
      k))

  clojure.lang.IPersistentCollection
  (cons [this o]
    (operations/set-assoc-value! whs (common/unwrap o))
    this)

  (empty [this]
    (operations/set-empty! whs)
    this)

  (equiv [this other]
    (and (instance? clojure.lang.IPersistentSet other)
         (= (count this) (count other))
         (every? #(.contains this %) other)))

  (count [_]
    (operations/set-item-count whs))

  clojure.lang.Seqable
  (seq [_]
    (set-seq whs))

  clojure.lang.ILookup
  (valAt [this k]
    (.valAt this k nil))

  (valAt [this k not-found]
    (if (.contains this k)
      k
      not-found))

  common/ISlot
  (-slot [_]
    (-> whs .cursor .slot))

  common/IUnwrap
  (-unwrap [_]
    whs)

  Object
  (toString [_]
    (str "XITDBWriteHashSet")))

;; Constructor functions
(defn xwrite-hash-set [^WriteCursor write-cursor]
  (->XITDBWriteHashSet (WriteHashSet. write-cursor)))

(defn xhash-set [^ReadCursor read-cursor]
  (->XITDBHashSet (ReadHashSet. read-cursor)))

(defn xwrite-hash-set-counted [^WriteCursor write-cursor]
  (->XITDBWriteHashSet (WriteCountedHashSet. write-cursor)))

(defn xhash-set-counted [^ReadCursor cursor]
  (->XITDBHashSet (ReadCountedHashSet. cursor)))