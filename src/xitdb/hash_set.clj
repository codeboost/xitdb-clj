(ns xitdb.hash-set
  (:require
    [xitdb.common :as common]
    [xitdb.util.collection :as collection]
    [xitdb.util.conversion :as conversion]
    [xitdb.util.operations :as operations])
  (:import
    [io.github.radarroark.xitdb
     ReadCountedHashSet ReadCursor ReadHashSet
     WriteCountedHashSet WriteCursor WriteHashSet]))

(defn set-seq
  [rhm]
  "The cursors used must implement the IReadFromCursor protocol."
  (operations/set-seq rhm common/-read-from-cursor))

(deftype XITDBHashSet [^ReadHashSet rhs]
  clojure.lang.IPersistentSet
  (disjoin [this k]
    (disj (common/-materialize-shallow this) k))

  (contains [this k]
    (operations/set-contains? rhs k))

  (get [this k]
    (when (.contains this k)
      k))

  clojure.lang.IPersistentCollection
  (cons [this o]
    (. clojure.lang.RT (conj (common/-materialize-shallow this) o)))

  (empty [this]
    #{})

  (count [_]
    (operations/set-item-count rhs))

  (equiv [this other]
    (clojure.lang.APersistentSet/setEquals this other))

  clojure.lang.IHashEq
  (hasheq [this]
    (collection/unordered-hasheq this))

  java.util.Set
  (size [this]
    (count this))

  (isEmpty [this]
    (zero? (count this)))

  (containsAll [this other]
    (every? #(.contains this %) other))

  (^objects toArray [this]
    (to-array (seq this)))

  (^objects toArray [this ^objects array]
    (.toArray ^java.util.Collection (vec (seq this)) array))

  (add [_ _]
    (collection/unsupported-mutation!))

  (remove [_ _]
    (collection/unsupported-mutation!))

  (addAll [_ _]
    (collection/unsupported-mutation!))

  (retainAll [_ _]
    (collection/unsupported-mutation!))

  (removeAll [_ _]
    (collection/unsupported-mutation!))

  (clear [_]
    (collection/unsupported-mutation!))

  clojure.lang.Seqable
  (seq [this]
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

  common/ISlot
  (-slot [this]
    (-> rhs .cursor .slot))

  common/IUnwrap
  (-unwrap [_]
    rhs)

  common/IMaterialize
  (-materialize [this]
    (into #{} (map common/materialize (seq this))))

  common/IMaterializeShallow
  (-materialize-shallow [this]
    (into #{} (seq this)))

  Object
  (equals [this other]
    (clojure.lang.APersistentSet/setEquals this other))

  (hashCode [this]
    (collection/set-hash this))

  (toString [this]
    (str (into #{} this))))

(defmethod print-method XITDBHashSet [o ^java.io.Writer w]
  (.write w "#XITDBHashSet")
  (print-method (into #{} o) w))

;; Writable version of the set
(deftype XITDBWriteHashSet [^WriteHashSet whs]
  clojure.lang.IPersistentSet
  (disjoin [this v]
    (operations/set-disj-value! whs v)
    this)

  (contains [this v]
    (operations/set-contains? whs v))

  (get [this k]
    (when (.contains this k)
      k))

  clojure.lang.IPersistentCollection
  (cons [this o]
    (operations/set-assoc-value! whs o)
    this)

  (empty [this]
    (operations/set-empty! whs)
    this)

  (count [_]
    (operations/set-item-count whs))

  (equiv [this other]
    (clojure.lang.APersistentSet/setEquals this other))

  clojure.lang.IHashEq
  (hasheq [this]
    (collection/unordered-hasheq this))

  java.util.Set
  (size [this]
    (count this))

  (isEmpty [this]
    (zero? (count this)))

  (containsAll [this other]
    (every? #(.contains this %) other))

  (^objects toArray [this]
    (to-array (seq this)))

  (^objects toArray [this ^objects array]
    (.toArray ^java.util.Collection (vec (seq this)) array))

  (add [_ _]
    (collection/unsupported-mutation!))

  (remove [_ _]
    (collection/unsupported-mutation!))

  (addAll [_ _]
    (collection/unsupported-mutation!))

  (retainAll [_ _]
    (collection/unsupported-mutation!))

  (removeAll [_ _]
    (collection/unsupported-mutation!))

  (clear [_]
    (collection/unsupported-mutation!))

  java.lang.Iterable
  (iterator [this]
    (clojure.lang.SeqIterator. (seq this)))

  clojure.lang.Seqable
  (seq [this]
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
  (equals [this other]
    (clojure.lang.APersistentSet/setEquals this other))

  (hashCode [this]
    (collection/set-hash this))

  (toString [_]
    (str "XITDBWriteHashSet")))

(defmethod print-method XITDBWriteHashSet [o ^java.io.Writer w]
  (.write w "#XITDBWriteHashSet")
  (print-method (into #{} (seq o)) w))

;; Constructor functions
(defn xwrite-hash-set [^WriteCursor write-cursor]
  (->XITDBWriteHashSet (WriteHashSet. write-cursor)))

(defn xhash-set [^ReadCursor read-cursor]
  (->XITDBHashSet (ReadHashSet. read-cursor)))

(defn xwrite-hash-set-counted [^WriteCursor write-cursor]
  (->XITDBWriteHashSet (WriteCountedHashSet. write-cursor)))

(defn xhash-set-counted [^ReadCursor cursor]
  (->XITDBHashSet (ReadCountedHashSet. cursor)))
