(ns xitdb.hash-set
  (:require
    [xitdb.common :as common]
    [xitdb.util.conversion :as conversion]
    [xitdb.util.operations :as operations])
  (:import
    [io.github.radarroark.xitdb ReadHashMap WriteCursor WriteHashMap]))

(defn set-seq
  [rhm]
  "The cursors used must implement the IReadFromCursor protocol."
  (map val (operations/map-seq rhm #(common/-read-from-cursor %))))

(deftype XITDBHashSet [^ReadHashMap rhm]
  clojure.lang.IPersistentSet
  (disjoin [_ k]
    (throw (UnsupportedOperationException. "XITDBHashSet is read-only")))

  (contains [this k]
    (not (nil? (.getCursor rhm (conversion/db-key (if (nil? k) 0 (.hashCode k)))))))

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
    (operations/map-item-count rhm))

  clojure.lang.Seqable
  (seq [_]
    (set-seq rhm))

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
    rhm)

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
(deftype XITDBWriteHashSet [^WriteHashMap whm]
  clojure.lang.IPersistentSet
  (disjoin [this k]
    (operations/map-dissoc-key! whm (.hashCode k))
    this)

  (contains [this k]
    (operations/map-contains-key? whm (.hashCode k)))

  (get [this k]
    (when (.contains this k)
      k))

  clojure.lang.IPersistentCollection
  (cons [this o]
    (operations/set-assoc-value! whm (common/unwrap o))
    this)

  (empty [this]
    (operations/set-empty! whm)
    this)

  (equiv [this other]
    (and (instance? clojure.lang.IPersistentSet other)
         (= (count this) (count other))
         (every? #(.contains this %) other)))

  (count [_]
    (operations/map-item-count whm))

  clojure.lang.Seqable
  (seq [_]
    (set-seq whm))

  clojure.lang.ILookup
  (valAt [this k]
    (.valAt this k nil))

  (valAt [this k not-found]
    (if (.contains this k)
      k
      not-found))

  common/ISlot
  (-slot [_]
    (-> whm .cursor .slot))

  common/IUnwrap
  (-unwrap [_]
    whm)

  Object
  (toString [_]
    (str "XITDBWriteHashSet")))

;; Constructor functions
(defn xwrite-hash-set [^WriteCursor write-cursor]
  (let [whm (operations/init-hash-set! write-cursor)]
    (->XITDBWriteHashSet whm)))

(defn xhash-set [^ReadHashMap read-cursor]
  (->XITDBHashSet (ReadHashMap. read-cursor)))
