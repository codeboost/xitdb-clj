(ns xitdb.array-list
  (:require
    [xitdb.common :as common]
    [xitdb.util.collection :as collection]
    [xitdb.util.operations :as operations])
  (:import
    (io.github.radarroark.xitdb ReadArrayList ReadCursor WriteArrayList WriteCursor)))

(defn array-seq
  [^ReadArrayList ral]
  "The cursors used must implement the IReadFromCursor protocol."
  (operations/array-seq ral #(common/-read-from-cursor %)))

(deftype XITDBArrayList [^ReadArrayList ral]
  clojure.lang.IPersistentCollection
  (seq [_]
    (array-seq ral))

  (count [_]
    (.count ral))

  (cons [this o]
    (. clojure.lang.RT (conj (common/-materialize-shallow this) o)))

  (empty [this]
    [])

  (equiv [this other]
    (collection/sequence-equal? this other true))

  clojure.lang.IHashEq
  (hasheq [this]
    (collection/ordered-hasheq this))

  java.util.List
  (size [this]
    (count this))

  (isEmpty [this]
    (zero? (count this)))

  (contains [this v]
    (.contains (collection/list-view this) v))

  (containsAll [this values]
    (.containsAll (collection/list-view this) values))

  (iterator [this]
    (clojure.lang.SeqIterator. (seq this)))

  (^objects toArray [this]
    (.toArray (collection/list-view this)))

  (^objects toArray [this ^objects array]
    (.toArray (collection/list-view this) array))

  (get [this i]
    (collection/list-get this i))

  (indexOf [this v]
    (.indexOf (collection/list-view this) v))

  (lastIndexOf [this v]
    (.lastIndexOf (collection/list-view this) v))

  (listIterator [this]
    (.listIterator (collection/list-view this)))

  (listIterator [this i]
    (.listIterator (collection/list-view this) i))

  (subList [this from to]
    (.subList (collection/list-view this) from to))

  (add [_ _]
    (collection/unsupported-mutation!))

  (add [_ _ _]
    (collection/unsupported-mutation!))

  (set [_ _ _]
    (collection/unsupported-mutation!))

  (^Object remove [_ ^int _]
    (collection/unsupported-mutation!))

  (^boolean remove [_ ^Object _]
    (collection/unsupported-mutation!))

  (addAll [_ _]
    (collection/unsupported-mutation!))

  (addAll [_ _ _]
    (collection/unsupported-mutation!))

  (removeAll [_ _]
    (collection/unsupported-mutation!))

  (retainAll [_ _]
    (collection/unsupported-mutation!))

  (clear [_]
    (collection/unsupported-mutation!))

  clojure.lang.Sequential

  clojure.lang.Associative
  (assoc [this k v]
    (assoc (common/-materialize-shallow this) k v))

  (containsKey [this k]
    (and (integer? k) (>= k 0) (< k (.count ral))))

  (entryAt [this k]
    (when (.containsKey this k)
      (clojure.lang.MapEntry. k (.valAt this k))))

  clojure.lang.IPersistentVector
  (assocN [this i val]
    (assoc (common/-materialize-shallow this) i val))

  (length [this]
    (.count ral))

  clojure.lang.Indexed
  (nth [_ i]
    (let [count (.count ral)
          idx   (long i)]
      (if (and (>= idx 0) (< idx count))
        (when-let [cursor (.getCursor ral idx)]
          (common/-read-from-cursor cursor))
        (throw (IndexOutOfBoundsException. (str "Index: " i ", Size: " count))))))

  (nth [_ i not-found]
    (if (and (>= i 0) (< i (.count ral)))
      (when-let [cursor (.getCursor ral (long i))]
        (common/-read-from-cursor cursor))
      not-found))

  clojure.lang.ILookup
  (valAt [this k]
    (.valAt this k nil))

  (valAt [this k not-found]
    (if (.containsKey this k)
      (.nth this (int k) not-found)
      not-found))

  clojure.lang.IFn
  (invoke [this k]
    (when-not (integer? k)
      (throw (IllegalArgumentException. "Key must be integer")))
    (if (.containsKey this k)
      (.nth this (int k))
      (throw (IndexOutOfBoundsException. (str "Index: " k ", Size: " (.count ral))))))

  (invoke [this k not-found]
    (.valAt this k not-found))

  (applyTo [this args]
    (case (count args)
      1 (.invoke this (first args))
      2 (.invoke this (first args) (second args))
      (throw (IllegalArgumentException. "Wrong number of args passed to XITDBArrayList"))))

  clojure.lang.IReduce
  (reduce [this f]
    (let [s (seq this)]
      (if s
        (reduce f (first s) (rest s))
        (f))))

  clojure.lang.IReduceInit
  (reduce [this f init]
    (reduce f init (array-seq ral)))

  clojure.core.protocols/IKVReduce
  (kv-reduce [this f init]
    (operations/array-kv-reduce ral #(common/-read-from-cursor %) f init))

  common/ISlot
  (-slot [this]
    (-> ral .cursor .slot))

  common/IUnwrap
  (-unwrap [this]
    ral)

  common/IMaterialize
  (-materialize [this]
    (reduce (fn [a v]
              (conj a (common/materialize v))) [] (seq this)))

  common/IMaterializeShallow
  (-materialize-shallow [this]
    (reduce (fn [a v]
              (conj a v)) [] (seq this)))

  Object
  (equals [this other]
    (collection/sequence-equal? this other false))

  (hashCode [this]
    (collection/ordered-hash this))

  (toString [this]
    (pr-str (into [] this))))

(defmethod print-method XITDBArrayList [o ^java.io.Writer w]
  (.write w "#XITDBArrayList")
  (print-method (into [] o) w))

;;-----------------------------------------------

(deftype XITDBWriteArrayList [^WriteArrayList wal]
  clojure.lang.IPersistentCollection
  (count [this]
    (.count wal))

  (cons [this o]
    (operations/array-list-append-value! wal (common/unwrap o))
    this)

  (empty [this]
    (operations/array-list-empty! wal)
    this)

  (equiv [this other]
    (collection/sequence-equal? this other true))

  clojure.lang.IHashEq
  (hasheq [this]
    (collection/ordered-hasheq this))

  java.util.List
  (size [this]
    (count this))

  (isEmpty [this]
    (zero? (count this)))

  (contains [this v]
    (.contains (collection/list-view this) v))

  (containsAll [this values]
    (.containsAll (collection/list-view this) values))

  (iterator [this]
    (clojure.lang.SeqIterator. (seq this)))

  (^objects toArray [this]
    (.toArray (collection/list-view this)))

  (^objects toArray [this ^objects array]
    (.toArray (collection/list-view this) array))

  (get [this i]
    (collection/list-get this i))

  (indexOf [this v]
    (.indexOf (collection/list-view this) v))

  (lastIndexOf [this v]
    (.lastIndexOf (collection/list-view this) v))

  (listIterator [this]
    (.listIterator (collection/list-view this)))

  (listIterator [this i]
    (.listIterator (collection/list-view this) i))

  (subList [this from to]
    (.subList (collection/list-view this) from to))

  (add [_ _]
    (collection/unsupported-mutation!))

  (add [_ _ _]
    (collection/unsupported-mutation!))

  (set [_ _ _]
    (collection/unsupported-mutation!))

  (^Object remove [_ ^int _]
    (collection/unsupported-mutation!))

  (^boolean remove [_ ^Object _]
    (collection/unsupported-mutation!))

  (addAll [_ _]
    (collection/unsupported-mutation!))

  (addAll [_ _ _]
    (collection/unsupported-mutation!))

  (removeAll [_ _]
    (collection/unsupported-mutation!))

  (retainAll [_ _]
    (collection/unsupported-mutation!))

  (clear [_]
    (collection/unsupported-mutation!))

  clojure.lang.Indexed
  (nth [this i]
    (if (and (>= i 0) (< i (.count wal)))
      (.nth this i nil)
      (throw (IndexOutOfBoundsException. (str "Index: " i ", Size: " (.count wal))))))

  (nth [this i not-found]
    (if (and (>= i 0) (< i (.count wal)))
      (common/-read-from-cursor (.putCursor wal i))
      not-found))

  clojure.lang.IPersistentVector
  (assocN [this i val]
    (operations/array-list-assoc-value! wal i (common/unwrap val))
    this)

  (length [this]
    (.count wal))

  clojure.lang.Sequential

  clojure.lang.Associative
  (assoc [this k v]
    (when-not (integer? k)
      (throw (IllegalArgumentException. "Key must be integer")))
    (operations/array-list-assoc-value! wal k (common/unwrap v))
    this)

  (containsKey [this k]
    (and (integer? k) (>= k 0) (< k (.count wal))))

  (entryAt [this k]
    (when (.containsKey this k)
      (clojure.lang.MapEntry. k (.valAt this k))))

  clojure.lang.ILookup
  (valAt [this k]
    (.valAt this k nil))

  (valAt [this k not-found]
    (if (.containsKey this k)
      (.nth this (int k) not-found)
      not-found))

  clojure.lang.Seqable
  (seq [this]
    (array-seq wal))

  clojure.core.protocols/IKVReduce
  (kv-reduce [this f init]
    (operations/array-kv-reduce wal #(common/-read-from-cursor %) f init))

  clojure.lang.IObj
  (withMeta [this _]
    this)

  clojure.lang.IMeta
  (meta [this]
    nil)

  clojure.lang.IEditableCollection
  (asTransient [this]
    this)

  clojure.lang.ITransientCollection
  (conj [this val]
    (operations/array-list-append-value! wal (common/unwrap val))
    this)

  (persistent [this]
    this)

  clojure.lang.ITransientVector ;; assoc already implemented

  (pop [this]
    (let [value (common/-read-from-cursor (-> wal .-cursor))]
      (operations/array-list-pop! wal)
      value))

  common/ISlot
  (-slot [this]
    (-> wal .cursor .slot))

  common/IUnwrap
  (-unwrap [this]
    wal)

  Object
  (equals [this other]
    (collection/sequence-equal? this other false))

  (hashCode [this]
    (collection/ordered-hash this))

  (toString [this]
    (str "XITDBWriteArrayList")))

(defmethod print-method XITDBWriteArrayList [o ^java.io.Writer w]
  (.write w "#XITDBWriteArrayList")
  (print-method (into [] (seq o)) w))

;; Constructors

(defn xwrite-array-list [^WriteCursor write-cursor]
  (->XITDBWriteArrayList (WriteArrayList. write-cursor)))

(defn xarray-list [^ReadCursor cursor]
  (->XITDBArrayList (ReadArrayList. cursor)))
