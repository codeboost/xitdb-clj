(ns xitdb.array-list
  (:require
    [xitdb.common :as common]
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

  (cons [_ o]
    (throw (UnsupportedOperationException. "XITDBArrayList is read-only")))

  (empty [_]
    (throw (UnsupportedOperationException. "XITDBArrayList is read-only")))

  (equiv [this other]
    (and (sequential? other)
         (= (count this) (count other))
         (every? identity (map = this other))))

  clojure.lang.Sequential  ;; Add this to mark as sequential

  clojure.lang.IPersistentVector
  (assocN [this i val]
    (throw (UnsupportedOperationException. "XITDBArrayList is read-only")))

  (length [this]
    (.count ral))

  clojure.lang.Indexed
  (nth [_ i]
    (let [cursor (.getCursor ral (long i))]
      (common/-read-from-cursor cursor)))

  (nth [_ i not-found]
    (try
      (let [cursor (.getCursor ral (long i))]
        (if cursor
          (common/-read-from-cursor cursor)
          not-found))
      (catch Exception _
        not-found)))

  clojure.lang.ILookup
  (valAt [this k]
    (if (number? k)
      (.nth this (long k))
      (throw (IllegalArgumentException. "Key must be a number"))))

  (valAt [this k not-found]
    (if (number? k)
      (.nth this (long k) not-found)
      not-found))

  clojure.lang.IFn
  (invoke [this k]
    (.valAt this k))

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

  java.util.Collection
  (^objects toArray [this]
    (to-array (into [] this)))

  (^objects toArray [this ^objects array]
    (let [len (count this)
          ^objects result (if (or (nil? array) (< (alength array) len))
                            (make-array Object len)
                            array)]
      (dotimes [i len]
        (aset result i (nth this i)))
      (when (> (alength result) len)
        (aset result len nil))
      result))

  common/IUnwrap
  (-unwrap [this]
    ral)

  Object
  (toString [this]
    (pr-str (into [] this))))

(defmethod print-method XITDBArrayList [o ^java.io.Writer w]
  (.write w "#XITDBArrayList")
  (print-method (into [] o) w))

(extend-protocol common/IMaterialize
  XITDBArrayList
  (-materialize [this]
    (reduce (fn [a v]
              (conj a (common/materialize v))) [] (seq this))))

;;-----------------------------------------------

(deftype XITDBWriteArrayList [^WriteArrayList wal]
  clojure.lang.IPersistentCollection
  (count [this]
    (.count wal))

  (cons [this o]
    ;;TODO: Figure out if it is correct to append to the end
    (operations/array-list-append-value! wal (common/unwrap o))
    this)

  (empty [this]
    (operations/array-list-empty! wal)
    this)

  (equiv [this other]
    (if (instance? XITDBWriteArrayList other)
      (and (= (count this) (count other))
           (every? (fn [i] (= (get this i) (get other i)))
                   (range (count this))))
      false))

  clojure.lang.Indexed
  (nth [this i]
    (.nth this i nil))

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
    (.nth this k not-found))

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
  (toString [this]
    (str "XITDBWriteArrayList")))

;; Constructors

(defn xwrite-array-list [^WriteCursor write-cursor]
  (->XITDBWriteArrayList (WriteArrayList. write-cursor)))

(defn xarray-list [^ReadCursor cursor]
  (->XITDBArrayList (ReadArrayList. cursor)))