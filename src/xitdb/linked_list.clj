(ns xitdb.linked-list
  (:require
    [xitdb.common :as common]
    [xitdb.util.conversion :as conversion]
    [xitdb.util.operations :as operations])
  (:import
    [io.github.radarroark.xitdb ReadCursor ReadLinkedArrayList WriteCursor WriteLinkedArrayList]))

(defn array-seq
  [^ReadLinkedArrayList rlal]
  "The cursors used must implement the IReadFromCursor protocol."
  (operations/linked-array-seq rlal #(common/-read-from-cursor %)))

(deftype XITDBLinkedArrayList [^ReadLinkedArrayList rlal]
  clojure.lang.IPersistentCollection
  (seq [_]
    (array-seq rlal))

  (count [_]
    (.count rlal))

  (cons [_ o]
    (throw (UnsupportedOperationException. "XITDBLinkedArrayList is read-only")))

  (empty [_]
    (throw (UnsupportedOperationException. "XITDBLinkedArrayList is read-only")))

  (equiv [this other]
    (and (sequential? other)
         (= (count this) (count other))
         (every? identity (map = this other))))

  clojure.lang.Sequential  ;; Mark as sequential

  clojure.lang.Indexed
  (nth [_ i]
    (let [cursor (.getCursor rlal (long i))]
      (common/-read-from-cursor cursor)))

  (nth [_ i not-found]
    (try
      (let [cursor (.getCursor rlal (long i))]
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
      (throw (IllegalArgumentException. "Wrong number of args passed to XITDBLinkedArrayList"))))

  clojure.lang.IReduceInit
  (reduce [this f init]
    (reduce f init (array-seq rlal)))

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

  Object
  (toString [this]
    (pr-str (into [] this))))

(defmethod print-method XITDBLinkedArrayList [o ^java.io.Writer w]
  (.write w "#XITDBLinkedArrayList")
  (print-method (into [] o) w))

(extend-protocol common/IMaterialize
  XITDBLinkedArrayList
  (-materialize [this]
    (reduce (fn [a v]
              (conj a (common/materialize v))) [] (seq this))))

;; -----------------------------------------------------------------

(deftype XITDBWriteLinkedArrayList [^WriteLinkedArrayList wlal]
  clojure.lang.IPersistentCollection
  (count [this]
    (.count wlal))

  (cons [this o]
    ;; TODO: This should insert at position 0
    (operations/linked-array-list-insert-value! wlal 0 (common/unwrap o))
    this)

  (empty [this]
    ;; Assuming similar empty behavior as arrays
    (let [^WriteCursor cursor (-> wlal .cursor)]
      (.write cursor (conversion/v->slot! cursor (list))))
    this)

  (equiv [this other]
    (if (instance? XITDBWriteLinkedArrayList other)
      (and (= (count this) (count other))
           (every? (fn [i] (= (get this i) (get other i)))
                   (range (count this))))
      false))

  clojure.lang.Indexed
  (nth [this i]
    (.nth this i nil))

  (nth [this i not-found]
    (if (and (>= i 0) (< i (.count wlal)))
      (common/-read-from-cursor (.putCursor wlal i))
      not-found))

  clojure.lang.Associative
  (assoc [this k v]
    (when-not (integer? k)
      (throw (IllegalArgumentException. "Key must be integer")))
    ;; LinkedArrayList doesn't support random access assoc operations
    ;; This might require a different implementation
    (throw (UnsupportedOperationException. "LinkedArrayList doesn't support random access"))
    this)

  (containsKey [this k]
    (and (integer? k) (>= k 0) (< k (.count wlal))))

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
    (array-seq wlal))

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
    (operations/linked-array-list-append-value! wlal (common/unwrap val))
    this)

  (persistent [this]
    this)

  clojure.lang.IPersistentStack
  (peek [this]
    (if (pos? (.count wlal))
      (common/-read-from-cursor (.getCursor wlal 0))
      nil))

  (pop [this]
    (if (pos? (.count wlal))
      (operations/linked-array-list-pop! wlal)
      (throw (IllegalStateException. "Can't pop empty list")))
    this)

  clojure.lang.IPersistentList
  ;; No additional methods needed, IPersistentList just extends IPersistentStack

  common/ISlot
  (-slot [this]
    (-> wlal .cursor .slot))

  common/IUnwrap
  (-unwrap [this]
    wlal)

  Object
  (toString [this]
    (str "XITDBWriteLinkedArrayList")))

(extend-protocol common/IMaterialize
  XITDBLinkedArrayList
  (-materialize [this]
    (apply list
      (reduce (fn [a v]
                (conj a (common/materialize v))) [] (seq this)))))

;; Constructors

(defn xlinked-list [^ReadCursor cursor]
  (->XITDBLinkedArrayList (ReadLinkedArrayList. cursor)))

(defn xwrite-linked-list [^WriteCursor write-cursor]
  (->XITDBWriteLinkedArrayList (WriteLinkedArrayList. write-cursor)))