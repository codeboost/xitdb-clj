(ns xitdb.hash-map
  (:require
    [xitdb.common :as common]
    [xitdb.xitdb-util :as util])
  (:import
    (io.github.radarroark.xitdb ReadCursor ReadHashMap WriteCursor WriteHashMap)))

(defn map-seq
  [rhm]
  "The cursors used must implement the IReadFromCursor protocol."
  (util/map-seq rhm #(common/-read-from-cursor %)))

(deftype XITDBHashMap [^ReadHashMap rhm]
  clojure.lang.ILookup
  (valAt [this key]
    (.valAt this key nil))

  (valAt [this key not-found]
    (let [cursor (.getCursor rhm (util/db-key key))]
      (if (nil? cursor)
        not-found
        (common/-read-from-cursor cursor))))

  clojure.lang.Associative
  (containsKey [this key]
    (not (nil? (.getCursor rhm (util/db-key key)))))

  (entryAt [this key]
    (let [v (.valAt this key nil)]
      (when-not (nil? v)
        (clojure.lang.MapEntry. key v))))

  (assoc [this k v]
    (throw (UnsupportedOperationException. "XITDBHashMap is read-only")))

  clojure.lang.IPersistentMap
  (without [_ _]
    (throw (UnsupportedOperationException. "XITDBHashMap is read-only")))

  (count [this]
    (util/map-item-count rhm))

  clojure.lang.IPersistentCollection
  (cons [_ _]
    (throw (UnsupportedOperationException. "XITDBHashMap is read-only")))

  (empty [_]
    (throw (UnsupportedOperationException. "XITDBHashMap is read-only")))

  (equiv [this other]
    (and (instance? clojure.lang.IPersistentMap other)
         (= (into {} this) (into {} other))))

  clojure.lang.Seqable
  (seq [this]
    (map-seq rhm))

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
          (throw (UnsupportedOperationException. "XITDBHashMap iterator is read-only"))))))


  common/IUnwrap
  (-unwrap [this]
    rhm)

  Object
  (toString [this]
    (str (into {} this))))

(defmethod print-method XITDBHashMap [o ^java.io.Writer w]
  (.write w "#XITDBHashMap")
  (print-method (into {} o) w))

(extend-protocol common/IMaterialize
  XITDBHashMap
  (-materialize [this]
    (reduce (fn [m [k v]]
              (assoc m k (common/materialize v))) {} (seq this))))

;---------------------------------------------------


(deftype XITDBWriteHashMap [^WriteHashMap whm]
  clojure.lang.IPersistentCollection
  (cons [this o]

    (cond
      (instance? clojure.lang.MapEntry o)
      (.assoc this (key o) (val o))

      (map? o)
      (doseq [[k v] (seq o)]
        (.assoc this k v))

      (and (sequential? o) (= 2 (count o)))
      (do
        (.assoc this (first o) (second o)))

      :else
      (throw (IllegalArgumentException. "Can only cons MapEntries or key-value pairs onto maps")))
    this)

  (empty [this]
    (util/map-empty! whm)
    this)

  (equiv [this other]
    (and (= (count this) (count other))
         (every? (fn [[k v]] (= v (get other k ::not-found)))
                 (seq this))))
  clojure.lang.Associative
  (assoc [this k v]
    (util/map-assoc-value! whm k (common/unwrap v))
    this)

  (containsKey [this key]
    (util/map-contains-key? whm key))

  (entryAt [this key]
    (when (.containsKey this key)
      (clojure.lang.MapEntry. key (.valAt this key))))

  clojure.lang.IPersistentMap
  (without [this key]
    (util/map-dissoc-key! whm key)
    this)

  (count [this]
    (util/map-item-count whm))

  clojure.lang.ILookup
  (valAt [this key]
    (.valAt this key nil))

  (valAt [this key not-found]
    (let [cursor (util/map-read-cursor whm key)]
      (if (nil? cursor)
        not-found
        (common/-read-from-cursor (util/map-write-cursor whm key)))))

  clojure.lang.Seqable
  (seq [this]
    (map-seq whm))

  common/ISlot
  (-slot [this]
    (-> whm .cursor .slot))

  common/IUnwrap
  (-unwrap [this]
    whm)

  Object
  (toString [this]
    (str "XITDBWriteHashMap")))


(defn xwrite-hash-map [^WriteCursor write-cursor]
  (->XITDBWriteHashMap (WriteHashMap. write-cursor)))

(defn xhash-map [^ReadCursor read-cursor]
  (->XITDBHashMap (ReadHashMap. read-cursor)))


