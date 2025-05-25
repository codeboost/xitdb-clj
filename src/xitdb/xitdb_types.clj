(ns xitdb.xitdb-types
  (:require
    [xitdb.array-list :as xarray-list]
    [xitdb.common :as common]
    [xitdb.hash-map :as xhash-map]
    [xitdb.linked-list :as xlinked-list]
    [xitdb.hash-set :as xhash-set]
    [xitdb.util.conversion :as conversion])
  (:import
    (io.github.radarroark.xitdb ReadCursor ReadHashMap Slot Tag WriteCursor)))

(defn xhash-map-or-set [^ReadCursor cursor]
  (let [hm (ReadHashMap. cursor)]
    (if (.getCursor hm (conversion/db-key :%xitdb_set))
      (xhash-set/xhash-set cursor)
      (xhash-map/xhash-map cursor))))

(defn x-write-map-or-set [^ReadCursor cursor]
  (let [hm (ReadHashMap. cursor)]
    (if (.getCursor hm (conversion/db-key :%xitdb_set))
      (xhash-set/xwrite-hash-set cursor)
      (xhash-map/xwrite-hash-map cursor))))


(defn read-from-cursor [^ReadCursor cursor for-writing?]
  (let [value-tag (some-> cursor .slot .tag)]
    #_(println "read-from-cursor" value-tag "for-writing?" for-writing?)
    (cond
      (contains? #{Tag/SHORT_BYTES Tag/BYTES} value-tag)
      (conversion/read-bytes-with-format-tag cursor)

      (= value-tag Tag/UINT)
      (.readUint cursor)

      (= value-tag Tag/INT)
      (.readInt cursor)

      (= value-tag Tag/FLOAT)
      (.readFloat cursor)

      (= value-tag Tag/HASH_MAP)

      (if for-writing?
        (x-write-map-or-set cursor)
        (xhash-map-or-set cursor))

      (= value-tag Tag/ARRAY_LIST)
      (if for-writing?
        (xarray-list/xwrite-array-list cursor)
        (xarray-list/xarray-list cursor))

      (= value-tag Tag/LINKED_ARRAY_LIST)
      (if for-writing?
        (xlinked-list/xwrite-linked-list cursor)
        (xlinked-list/xlinked-list cursor))

      :else
      nil)))

(extend-protocol common/IReadFromCursor
  ReadCursor
  (-read-from-cursor [this]
    (read-from-cursor this false))

  WriteCursor
  (-read-from-cursor [this]
    (read-from-cursor this true)))

(defn ^Slot slot-for-value! [^WriteCursor cursor v]
  (cond
    (satisfies? common/ISlot v)
    (common/-slot v)
    :else
    (conversion/v->slot! cursor v)))

(defn materialize
  "Converts a xitdb data structure `v` to a clojure data structure.
  This has the effect of reading the whole data structure into memory."
  [v]
  (common/materialize v))