(ns xitdb.common
  (:require
    [xitdb.xitdb-util :as util]))

(defprotocol ISlot
  (-slot [this]))

(defprotocol IReadFromCursor
  (-read-from-cursor [this]))

(defprotocol IMaterialize
  (-materialize [this]))

(defprotocol IUnwrap
  (-unwrap [this]))


(defn materialize [v]
  (if (satisfies? IMaterialize v)
    (-materialize v)
    v))

(defn unwrap
  "For a value that wraps another value, returns the wrapped value."
  [v]
  (if (satisfies? IUnwrap v)
    (-unwrap v)
    v))