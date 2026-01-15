(ns xitdb.common)

(defprotocol ISlot
  (-slot [this]))

(defprotocol IReadFromCursor
  (-read-from-cursor [this]))

(defprotocol IMaterialize
  (-materialize [this]))

(defprotocol IMaterializeShallow
  (-materialize-shallow [this]))

(defprotocol IUnwrap
  (-unwrap [this]))

(defprotocol IReadOnly
  (-read-only [this]))

(defn materialize [v]
  (cond
    (satisfies? IMaterialize v) (-materialize v)
    (vector? v) (mapv materialize v)
    (map? v) (reduce-kv (fn [m k v] (assoc m (materialize k) (materialize v))) {} v)
    (set? v) (into #{} (map materialize v))
    (seq? v) (doall (map materialize v))
    :else v))

(defn unwrap
  "For a value that wraps another value, returns the wrapped value."
  [v]
  (if (satisfies? IUnwrap v)
    (-unwrap v)
    v))