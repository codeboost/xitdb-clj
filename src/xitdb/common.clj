(ns xitdb.common
  (:require [xitdb.util.db-context :as db-context])
  (:import [io.github.radarroark.xitdb ReadCursor]))

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

(def ^:private ^Class unwrap-interface (:on-interface IUnwrap))

(defn wrapper?
  "True for the XITDB* wrapper types, which all implement `IUnwrap` inline.
  An interface check rather than `satisfies?`, which costs microseconds per
  call on non-implementing classes and sits on the per-element write path."
  [v]
  (instance? unwrap-interface v))

(defn unwrap
  "For a value that wraps another value, returns the wrapped value."
  [v]
  (if (wrapper? v)
    (-unwrap v)
    v))

;; track ancestors only, so shared values in separate branches are allowed
(def ^:dynamic ^:private *materializing* #{})

(declare materialize)

(defn- materialize-value [v]
  (cond
    (satisfies? IMaterialize v) (-materialize v)
    (vector? v) (mapv materialize v)
    (map? v) (reduce-kv (fn [m k v] (assoc m (materialize k) (materialize v))) {} v)
    (set? v) (into #{} (map materialize v))
    (seq? v) (doall (map materialize v))
    :else v))

(defn materialize
  "converts collections to native values, rejecting cyclic database references."
  [v]
  (if (wrapper? v)
    (let [^ReadCursor cursor (-> v -unwrap .cursor)
          slot (.slot cursor)
          offset (.valueOffset slot)]
      (if (some? offset)
        (let [reference [(db-context/reader-database (.-db cursor)) (.tag slot) offset]]
          (when (contains? *materializing* reference)
            (throw (IllegalArgumentException. "Cannot materialize a cyclic xitdb value.")))
          (binding [*materializing* (conj *materializing* reference)]
            (materialize-value v)))
        (materialize-value v)))
    (materialize-value v)))
