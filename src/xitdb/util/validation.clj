(ns xitdb.util.validation)

(defn lazy-seq?
  "True when `v` is a lazy sequence: a LazySeq, a `repeat`/`cycle`/`iterate`
  seq (rejected even when bounded, since the type cannot say), or a chain of
  already-realized cons cells (`cons`, `list*`, a chunked seq) whose tail is
  one of those. Only walks cells that exist, so it never realizes anything."
  [v]
  (loop [v v]
    (cond
      (or (instance? clojure.lang.LazySeq v)
          (instance? clojure.lang.Repeat v)
          (instance? clojure.lang.Cycle v)
          (instance? clojure.lang.Iterate v)) true
      (instance? clojure.lang.Cons v) (recur (.more ^clojure.lang.Cons v))
      (instance? clojure.lang.ChunkedCons v) (recur (.more ^clojure.lang.ChunkedCons v))
      :else false)))

(defn vector-or-chunked? [v]
  (or (vector? v) (chunked-seq? v)))

(defn list-or-cons? [v]
  (or (list? v) (instance? clojure.lang.Cons v)))

(defn validate-index-bounds
  "Validates that index i is within bounds for a collection of given count.
  Throws IllegalArgumentException if out of bounds."
  [i count operation-name]
  (when (or (< i 0) (> i count))
    (throw (IllegalArgumentException.
             (str operation-name " index " i " out of bounds for collection of size " count)))))

(defn validate-non-empty
  "Validates that a collection is not empty.
  Throws IllegalStateException if empty."
  [count operation-name]
  (when (zero? count)
    (throw (IllegalStateException.
             (str "Cannot " operation-name " on empty collection")))))

(defn validate-not-lazy-seq
  "Validates that a value is not a lazy sequence.
  Throws IllegalArgumentException if it is a lazy sequence."
  [v]
  (when (lazy-seq? v)
    (throw (IllegalArgumentException. "Lazy sequences can be infinite and not allowed!"))))

(defn validate-supported-type
  "Validates that a type is supported for conversion.
  Throws IllegalArgumentException for unsupported types."
  [v]
  (let [supported-types #{java.lang.String
                          clojure.lang.Keyword
                          java.lang.Long
                          java.lang.Integer
                          java.lang.Boolean
                          java.lang.Double
                          java.lang.Float
                          java.time.Instant
                          java.util.Date}]
    (when-not (or (nil? v)
                  (some #(instance? % v) supported-types)
                  (map? v)
                  (vector? v)
                  (list? v)
                  (set? v))
      (throw (IllegalArgumentException. (str "Unsupported type: " (type v) " for value: " v))))))