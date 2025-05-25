(ns xitdb.util.validation)

(defn lazy-seq? [v]
  (instance? clojure.lang.LazySeq v))

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