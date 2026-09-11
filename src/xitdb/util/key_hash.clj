(ns xitdb.util.key-hash
  "Canonical hashes for collection-valued keys. Sequence order matters; map
  and set order does not. Native and database-backed collections share the
  same encoding. Printer settings and collection hashCode are never consulted."
  (:require [xitdb.util.validation :as validation])
  (:import [io.github.radarroark.xitdb Database Database$HashFunction]
           [java.io DataOutputStream OutputStream]
           [java.nio.charset StandardCharsets]
           [java.security DigestOutputStream]
           [java.time Instant]
           [java.util Arrays Date UUID]))

;; Type discriminators for the canonical hash encoding. These belong to a
;; separate encoding from sorted-key's ordering tags and the engine's storage
;; tags. Keep their byte values stable: they contribute to persisted key hashes.
;; Equal lists and vectors share a tag; maps and sets each have their own shape.
(def ^:private ^:const tag-nil 0)
(def ^:private ^:const tag-boolean 1)
(def ^:private ^:const tag-integer 2)
(def ^:private ^:const tag-float 3)
(def ^:private ^:const tag-char 4)
(def ^:private ^:const tag-string 5)
(def ^:private ^:const tag-keyword 6)
(def ^:private ^:const tag-uuid 7)
(def ^:private ^:const tag-instant 8)
(def ^:private ^:const tag-date 9)
(def ^:private ^:const tag-sequence 10)
(def ^:private ^:const tag-map 11)
(def ^:private ^:const tag-set 12)

(declare value-hash)

(defn date-millis
  "Returns epoch milliseconds for an exact java.util.Date key.
  Rejects subclasses with IllegalArgumentException because they may have
  different equality or extra precision; callers can convert them to Instant."
  [^Date date]
  (when-not (= Date (class date))
    (throw (IllegalArgumentException.
             "Date subclasses are not supported as keys; convert the key to java.time.Instant instead.")))
  (.getTime date))

(defn- write-text!
  "Writes `s` as UTF-8 bytes prefixed by their 32-bit byte length.
  The length keeps adjacent text fields unambiguous in the hash input."
  [^DataOutputStream out ^String s]
  (let [bytes (.getBytes s StandardCharsets/UTF_8)]
    (.writeInt out (alength bytes))
    (.write out bytes)))

(defn- write-children!
  "Writes fixed-width child digests computed with `db`'s hash configuration.
  Streams them in sequence order, or sorts them by unsigned byte order when
  `unordered?` is true so map/set iteration order cannot affect the result.
  Sorting buffers immediate child digests, without materializing nested values."
  [db ^DataOutputStream out children unordered?]
  (let [hashes (map #(value-hash db %) children)]
    (doseq [^bytes hash (if unordered?
                         (sort #(Arrays/compareUnsigned ^bytes %1 ^bytes %2) hashes)
                         hashes)]
      (.write out hash))))

(defn- write-value!
  "Writes `v`'s canonical type tag and payload to the hash input stream `out`.
  Scalars use binary fields; collections contribute recursively computed child
  digests. Normalizes equal numeric representations and list/vector shapes.
  Rejects lazy sequences, unsupported types, and Date subclasses."
  [db ^DataOutputStream out v]
  (cond
    (validation/lazy-seq? v)
    (throw (IllegalArgumentException. "Lazy sequences can be infinite and not allowed!"))

    (nil? v)
    (.writeByte out tag-nil)

    (boolean? v)
    (do
      (.writeByte out tag-boolean)
      (.writeBoolean out v))

    (integer? v)
    (do
      (.writeByte out tag-integer)
      (.writeLong out (long v)))

    (float? v)
    (do
      (.writeByte out tag-float)
      ;; Clojure considers positive and negative zero equal.
      (.writeDouble out (if (zero? v) 0.0 (double v))))

    (char? v)
    (do
      (.writeByte out tag-char)
      (.writeChar out (int v)))

    (string? v)
    (do
      (.writeByte out tag-string)
      (write-text! out v))

    (keyword? v)
    (do
      (.writeByte out tag-keyword)
      (.writeBoolean out (some? (namespace v)))
      (when-let [ns (namespace v)] (write-text! out ns))
      (write-text! out (name v)))

    (instance? UUID v)
    (do
      (.writeByte out tag-uuid)
      (.writeLong out (.getMostSignificantBits ^UUID v))
      (.writeLong out (.getLeastSignificantBits ^UUID v)))

    (instance? Instant v)
    (do
      (.writeByte out tag-instant)
      (.writeLong out (.getEpochSecond ^Instant v))
      (.writeInt out (.getNano ^Instant v)))

    (instance? Date v)
    (do
      (.writeByte out tag-date)
      (.writeLong out (date-millis v)))

    (sequential? v)
    (do
      (.writeByte out tag-sequence)
      (write-children! db out v false))

    (map? v)
    (do
      (.writeByte out tag-map)
      (write-children! db out v true))

    (set? v)
    (do
      (.writeByte out tag-set)
      (write-children! db out v true))

    :else
    (throw (IllegalArgumentException. (str "Unsupported key element type: " (type v))))))

(defn value-hash
  "Hash a collection key recursively using the database's independent digests.
  Tags distinguish shapes and scalar types; fixed-width child digests preserve
  element boundaries."
  ^bytes [^Database db v]
  (.hash db
    (reify Database$HashFunction
      (update [_ digest]
        (with-open [out (DataOutputStream.
                          (DigestOutputStream. (OutputStream/nullOutputStream) digest))]
          (write-value! db out v))))))
