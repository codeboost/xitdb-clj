(ns xitdb.util.sorted-key
  "Order-preserving, reversible key codec for on-disk sorted maps/sets.

  Unlike hash maps (which SHA-1-hash their keys), sorted collections store the
  real key bytes so they can be recovered on read and compared by the engine's
  unsigned lexicographic byte comparison (`Arrays.compareUnsigned`). The codec
  must therefore be:

    1. reversible  - `decode-key (encode-key k)` == k
    2. order-preserving - `sign(compareUnsigned (encode a) (encode b))`
                          == `sign(compare a b)` for any two keys.

  Every encoding carries a leading 1-byte type tag. The tag both identifies the
  type on decode and establishes a total order across types, so heterogeneous
  keys never throw.

  This namespace currently implements string and keyword keys (UTF-8 bytes,
  which already sort in code-point order). Numeric/temporal keys are added in a
  later slice."
  (:import
    [java.io ByteArrayOutputStream]
    [java.nio ByteBuffer]
    [java.nio.charset StandardCharsets]
    [java.time Instant]
    [java.util Date]))

;; Type tags. Ordering of the tag values defines the cross-type order; they are
;; intentionally sparse to leave room for additional types in later slices.
;; Current cross-type order (by ascending tag byte):
;;   long (0x10) < double (0x11) < instant (0x18) < date (0x19)
;;   < string (0x20) < keyword (0x21)
(def ^:const tag-long    (int 0x10))
(def ^:const tag-double  (int 0x11))
(def ^:const tag-instant (int 0x18))
(def ^:const tag-date    (int 0x19))
(def ^:const tag-string  (int 0x20))
(def ^:const tag-keyword (int 0x21))

(defn- ^bytes utf8 [^String s]
  (.getBytes s StandardCharsets/UTF_8))

(defn- ^bytes tagged [tag ^bytes body]
  (let [out (ByteArrayOutputStream. (inc (alength body)))]
    (.write out (int tag))
    (.write out body 0 (alength body))
    (.toByteArray out)))

(defn- ^bytes long->bytes
  "8-byte big-endian with the sign bit flipped, so signed longs sort correctly
  under unsigned byte comparison (negatives before positives)."
  [^long n]
  (let [buf (doto (ByteBuffer/allocate 8) (.putLong (bit-xor n Long/MIN_VALUE)))]
    (.array buf)))

(defn- bytes->long
  "Inverse of `long->bytes`. Reads 8 big-endian bytes starting at `off`."
  [^bytes ba off]
  (bit-xor (.getLong (ByteBuffer/wrap ba (int off) 8)) Long/MIN_VALUE))

(defn- ^bytes double->bytes
  "IEEE-754 8-byte big-endian with an order-preserving bit flip: if the sign bit
  is set, flip all bits; otherwise flip only the sign bit. This makes doubles
  sort numerically under unsigned byte comparison. NaN is rejected by
  `encode-key` (its ordering is undefined), so it never reaches here."
  [^double d]
  (let [bits (Double/doubleToLongBits d)
        flipped (if (neg? bits)
                  (bit-not bits)
                  (bit-or bits Long/MIN_VALUE))
        buf (doto (ByteBuffer/allocate 8) (.putLong flipped))]
    (.array buf)))

(defn- bytes->double
  "Inverse of `double->bytes`. Reads 8 big-endian bytes starting at `off`."
  [^bytes ba off]
  (let [flipped (.getLong (ByteBuffer/wrap ba (int off) 8))
        bits (if (neg? flipped)
               (bit-and flipped Long/MAX_VALUE)
               (bit-not flipped))]
    (Double/longBitsToDouble bits)))

(defn- ^bytes instant->bytes
  "12 bytes: epoch-second (8-byte big-endian, sign-flipped so negative epochs
  sort first) followed by nano-of-second (4-byte big-endian; always 0..1e9-1, so
  unsigned order is chronological). Byte order therefore equals chronological
  order across the full Instant range."
  [^Instant i]
  (let [buf (doto (ByteBuffer/allocate 12)
              (.putLong (bit-xor (.getEpochSecond i) Long/MIN_VALUE))
              (.putInt (int (.getNano i))))]
    (.array buf)))

(defn- ^Instant bytes->instant [^bytes ba off]
  (let [bb   (ByteBuffer/wrap ba (int off) 12)
        secs (bit-xor (.getLong bb) Long/MIN_VALUE)
        nano (.getInt bb)]
    (Instant/ofEpochSecond secs nano)))

(defn- ^bytes date->bytes
  "8-byte big-endian epoch-millis with the sign bit flipped, so pre-epoch dates
  sort before the epoch. Byte order equals chronological order."
  [^Date d]
  (let [buf (doto (ByteBuffer/allocate 8)
              (.putLong (bit-xor (.getTime d) Long/MIN_VALUE)))]
    (.array buf)))

(defn- ^Date bytes->date [^bytes ba off]
  (Date. (bit-xor (.getLong (ByteBuffer/wrap ba (int off) 8)) Long/MIN_VALUE)))

(defn ^String keyname
  "String form of a keyword key, namespace-qualified when present."
  [k]
  (if (namespace k)
    (str (namespace k) "/" (name k))
    (name k)))

(defn encode-key
  "Encodes Clojure key `k` to an order-preserving, reversible byte array."
  ^bytes [k]
  (cond
    (string? k)
    (tagged tag-string (utf8 k))

    (keyword? k)
    (tagged tag-keyword (utf8 (keyname k)))

    (integer? k)
    (tagged tag-long (long->bytes (long k)))

    (float? k)
    (let [d (double k)]
      (when (Double/isNaN d)
        (throw (IllegalArgumentException.
                 "NaN is not a valid sorted-map key (ordering undefined)")))
      (tagged tag-double (double->bytes d)))

    (instance? Instant k)
    (tagged tag-instant (instant->bytes k))

    (instance? Date k)
    (tagged tag-date (date->bytes k))

    :else
    (throw (IllegalArgumentException.
             (str "Unsupported sorted-map key type: " (type k))))))

(defn- ^String utf8-body [^bytes ba]
  (String. ba 1 (dec (alength ba)) StandardCharsets/UTF_8))

(defn decode-key
  "Decodes a byte array produced by `encode-key` back to the Clojure key."
  [^bytes ba]
  (let [tag (bit-and (int (aget ba 0)) 0xff)]
    (condp = tag
      tag-string  (utf8-body ba)
      tag-keyword (keyword (utf8-body ba))
      tag-long    (bytes->long ba 1)
      tag-double  (bytes->double ba 1)
      tag-instant (bytes->instant ba 1)
      tag-date    (bytes->date ba 1)
      (throw (IllegalArgumentException. (str "Unknown sorted-key tag: " tag))))))
