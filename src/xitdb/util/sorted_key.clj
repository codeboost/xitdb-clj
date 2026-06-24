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

  Supported key types: string, keyword, long, double, Instant and Date. Strings
  encode as their UTF-8 bytes (already code-point ordered); keywords use a flag
  + namespace + name layout so they sort like Clojure's default comparator (see
  `keyword->bytes`); numeric/temporal keys use order-preserving big-endian
  encodings."
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

;; Keyword presence-of-namespace flag (the first body byte). 0 sorts before 1,
;; so non-namespaced keywords sort before namespaced ones, matching Clojure's
;; default comparator (clojure.lang.Symbol.compareTo).
(def ^:const kw-no-ns  (int 0x00))
(def ^:const kw-has-ns (int 0x01))

(defn- keyword->bytes
  "Order-preserving, collision-free encoding of a keyword, matching Clojure's
  default comparator: non-namespaced keywords sort before namespaced ones, then
  by namespace, then by name.

  Layout (after the type tag): a flag byte, then the parts.
    - no namespace : `kw-no-ns`  ++ name-utf8
    - namespaced   : `kw-has-ns` ++ ns-utf8 ++ 0x00 ++ name-utf8

  The 0x00 separator can never appear inside UTF-8 keyword text (NUL is not a
  legal keyword character), so it sorts below every namespace byte and cleanly
  delimits namespace from name. The flag byte also keeps `(keyword nil \"a/b\")`
  (no namespace, name \"a/b\") distinct from `:a/b` (namespace \"a\", name
  \"b\"), which would otherwise both flatten to \"a/b\"."
  ^bytes [k]
  (let [out (ByteArrayOutputStream.)
        ns  (namespace k)
        nm  ^bytes (utf8 (name k))]
    (if ns
      (let [nsb ^bytes (utf8 ns)]
        (.write out kw-has-ns)
        (.write out nsb 0 (alength nsb))
        (.write out (int 0x00))
        (.write out nm 0 (alength nm)))
      (do
        (.write out kw-no-ns)
        (.write out nm 0 (alength nm))))
    (.toByteArray out)))

(defn encode-key
  "Encodes Clojure key `k` to an order-preserving, reversible byte array."
  ^bytes [k]
  (cond
    (string? k)
    (tagged tag-string (utf8 k))

    (keyword? k)
    (tagged tag-keyword (keyword->bytes k))

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

(defn- decode-keyword
  "Inverse of `keyword->bytes`. `ba[0]` is the type tag, `ba[1]` is the
  namespace-presence flag, the remainder is the part(s)."
  [^bytes ba]
  (let [flag (bit-and (int (aget ba 1)) 0xff)]
    (if (= flag kw-no-ns)
      ;; Use the 2-arg form with a nil namespace so a name containing \"/\"
      ;; is not re-parsed into a namespace (which would corrupt the key).
      (keyword nil (String. ba 2 (- (alength ba) 2) StandardCharsets/UTF_8))
      (let [sep (loop [i 2]
                  (if (zero? (aget ba i)) i (recur (inc i))))
            ns  (String. ba 2 (- sep 2) StandardCharsets/UTF_8)
            nm  (String. ba (inc sep) (- (alength ba) (inc sep)) StandardCharsets/UTF_8)]
        (keyword ns nm)))))

(def key-comparator
  "A `java.util.Comparator` consistent with the engine's natural ordering:
  compares two keys by `Arrays.compareUnsigned` over their encoded bytes. Use
  this (not `clojure.core/compare`) so `subseq`/`rsubseq` bound checks agree with
  on-disk order across all supported types, including heterogeneous keys."
  (reify java.util.Comparator
    (compare [_ a b]
      (java.util.Arrays/compareUnsigned (encode-key a) (encode-key b)))))

(defn decode-key
  "Decodes a byte array produced by `encode-key` back to the Clojure key."
  [^bytes ba]
  (let [tag (bit-and (int (aget ba 0)) 0xff)]
    (condp = tag
      tag-string  (utf8-body ba)
      tag-keyword (decode-keyword ba)
      tag-long    (bytes->long ba 1)
      tag-double  (bytes->double ba 1)
      tag-instant (bytes->instant ba 1)
      tag-date    (bytes->date ba 1)
      (throw (IllegalArgumentException. (str "Unknown sorted-key tag: " tag))))))
