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
    [java.nio.charset StandardCharsets]))

;; Type tags. Ordering of the tag values defines the cross-type order; they are
;; intentionally sparse to leave room for numeric/temporal types between/around
;; them in later slices.
(def ^:const tag-string  (int 0x20))
(def ^:const tag-keyword (int 0x21))

(defn- ^bytes utf8 [^String s]
  (.getBytes s StandardCharsets/UTF_8))

(defn- ^bytes tagged [tag ^bytes body]
  (let [out (ByteArrayOutputStream. (inc (alength body)))]
    (.write out (int tag))
    (.write out body 0 (alength body))
    (.toByteArray out)))

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

    :else
    (throw (IllegalArgumentException.
             (str "Unsupported sorted-map key type: " (type k))))))

(defn decode-key
  "Decodes a byte array produced by `encode-key` back to the Clojure key."
  [^bytes ba]
  (let [tag  (bit-and (int (aget ba 0)) 0xff)
        body (String. ba 1 (dec (alength ba)) StandardCharsets/UTF_8)]
    (condp = tag
      tag-string  body
      tag-keyword (keyword body)
      (throw (IllegalArgumentException. (str "Unknown sorted-key tag: " tag))))))
