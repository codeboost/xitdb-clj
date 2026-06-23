(ns xitdb.sorted-key-test
  (:require
    [clojure.test :refer :all]
    [xitdb.util.sorted-key :as sk]))

(defn cmp-unsigned [^bytes a ^bytes b]
  (java.util.Arrays/compareUnsigned a b))

(deftest string-roundtrip
  (testing "strings encode and decode back to the same string"
    (doseq [s ["" "a" "hello" "with spaces" "unicode-é-字"]]
      (is (= s (sk/decode-key (sk/encode-key s)))))))

(deftest keyword-roundtrip
  (testing "keywords round-trip, including namespaced"
    (doseq [k [:a :foo/bar :a-much-longer-keyword]]
      (is (= k (sk/decode-key (sk/encode-key k)))))))

(deftest string-order-preserved
  (testing "byte order matches code-point order for strings"
    (doseq [[a b] [["a" "b"] ["a" "ab"] ["abc" "abd"] ["" "a"] ["k0009" "k0010"]]]
      (is (neg? (cmp-unsigned (sk/encode-key a) (sk/encode-key b)))
          (str a " < " b)))))
