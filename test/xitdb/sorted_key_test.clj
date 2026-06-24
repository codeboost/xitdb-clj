(ns xitdb.sorted-key-test
  (:require
    [clojure.test :refer :all]
    [xitdb.util.sorted-key :as sk])
  (:import
    [java.time Instant]
    [java.util Date]))

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

(deftest keyword-order-matches-clojure
  (testing "byte order matches Clojure's default keyword comparator:
            non-namespaced keywords sort before namespaced ones"
    (doseq [[a b] [[:a :aa] [:aa :b]
                   ;; every non-namespaced keyword sorts before any namespaced
                   [:b :a/a] [:zzz :a/a]
                   ;; among namespaced: by namespace then name
                   [:a/a :a/b] [:a/x :ab/a] [:a/b :b/a]]]
      (is (neg? (cmp-unsigned (sk/encode-key a) (sk/encode-key b)))
          (str a " < " b))
      ;; and consistent with clojure.core/compare on keywords
      (is (= (Integer/signum (compare a b))
             (Integer/signum (cmp-unsigned (sk/encode-key a) (sk/encode-key b))))
          (str "order-agrees " a " " b)))))

(deftest keyword-namespace-no-collision
  (testing "(keyword nil \"a/b\") and :a/b are distinct keys that both round-trip"
    (let [k1 (keyword nil "a/b") ;; no namespace, name contains a slash
          k2 :a/b]              ;; namespace \"a\", name \"b\"
      (is (not= k1 k2))
      (is (= k1 (sk/decode-key (sk/encode-key k1))))
      (is (= k2 (sk/decode-key (sk/encode-key k2))))
      (is (not (java.util.Arrays/equals (sk/encode-key k1) (sk/encode-key k2)))
          "encodings must differ so the keys do not collide on disk"))))

(deftest string-order-preserved
  (testing "byte order matches code-point order for strings"
    (doseq [[a b] [["a" "b"] ["a" "ab"] ["abc" "abd"] ["" "a"] ["k0009" "k0010"]]]
      (is (neg? (cmp-unsigned (sk/encode-key a) (sk/encode-key b)))
          (str a " < " b)))))

(deftest long-roundtrip
  (testing "longs round-trip, including boundary values"
    (doseq [n [0 1 -1 42 -42 Long/MIN_VALUE Long/MAX_VALUE
               (long Integer/MIN_VALUE) (long Integer/MAX_VALUE)]]
      (is (= n (sk/decode-key (sk/encode-key n)))
          (str "roundtrip " n)))))

(deftest long-order-preserved
  (testing "byte order matches numeric order, negatives before positives"
    (doseq [[a b] [[1 2] [9 10] [-5 0] [-5 3] [0 3]
                   [Long/MIN_VALUE -1] [Long/MIN_VALUE Long/MAX_VALUE]
                   [-1 0] [0 Long/MAX_VALUE]]]
      (is (neg? (cmp-unsigned (sk/encode-key a) (sk/encode-key b)))
          (str a " < " b)))))

(deftest double-roundtrip
  (testing "doubles round-trip, including extremes"
    (doseq [d [0.0 1.0 -1.0 3.14 -3.14 1.0e308 -1.0e308 1.0e-308 -1.0e-308
               Double/MIN_VALUE Double/MAX_VALUE]]
      (is (= d (sk/decode-key (sk/encode-key d)))
          (str "roundtrip " d)))))

(deftest double-order-preserved
  (testing "byte order matches numeric order across sign and magnitude"
    (doseq [[a b] [[1.0 2.0] [-1.0 0.0] [-2.0 -1.0] [-1.0e308 1.0e308]
                   [0.0 1.0e-308] [-3.14 3.14] [-1.0e-308 0.0]]]
      (is (neg? (cmp-unsigned (sk/encode-key a) (sk/encode-key b)))
          (str a " < " b)))))

(deftest date-roundtrip
  (testing "dates round-trip to Date"
    (doseq [d [(Date. 0) (Date. 1719100000000) (Date. -100000) (Date.)]]
      (is (= d (sk/decode-key (sk/encode-key d)))
          (str "roundtrip " d))
      (is (instance? Date (sk/decode-key (sk/encode-key d)))))))

(deftest date-order-preserved
  (testing "byte order matches chronological order, including pre-epoch"
    (doseq [[a b] [[(Date. 0) (Date. 1)]
                   [(Date. -5000) (Date. 0)]
                   [(Date. 1000) (Date. 2000)]
                   [(Date. -2000) (Date. -1000)]]]
      (is (neg? (cmp-unsigned (sk/encode-key a) (sk/encode-key b)))
          (str a " < " b)))))

;; ----- property-based ordering (deterministic randomized loops, fixed seed) ---

(defn- order-agrees? [a b]
  (= (Integer/signum (compare a b))
     (Integer/signum (cmp-unsigned (sk/encode-key a) (sk/encode-key b)))))

(deftest prop-long-order
  (testing "for 2000 random long pairs, byte order == numeric order"
    (let [r (java.util.Random. 42)]
      (is (every? (fn [_] (order-agrees? (.nextLong r) (.nextLong r)))
                  (range 2000))))))

(deftest prop-double-order
  (testing "for 2000 random finite double pairs, byte order == numeric order"
    (let [r (java.util.Random. 43)
          rand-d (fn [] (* (.nextDouble r)
                           (Math/pow 10 (- (.nextInt r 40) 20))
                           (if (.nextBoolean r) 1.0 -1.0)))]
      (is (every? (fn [_] (order-agrees? (rand-d) (rand-d)))
                  (range 2000))))))

(deftest prop-instant-order
  (testing "for 2000 random instant pairs, byte order == chronological order"
    (let [r (java.util.Random. 44)
          rand-i (fn [] (Instant/ofEpochSecond
                          (- (.nextLong (java.util.Random. (.nextLong r))
                                        4000000000) 2000000000)
                          (.nextInt r 1000000000)))]
      (is (every? (fn [_] (order-agrees? (rand-i) (rand-i)))
                  (range 2000))))))

(deftest prop-roundtrip
  (testing "random keys of every type round-trip exactly"
    (let [r (java.util.Random. 45)]
      (is (every?
            (fn [_]
              (let [k (case (.nextInt r 5)
                        0 (.nextLong r)
                        1 (* (.nextDouble r) (if (.nextBoolean r) 1e9 -1e-9))
                        2 (Instant/ofEpochSecond (.nextInt r 2000000000)
                                                 (.nextInt r 1000000000))
                        3 (Date. (long (.nextInt r 2000000000)))
                        4 (str "k" (.nextInt r 100000)))]
                (= k (sk/decode-key (sk/encode-key k)))))
            (range 2000))))))

(deftest cross-type-never-throws
  (testing "encoding any supported type and comparing across types never throws"
    (let [vals [0 -1 Long/MAX_VALUE 3.14 -2.0 "abc" :kw
                (Instant/ofEpochSecond 5) (Date. 1000)]
          encoded (map sk/encode-key vals)]
      (doseq [a encoded b encoded]
        (is (integer? (cmp-unsigned a b)))))))

(deftest unsupported-key-throws
  (is (thrown? IllegalArgumentException (sk/encode-key nil)))
  (is (thrown? IllegalArgumentException (sk/encode-key true)))
  (is (thrown? IllegalArgumentException (sk/encode-key Double/NaN))))

(deftest instant-roundtrip
  (testing "instants round-trip to Instant, preserving sub-second precision"
    (doseq [i [(Instant/ofEpochSecond 0)
               (Instant/ofEpochSecond 1719100000 123456789)
               (Instant/ofEpochSecond -100 500)
               Instant/EPOCH]]
      (is (= i (sk/decode-key (sk/encode-key i)))
          (str "roundtrip " i))
      (is (instance? Instant (sk/decode-key (sk/encode-key i)))))))

(deftest instant-order-preserved
  (testing "byte order matches chronological order, incl. negative epoch & nanos"
    (doseq [[a b] [[(Instant/ofEpochSecond 0) (Instant/ofEpochSecond 1)]
                   [(Instant/ofEpochSecond -5) (Instant/ofEpochSecond 0)]
                   [(Instant/ofEpochSecond 10 100) (Instant/ofEpochSecond 10 200)]
                   [(Instant/ofEpochSecond 10 999999999) (Instant/ofEpochSecond 11 0)]]]
      (is (neg? (cmp-unsigned (sk/encode-key a) (sk/encode-key b)))
          (str a " < " b)))))
