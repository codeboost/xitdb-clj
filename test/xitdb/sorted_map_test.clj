(ns xitdb.sorted-map-test
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]
    [xitdb.test-utils :as tu :refer [with-db]])
  (:import
    [java.time Instant]
    [java.util Date]))

(deftest lookups-and-count
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-map "a" 1 "b" 2 "c" 3))
    (let [m @db]
      (testing "get / invoke / find / contains?"
        (is (= 1 (get m "a")))
        (is (= 2 (m "b")))
        (is (= ::nf (get m "z" ::nf)))
        (is (true? (contains? m "c")))
        (is (false? (contains? m "z")))
        (is (= (clojure.lang.MapEntry. "a" 1) (find m "a")))
        (is (nil? (find m "z"))))
      (testing "count is correct"
        (is (= 3 (count m)))))))

(deftest mutation-keeps-order
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-map "b" 2 "d" 4))
    (testing "assoc inserts in order"
      (swap! db assoc "c" 3)
      (swap! db assoc "a" 1)
      (is (= ["a" "b" "c" "d"] (map key (seq @db)))))
    (testing "dissoc removes and preserves order"
      (swap! db dissoc "b")
      (is (= ["a" "c" "d"] (map key (seq @db)))))
    (testing "re-assoc replaces value without changing count"
      (swap! db assoc "c" 30)
      (is (= 3 (count @db)))
      (is (= 30 (get @db "c"))))))

(deftest keyword-keys
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-map :banana 2 :apple 1 :cherry 3))
    (testing "keyword keys round-trip as keywords, in sorted order"
      (is (= [:apple :banana :cherry] (map key (seq @db))))
      (is (every? keyword? (map key (seq @db))))
      (is (= 1 (get @db :apple))))))

(deftest namespaced-keyword-keys-match-clojure-order
  (with-open [db (xdb/xit-db :memory)]
    (let [oracle (sorted-map :b 2 :a/a 3 :a 1 :aa 4)]
      (reset! db oracle)
      (testing "namespaced keywords sort like Clojure's default comparator
                (non-namespaced before namespaced), not as flattened strings"
        (is (= (keys oracle) (map key (seq @db))))
        (is (= [:a :aa :b :a/a] (map key (seq @db)))))
      (testing "subseq agrees with the Clojure oracle"
        (is (= (vec (subseq oracle >= :aa))
               (vec (subseq @db >= :aa)))))
      (testing "values round-trip under namespaced keys"
        (is (= 3 (get @db :a/a)))))))

(deftest heterogeneous-keys-materialize-and-print
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-map))
    (swap! db assoc 1 :one)
    (swap! db assoc "x" :ex)
    (testing "seq works with mixed key types"
      (is (= [1 "x"] (map key (seq @db)))))
    (testing "materialize does not throw on mixed key types"
      (let [m (tu/materialize @db)]
        (is (sorted? m))
        (is (= [1 "x"] (keys m)))
        (is (= {1 :one "x" :ex} (into {} m)))))
    (testing "pr-str does not throw on mixed key types"
      (is (string? (pr-str @db))))))

(deftest materialized-sorted-map-can-be-written-back
  (testing "a materialized sorted map (which carries key-comparator) can be
            stored into another db without being rejected as a custom comparator"
    (with-open [db1 (xdb/xit-db :memory)
                db2 (xdb/xit-db :memory)]
      (reset! db1 (sorted-map "b" 2 "a" 1))
      (let [m (tu/materialize @db1)]
        (reset! db2 m)
        (is (= ["a" "b"] (map key (seq @db2))))
        (is (= 1 (get @db2 "a"))))))
  (testing "round-trips through materialize even with heterogeneous keys"
    (with-open [db1 (xdb/xit-db :memory)
                db2 (xdb/xit-db :memory)]
      (reset! db1 (sorted-map))
      (swap! db1 assoc 1 :one)
      (swap! db1 assoc "x" :ex)
      (let [m (tu/materialize @db1)]
        (reset! db2 m)
        (is (= [1 "x"] (map key (seq @db2))))))))

(deftest materialize-returns-plain-sorted-map
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-map "b" 2 "a" 1))
    (let [m (tu/materialize @db)]
      (is (sorted? m))
      (is (not (instance? xitdb.sorted_map.XITDBSortedMap m)))
      (is (= ["a" "b"] (keys m)))
      (is (= {"a" 1 "b" 2} m)))))

(deftest read-only-ops-return-plain-collections
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-map "a" 1 "b" 2))
    (let [m @db]
      (testing "assoc outside a transaction returns a plain sorted map"
        (let [r (assoc m "c" 3)]
          (is (not (instance? xitdb.sorted_map.XITDBSortedMap r)))
          (is (sorted? r))
          (is (= ["a" "b" "c"] (keys r)))))
      (testing "dissoc outside a transaction returns a plain sorted map"
        (let [r (dissoc m "a")]
          (is (not (instance? xitdb.sorted_map.XITDBSortedMap r)))
          (is (sorted? r))
          (is (= ["b"] (keys r))))))))

(deftest custom-comparator-rejected
  (with-open [db (xdb/xit-db :memory)]
    (is (thrown? IllegalArgumentException
                 (reset! db (sorted-map-by > 1 :a 2 :b))))))

(deftest nesting-and-complex-values
  (testing "sorted map nests inside a hash map value"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:idx (sorted-map "b" 2 "a" 1)})
      (is (instance? xitdb.sorted_map.XITDBSortedMap (:idx @db)))
      (is (= ["a" "b"] (map key (seq (:idx @db)))))))
  (testing "nested sorted map round-trips against an in-memory atom"
    (with-db [db (tu/test-db)]
      (reset! db {:idx (sorted-map "b" 2 "a" 1)})
      (is (tu/db-equal-to-atom? db))))
  (testing "sorted map values may be vectors, maps and sets"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (sorted-map "vec" [1 2 3]
                             "map" {:x 1}
                             "set" #{:a :b}))
      (is (= [1 2 3] (tu/materialize (get @db "vec"))))
      (is (= {:x 1} (tu/materialize (get @db "map"))))
      (is (= #{:a :b} (tu/materialize (get @db "set"))))))
  (testing "a sorted map nested directly inside a vector stays sorted"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db [(sorted-map 3 :c 1 :a 2 :b)])
      (let [m (first (seq @db))]
        (is (instance? xitdb.sorted_map.XITDBSortedMap m))
        (is (sorted? m))
        (is (= [1 2 3] (map key (seq m)))))))
  (testing "a sorted map nested inside a list stays sorted"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (list (sorted-map 3 :c 1 :a 2 :b)))
      (let [m (first (seq @db))]
        (is (instance? xitdb.sorted_map.XITDBSortedMap m))
        (is (= [1 2 3] (map key (seq m))))))))

(deftest empty-clears-map
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-map "a" 1 "b" 2))
    (swap! db empty)
    (is (= 0 (count @db)))
    (is (empty? (seq @db)))
    (swap! db assoc "c" 3)
    (is (= ["c"] (map key (seq @db))))))

(deftest empty-then-reassoc-stays-a-sorted-map
  (testing "after (swap! db empty) the value is still a sorted map, so keys
            re-inserted afterwards keep sorted (not hash-map) semantics"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (sorted-map "a" 1 "b" 2))
      (swap! db empty)
      (is (instance? xitdb.sorted_map.XITDBSortedMap @db))
      (is (sorted? @db))
      (is (= 0 (count @db)))
      (swap! db assoc "c" 3 "a" 1)
      (is (instance? xitdb.sorted_map.XITDBSortedMap @db))
      (is (sorted? @db))
      (is (= ["a" "c"] (map key (seq @db)))))))

(deftest print-method-ordered
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-map "b" 2 "a" 1))
    (let [s (pr-str @db)]
      (is (clojure.string/starts-with? s "#XITDBSortedMap"))
      (is (clojure.string/includes? s "\"a\" 1, \"b\" 2")))))

(deftest sorted-predicate-and-comparator
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-map 3 :c 1 :a 2 :b))
    (testing "sorted? is true for a persisted sorted map"
      (is (sorted? @db)))
    (testing "comparator is consistent with iteration order"
      (let [^java.util.Comparator c (.comparator ^clojure.lang.Sorted @db)]
        (is (neg? (.compare c 1 2)))
        (is (pos? (.compare c 2 1)))
        (is (zero? (.compare c 2 2)))
        ;; cross-type bound checks must agree with the engine (not core/compare)
        (is (neg? (.compare c 5 "x")))))))

(deftest nth-indexed
  (with-open [db (xdb/xit-db :memory)]
    (let [oracle (into (sorted-map) (map vector (shuffle (range 20)) (range 20)))]
      (reset! db oracle)
      (let [m @db
            ov (vec oracle)]
        (testing "nth by positive index matches the oracle's entry at that rank"
          (doseq [i (range 20)]
            (is (= (nth ov i) (nth m i)) (str "nth " i))))
        (testing "negative index counts from the end (-1 = last)"
          (is (= (last ov) (nth m -1)))
          (is (= (nth ov 18) (nth m -2))))
        (testing "out-of-range nth/2 returns not-found"
          (is (= ::nf (nth m 100 ::nf)))
          (is (= ::nf (nth m -100 ::nf))))
        (testing "out-of-range nth/1 throws like a vector"
          (is (thrown? IndexOutOfBoundsException (nth m 100))))))))

(deftest subseq-matches-oracle
  (with-open [db (xdb/xit-db :memory)]
    (let [oracle (into (sorted-map) (map vector (shuffle (range 0 40 2)) (range)))]
      (reset! db oracle)
      (let [m @db]
        (doseq [k [10 11 0 38 39 -1 50]]
          (testing (str "single-bound subseq at " k)
            (is (= (subseq oracle >= k) (subseq m >= k)) (str ">= " k))
            (is (= (subseq oracle > k)  (subseq m > k))  (str "> " k))
            (is (= (subseq oracle <= k) (subseq m <= k)) (str "<= " k))
            (is (= (subseq oracle < k)  (subseq m < k))  (str "< " k))))
        (testing "two-bound subseq"
          (is (= (subseq oracle >= 10 <= 30) (subseq m >= 10 <= 30)))
          (is (= (subseq oracle > 10 < 30)   (subseq m > 10 < 30)))
          (is (= (subseq oracle >= 11 <= 29) (subseq m >= 11 <= 29))))))))

(deftest rseq-and-rsubseq-match-oracle
  (with-open [db (xdb/xit-db :memory)]
    (let [oracle (into (sorted-map) (map vector (shuffle (range 0 40 2)) (range)))]
      (reset! db oracle)
      (let [m @db]
        (testing "rseq is the full descending sequence"
          (is (= (rseq oracle) (rseq m))))
        (doseq [k [10 11 0 38 39 -1 50]]
          (testing (str "single-bound rsubseq at " k)
            (is (= (rsubseq oracle >= k) (rsubseq m >= k)) (str ">= " k))
            (is (= (rsubseq oracle > k)  (rsubseq m > k))  (str "> " k))
            (is (= (rsubseq oracle <= k) (rsubseq m <= k)) (str "<= " k))
            (is (= (rsubseq oracle < k)  (rsubseq m < k))  (str "< " k))))
        (testing "two-bound rsubseq"
          (is (= (rsubseq oracle >= 10 <= 30) (rsubseq m >= 10 <= 30)))
          (is (= (rsubseq oracle > 10 < 30)   (rsubseq m > 10 < 30)))
          (is (= (rsubseq oracle >= 11 <= 29) (rsubseq m >= 11 <= 29))))))))

(deftest empty-sorted-map-range-queries
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-map))
    (let [m @db]
      (testing "range queries on an empty (none-cursor) sorted map yield nothing"
        (is (nil? (seq m)))
        (is (nil? (rseq m)))
        (is (empty? (subseq m >= 5)))
        (is (empty? (subseq m < 5)))
        (is (empty? (rsubseq m >= 5)))
        (is (empty? (rsubseq m <= 5)))
        (is (= ::nf (nth m 0 ::nf)))))))

(deftest numeric-keys-iterate-numerically
  (testing "long keys iterate in numeric, not lexical, order"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (sorted-map 9 :a 10 :b 1 :c))
      (is (= [1 9 10] (map key (seq @db))))
      (is (= [:c :a :b] (map val (seq @db))))))
  (testing "negative and positive longs sort together, incl. extremes"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (into (sorted-map)
                       (map vector [3 -5 0 Long/MIN_VALUE Long/MAX_VALUE]
                            (range))))
      (is (= [Long/MIN_VALUE -5 0 3 Long/MAX_VALUE] (map key (seq @db))))))
  (testing "double keys sort numerically, incl. negatives and zero"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (sorted-map 3.5 :a -1.5 :b 0.0 :c 1.0e308 :d -1.0e308 :e))
      (is (= [-1.0e308 -1.5 0.0 3.5 1.0e308] (map key (seq @db)))))))

(deftest mixed-long-double-keys-interleave-numerically
  (testing "long and double keys sort by numeric value and round-trip with type"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (into (sorted-map)
                       (map vector [1 0.5 2 1.5 3 -1.5] (range))))
      (is (= [-1.5 0.5 1 1.5 2 3] (map key (seq @db))))
      (is (some #(instance? Double %) (map key (seq @db))))
      (is (some integer? (map key (seq @db))))))
  (testing "matches the in-memory Clojure sorted-map oracle for mixed keys"
    (with-open [db (xdb/xit-db :memory)]
      (let [oracle (into (sorted-map)
                         (map vector [10 2.5 7 0.25 -3 -3.5 100.0 4] (range)))]
        (reset! db oracle)
        (is (= (keys oracle) (map key (seq @db)))))))
  (testing "a double bound queries a long-keyed map at the right place (subseq)"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (into (sorted-map) (map vector [1 2 3 4 5] (range))))
      (let [m @db]
        (is (= [2 3 4 5] (map key (subseq m >= 1.5))))
        (is (= [1 2 3]   (map key (subseq m <= 3.5))))
        (is (= [3 4]     (map key (subseq m > 2.5 < 4.5))))))))

(deftest temporal-keys-iterate-chronologically
  (testing "Instant keys iterate chronologically and round-trip to Instant"
    (with-open [db (xdb/xit-db :memory)]
      (let [t0 (Instant/ofEpochSecond 100)
            t1 (Instant/ofEpochSecond 200 500)
            t2 (Instant/ofEpochSecond 200 999)]
        (reset! db (sorted-map t2 :c t0 :a t1 :b))
        (is (= [t0 t1 t2] (map key (seq @db))))
        (is (every? #(instance? Instant %) (map key (seq @db)))))))
  (testing "Date keys iterate chronologically and round-trip to Date"
    (with-open [db (xdb/xit-db :memory)]
      (let [d0 (Date. 0) d1 (Date. 1000) d2 (Date. 2000)]
        (reset! db (sorted-map d2 :c d0 :a d1 :b))
        (is (= [d0 d1 d2] (map key (seq @db))))
        (is (every? #(instance? Date %) (map key (seq @db))))))))

(deftest write-view-supports-sorted-indexed-reversible
  (testing "the writeable sorted map handed to swap! supports nth/subseq/rseq
            and exposes the same comparator as the read view"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (into (sorted-map) (map vector (range 5) (range 5))))
      (swap! db
             (fn [m]
               (is (= (clojure.lang.MapEntry. 0 0) (nth m 0)))
               (is (= 2 (key (nth m 2))))
               (is (= ::nf (nth m 99 ::nf)))
               (is (= [2 3 4] (map key (subseq m >= 2))))
               (is (= [0 1] (map key (subseq m < 2))))
               (is (= [4 3 2 1 0] (map key (rseq m))))
               (is (instance? java.util.Comparator
                              (.comparator ^clojure.lang.Sorted m)))
               m))
      (testing "the data is unchanged after read-only queries in the txn"
        (is (= [0 1 2 3 4] (map key (seq @db))))))))

(deftest tracer-bullet-ordered-seq
  (testing "a persisted sorted-map is stored as a sorted map and seqs in key order"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (sorted-map "b" 2 "a" 1))
      (is (instance? xitdb.sorted_map.XITDBSortedMap @db))
      (is (= [["a" 1] ["b" 2]] (map (juxt key val) (seq @db))))))
  (testing "ordering holds for many keys regardless of insertion order"
    (with-open [db (xdb/xit-db :memory)]
      (let [ks (map #(format "k%04d" %) (shuffle (range 50)))]
        (reset! db (into (sorted-map) (map vector ks (range))))
        (is (= (sort ks) (map key (seq @db))))))))

(deftest nested-values-fetched-in-txn-are-writable
  (testing "in-place mutation of a nested map fetched via get persists,
            matching the write-hash-map behaviour"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (sorted-map "inner" {:x 1}))
      (swap! db (fn [m]
                  (assoc (get m "inner") :y 2)
                  m))
      (is (= {:x 1 :y 2} (tu/materialize (get @db "inner")))))))

(deftest txn-lookup-of-absent-key-does-not-create-it
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-map "a" 1))
    (swap! db (fn [m]
                (is (= ::nf (get m "missing" ::nf)))
                (is (nil? (get m "missing")))
                m))
    (is (= 1 (count @db)))
    (is (= ["a"] (map key (seq @db))))))

(deftest nested-sorted-collections-fetched-in-txn-are-writable
  (testing "a sorted map nested in a sorted map mutates in place via get"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (sorted-map "inner" (sorted-map 1 :a)))
      (swap! db (fn [m]
                  (assoc (get m "inner") 0 :z)
                  m))
      (is (= [[0 :z] [1 :a]] (map (juxt key val) (seq (get @db "inner")))))))
  (testing "a sorted set nested in a sorted map mutates in place via get"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (sorted-map "tags" (sorted-set 2 3)))
      (swap! db (fn [m]
                  (conj (get m "tags") 1)
                  m))
      (is (= [1 2 3] (seq (get @db "tags")))))))

(deftest nil-key-lookups-return-not-found
  (testing "on the read view, like Clojure's sorted-map (nil is never present)"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (sorted-map "a" 1))
      (let [m @db]
        (is (nil? (get m nil)))
        (is (= ::nf (get m nil ::nf)))
        (is (false? (contains? m nil)))
        (is (nil? (find m nil))))))
  (testing "on the write view inside a transaction"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (sorted-map "a" 1))
      (swap! db (fn [m]
                  (is (= ::nf (get m nil ::nf)))
                  (is (false? (contains? m nil)))
                  (is (nil? (find m nil)))
                  m)))))

(deftest dissoc-nil-key-is-a-no-op
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-map "a" 1))
    (swap! db dissoc nil)
    (is (= [["a" 1]] (map (juxt key val) (seq @db))))))

(deftest storing-nil-key-throws-with-clear-message
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-map "a" 1))
    (is (thrown-with-msg? IllegalArgumentException #"nil"
                          (swap! db assoc nil 1)))))

(deftest boolean-keys-round-trip-in-order
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-map true :t false :f))
    (is (= [[false :f] [true :t]] (map (juxt key val) (seq @db))))
    (is (= :t (get @db true)))
    (is (= :f (get @db false)))))

(deftest uuid-and-char-keys-round-trip-in-compareTo-order
  (testing "UUID keys iterate in UUID.compareTo order and round-trip"
    (with-open [db (xdb/xit-db :memory)]
      (let [us [#uuid "80000000-0000-0000-0000-000000000000"
                #uuid "00000000-0000-0000-0000-000000000001"
                #uuid "ffffffff-0000-0000-0000-000000000000"
                #uuid "123e4567-e89b-12d3-a456-426614174000"]
            oracle (into (sorted-map) (map vector us (range)))]
        (reset! db oracle)
        (is (= (keys oracle) (map key (seq @db))))
        (is (every? #(instance? java.util.UUID %) (map key (seq @db))))
        (is (= (get oracle (first us)) (get @db (first us)))))))
  (testing "char keys iterate in char order and round-trip"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (sorted-map \c 3 \a 1 \B 2))
      (is (= [\B \a \c] (map key (seq @db))))
      (is (= 1 (get @db \a))))))
