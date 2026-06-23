(ns xitdb.sorted-set-test
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]
    [xitdb.test-utils :as tu :refer [with-db]])
  (:import
    [java.time Instant]
    [java.util Date]))

(deftest tracer-bullet-ordered-seq
  (testing "a persisted sorted-set is stored as a sorted set and seqs in order"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (sorted-set 3 1 2))
      (is (instance? xitdb.sorted_set.XITDBSortedSet @db))
      (is (= [1 2 3] (seq @db))))))

(deftest membership-and-count
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-set 1 2 3))
    (let [s @db]
      (testing "contains? / get / invoke"
        (is (true? (contains? s 2)))
        (is (false? (contains? s 9)))
        (is (= 2 (get s 2)))
        (is (nil? (get s 9)))
        (is (= 3 (s 3)))
        (is (nil? (s 9))))
      (testing "count is correct and O(1)"
        (is (= 3 (count s)))))))

(deftest mutation-keeps-order
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-set 3 1))
    (testing "conj inserts in order"
      (swap! db conj 5)
      (swap! db conj 2)
      (is (= [1 2 3 5] (seq @db))))
    (testing "disj removes and preserves order"
      (swap! db disj 3)
      (is (= [1 2 5] (seq @db))))
    (testing "conj of a duplicate is a no-op and does not change count"
      (swap! db conj 2)
      (is (= 3 (count @db)))
      (is (= [1 2 5] (seq @db))))))

(deftest materialize-returns-plain-sorted-set
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-set 3 1 2))
    (let [s (tu/materialize @db)]
      (is (sorted? s))
      (is (not (instance? xitdb.sorted_set.XITDBSortedSet s)))
      (is (= [1 2 3] (seq s)))
      (is (= #{1 2 3} s)))))

(deftest read-only-ops-return-plain-collections
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-set 1 2))
    (let [s @db]
      (testing "conj outside a transaction returns a plain sorted set"
        (let [r (conj s 3)]
          (is (not (instance? xitdb.sorted_set.XITDBSortedSet r)))
          (is (sorted? r))
          (is (= [1 2 3] (seq r)))))
      (testing "disj outside a transaction returns a plain sorted set"
        (let [r (disj s 1)]
          (is (not (instance? xitdb.sorted_set.XITDBSortedSet r)))
          (is (sorted? r))
          (is (= [2] (seq r))))))))

(deftest sorted-predicate-and-comparator
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-set 3 1 2))
    (testing "sorted? is true for a persisted sorted set"
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
    (let [oracle (into (sorted-set) (shuffle (range 20)))]
      (reset! db oracle)
      (let [s @db
            ov (vec oracle)]
        (testing "nth by positive index matches the oracle's member at that rank"
          (doseq [i (range 20)]
            (is (= (nth ov i) (nth s i)) (str "nth " i))))
        (testing "negative index counts from the end (-1 = last)"
          (is (= (last ov) (nth s -1)))
          (is (= (nth ov 18) (nth s -2))))
        (testing "out-of-range nth/2 returns not-found"
          (is (= ::nf (nth s 100 ::nf)))
          (is (= ::nf (nth s -100 ::nf))))
        (testing "out-of-range nth/1 throws like a vector"
          (is (thrown? IndexOutOfBoundsException (nth s 100))))))))

(deftest subseq-matches-oracle
  (with-open [db (xdb/xit-db :memory)]
    (let [oracle (into (sorted-set) (shuffle (range 0 40 2)))]
      (reset! db oracle)
      (let [s @db]
        (doseq [k [10 11 0 38 39 -1 50]]
          (testing (str "single-bound subseq at " k)
            (is (= (subseq oracle >= k) (subseq s >= k)) (str ">= " k))
            (is (= (subseq oracle > k)  (subseq s > k))  (str "> " k))
            (is (= (subseq oracle <= k) (subseq s <= k)) (str "<= " k))
            (is (= (subseq oracle < k)  (subseq s < k))  (str "< " k))))
        (testing "two-bound subseq"
          (is (= (subseq oracle >= 10 <= 30) (subseq s >= 10 <= 30)))
          (is (= (subseq oracle > 10 < 30)   (subseq s > 10 < 30)))
          (is (= (subseq oracle >= 11 <= 29) (subseq s >= 11 <= 29))))))))

(deftest rseq-and-rsubseq-match-oracle
  (with-open [db (xdb/xit-db :memory)]
    (let [oracle (into (sorted-set) (shuffle (range 0 40 2)))]
      (reset! db oracle)
      (let [s @db]
        (testing "rseq is the full descending sequence"
          (is (= (rseq oracle) (rseq s))))
        (doseq [k [10 11 0 38 39 -1 50]]
          (testing (str "single-bound rsubseq at " k)
            (is (= (rsubseq oracle >= k) (rsubseq s >= k)) (str ">= " k))
            (is (= (rsubseq oracle > k)  (rsubseq s > k))  (str "> " k))
            (is (= (rsubseq oracle <= k) (rsubseq s <= k)) (str "<= " k))
            (is (= (rsubseq oracle < k)  (rsubseq s < k))  (str "< " k))))
        (testing "two-bound rsubseq"
          (is (= (rsubseq oracle >= 10 <= 30) (rsubseq s >= 10 <= 30)))
          (is (= (rsubseq oracle > 10 < 30)   (rsubseq s > 10 < 30)))
          (is (= (rsubseq oracle >= 11 <= 29) (rsubseq s >= 11 <= 29))))))))

(deftest empty-sorted-set-range-queries
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-set))
    (let [s @db]
      (testing "range queries on an empty (none-cursor) sorted set yield nothing"
        (is (nil? (seq s)))
        (is (nil? (rseq s)))
        (is (empty? (subseq s >= 5)))
        (is (empty? (subseq s < 5)))
        (is (empty? (rsubseq s >= 5)))
        (is (empty? (rsubseq s <= 5)))
        (is (= ::nf (nth s 0 ::nf)))))))

(deftest member-types-iterate-in-natural-order
  (testing "string members iterate lexicographically"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (sorted-set "banana" "apple" "cherry"))
      (is (= ["apple" "banana" "cherry"] (seq @db)))
      (is (every? string? (seq @db)))))
  (testing "keyword members iterate in natural order and round-trip"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (sorted-set :c :a :b))
      (is (= [:a :b :c] (seq @db)))
      (is (every? keyword? (seq @db)))))
  (testing "long members iterate numerically, incl. extremes"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (into (sorted-set) [3 -5 0 Long/MIN_VALUE Long/MAX_VALUE]))
      (is (= [Long/MIN_VALUE -5 0 3 Long/MAX_VALUE] (seq @db)))))
  (testing "double members iterate numerically, incl. negatives and zero"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (sorted-set 3.5 -1.5 0.0 1.0e308 -1.0e308))
      (is (= [-1.0e308 -1.5 0.0 3.5 1.0e308] (seq @db)))))
  (testing "Instant members iterate chronologically and round-trip to Instant"
    (with-open [db (xdb/xit-db :memory)]
      (let [t0 (Instant/ofEpochSecond 100)
            t1 (Instant/ofEpochSecond 200 500)
            t2 (Instant/ofEpochSecond 200 999)]
        (reset! db (sorted-set t2 t0 t1))
        (is (= [t0 t1 t2] (seq @db)))
        (is (every? #(instance? Instant %) (seq @db))))))
  (testing "Date members iterate chronologically and round-trip to Date"
    (with-open [db (xdb/xit-db :memory)]
      (let [d0 (Date. 0) d1 (Date. 1000) d2 (Date. 2000)]
        (reset! db (sorted-set d2 d0 d1))
        (is (= [d0 d1 d2] (seq @db)))
        (is (every? #(instance? Date %) (seq @db)))))))

(deftest custom-comparator-rejected
  (with-open [db (xdb/xit-db :memory)]
    (is (thrown? IllegalArgumentException
                 (reset! db (sorted-set-by > 1 2 3))))))

(deftest print-method-ordered
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-set 3 1 2))
    (let [s (pr-str @db)]
      (is (clojure.string/starts-with? s "#XITDBSortedSet"))
      (is (clojure.string/includes? s "1 2 3")))))

(deftest nesting-and-round-trip
  (testing "sorted set nests inside a hash map value"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:idx (sorted-set 3 1 2)})
      (is (instance? xitdb.sorted_set.XITDBSortedSet (:idx @db)))
      (is (= [1 2 3] (seq (:idx @db))))))
  (testing "nested sorted set round-trips against an in-memory atom"
    (with-db [db (tu/test-db)]
      (reset! db {:idx (sorted-set 3 1 2)})
      (is (tu/db-equal-to-atom? db))))
  (testing "a sorted set nested directly inside a vector stays sorted"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db [(sorted-set 30 10 20)])
      (let [s (first (seq @db))]
        (is (instance? xitdb.sorted_set.XITDBSortedSet s))
        (is (sorted? s))
        (is (= [10 20 30] (seq s))))))
  (testing "empty clears the set in place"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (sorted-set 1 2 3))
      (swap! db empty)
      (is (= 0 (count @db)))
      (is (empty? (seq @db)))
      (swap! db conj 7)
      (is (= [7] (seq @db))))))
