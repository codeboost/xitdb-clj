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
      (is (= #{:a :b} (tu/materialize (get @db "set")))))))

(deftest empty-clears-map
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-map "a" 1 "b" 2))
    (swap! db empty)
    (is (= 0 (count @db)))
    (is (empty? (seq @db)))
    (swap! db assoc "c" 3)
    (is (= ["c"] (map key (seq @db))))))

(deftest print-method-ordered
  (with-open [db (xdb/xit-db :memory)]
    (reset! db (sorted-map "b" 2 "a" 1))
    (let [s (pr-str @db)]
      (is (clojure.string/starts-with? s "#XITDBSortedMap"))
      (is (clojure.string/includes? s "\"a\" 1, \"b\" 2")))))

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
