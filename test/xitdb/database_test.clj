(ns xitdb.database-test
  (:require
    [clojure.test :refer :all]
    [xitdb.test-utils :as tu :refer [with-db]]))

(comment
  (let [db (tu/test-memory-db-raw)]
    (reset! db {:foo :bar})
    @db))

(deftest DatabaseTest
  (with-db [db (tu/test-db)]
    (testing "Resetting to map"
      (reset! db {:foo :bar})
      (is (= {:foo :bar} @db))

      (reset! db {:foo {:bar :Baz}})
      (is (= {:foo {:bar :Baz}} @db))

      (reset! db {:foo {:bar {:some "baz"}}})
      (is (= {:foo {:bar {:some "baz"}}} @db))

      (swap! db assoc-in [:foo :bar :some] [1 2 3 4])
      (is (= {:foo {:bar {:some [1 2 3 4]}}} @db))

      (swap! db assoc-in [:foo :bar :some] -42)
      (is (= (:foo {:bar {:some -42}})))

      (is (tu/db-equal-to-atom? db)))))

(deftest get-in-test
  (with-db [db (tu/test-memory-db-raw)]
    (reset! db {:foo {:bar :baz}})
    (is (= :baz (get-in @db [:foo :bar])))
    (reset! db {"foo" {:bar {1 :baz}}})
    (is (= :baz (get-in @db ["foo" :bar 1])))

    (testing "Negative keys and values"
      (reset! db {-999 {:bar -42}})
      (is (= {-999 {:bar -42}} (tu/materialize @db)))
      (is (= -42 (get-in @db [-999 :bar])))

      (reset! db [-39 23 "foo" :foo])
      (is (= -39 (get-in @db [0])))
      (is (= 23 (get-in @db [1])))
      (is (= "foo" (get-in @db [2])))
      (is (= :foo (get-in @db [3]))))

    (testing "Float values"
      (reset! db {-34 0.345542})
      (is (= @db {-34 0.345542}))
      (swap! db assoc -424 3.949494958483)
      (is (= {-34 0.345542, -424 3.949494958483}
             (tu/materialize @db))))))

(deftest array-reset-test
  (with-db [db (tu/test-db)]
    (testing "Resetting to array"
      ;; Start with a fresh database and reset to vector
      (reset! db [1 2 3 4])
      (is (= [1 2 3 4] @db))

      ;; Test accessing elements
      (is (= 3 (get @db 2)))

      ;; Test updating an element
      (swap! db assoc-in [1] 20)
      (is (= [1 20 3 4] @db))

      ;; Test adding elements
      (swap! db assoc-in [4] 5)
      (is (= [1 20 3 4 5] @db))
      (is (tu/db-equal-to-atom? db)))))

(deftest map-corner-cases-test
  (with-db [db (tu/test-db)]

    (testing "Empty map operations"
      (reset! db {})
      (is (= {} @db))
      (swap! db assoc-in [:foo] "bar")
      (is (= {:foo "bar"} @db)))

    (testing "Nested empty collections"
      (reset! db {:empty-map {} :empty-vec []})
      (is (= {:empty-map {} :empty-vec []} @db))
      (swap! db assoc-in [:empty-map :key] "value")
      (is (= {:empty-map {:key "value"} :empty-vec []} @db)))

    (testing "Special keys"
      (reset! db {})
      (swap! db assoc-in [:ns/keyword] "namespaced")
      (swap! db assoc-in ["string-key"] "string")
      (is (= {:ns/keyword "namespaced" "string-key" "string"} @db)))

    (testing "Creating nested paths"
      (reset! db {})
      (swap! db assoc-in [:a :b :c :d] "deep")
      (is (= {:a {:b {:c {:d "deep"}}}} @db)))

    (testing "Replacing data types"
      (reset! db {:foo {:bar "string"}})
      (swap! db assoc-in [:foo :bar] [1 2 3])
      (is (= {:foo {:bar [1 2 3]}} @db))
      (swap! db assoc-in [:foo :bar] {:nested "map"})
      (is (= {:foo {:bar {:nested "map"}}} @db)))

    (testing "Numeric and boolean keys"
      (reset! db {})
      (swap! db assoc-in [1] "numeric")

      (is (= {1 "numeric"} @db) "Keys are preserved")
      (swap! db assoc-in [true] "boolean")
      (is (= {1 "numeric" true "boolean"} @db)))
    (is (tu/db-equal-to-atom? db))))

(deftest SwapTest
  (with-db [db (tu/test-db)]
    (reset! db {:foo :bar})
    (is (= {:foo :bar} @db))

    (testing "arity-1 assoc"
      (swap! db #(assoc % :some 43))
      (is (= {:foo :bar :some 43} @db)))

    (testing "arity-2 assoc"
      (swap! db assoc :some 44)
      (is (= {:foo :bar :some 44} @db)))

    (testing "arity-3 assoc"
      (reset! db {:users {"1" {:name "john"}}})
      (is (= {:users {"1" {:name "john"}}} @db))

      (swap! db assoc-in [:users "1" :age] 44)
      (is (= {:users {"1" {:name "john" :age 44}}} @db)))

    (testing "dissoc"
      (reset! db {:users {"1" {:name "john"}}})
      (swap! db dissoc :users)
      (is (= {} @db))

      (reset! db {:users [] :foo :stays})
      (swap! db dissoc :users)
      (is (= {:foo :stays} @db)))

    (testing "more dissoc"
      (reset! db {:users {"1" {:name "john"}}})
      (swap! db dissoc :users)
      (is (= {} @db)))
    (is (tu/db-equal-to-atom? db))))

(deftest SwapArray
  (with-db [db (tu/test-db)]
    (testing "assoc"
      (reset! db [1 2 3])
      (swap! db #(assoc % 0 44))
      (is (= [44 2 3] @db))

      (swap! db #(assoc % 3 99))
      (is (= [44 2 3 99] @db)))

    (testing "conj"
      (reset! db [1 2 3])
      (swap! db conj 55)
      (is (= [1 2 3 55] @db)))

    (testing "assoc-in"
      (reset! db [1 2 {:title "Untitled"} 3 4])
      (swap! db assoc-in [2 :title] "Titled")
      (is (= [1 2 {:title "Titled"} 3 4] @db)))

    (testing "update-in"
      (reset! db [1 2 {:users [{:name "jp"}
                               {:name "cj"}]} 3 4])
      (swap! db update-in [2 :users] conj {:name "fl"})
      (is (= [1 2 {:users [{:name "jp"} {:name "cj"} {:name "fl"}]} 3 4]
             @db)))

    (testing "update-in array"
      (reset! db [1 2 [{:name "jp"} {:name "cj"}] 3 4])
      (swap! db update-in [2 1 :name ] str "-foo")
      (is (= [1 2 [{:name "jp"} {:name "cj-foo"}] 3 4] @db))
      (swap! db update-in [2 1] dissoc :name)
      (is (= [1 2 [{:name "jp"} {}] 3 4] @db))
      (swap! db update-in [2] butlast)
      (is (= [1 2 [{:name "jp"}] 3 4] @db)))

    (is (tu/db-equal-to-atom? db))))

(deftest AssocInTest
  (with-db [db (tu/test-db)]
    (testing "assoc-in more"
      (reset! db [1 2 {:users [{:name "jp"}
                               {:name "cj"}]} 3 4])
      (swap! db assoc-in [2 :users 1 :name] "maria")
      (is (= [1 2 {:users [{:name "jp"} {:name "maria"}]} 3 4]
             @db))
      (is (tu/db-equal-to-atom? db)))))

(deftest MergeTest
  (testing "Basic merges"
    (with-db [db (tu/test-db)]
      (reset! db {"1" {:name "jp"}
                  "2" {:name "cj"}})

      (swap! db merge {"3" {:name "maria"}})

      (is (= {"1" {:name "jp"}
              "2" {:name "cj"}
              "3" {:name "maria"}}
             @db))

      (swap! db merge {:foo [:bar]})
      (is (= {"1" {:name "jp"}
              "2" {:name "cj"}
              "3" {:name "maria"}
              :foo [:bar]}
             @db))
      (is (tu/db-equal-to-atom? db))))
  (testing "merge-with"
    (with-db [db (tu/test-db)]
      (reset! db {"1" {:foo1 {:bar :baz}}})
      (swap! db merge {"2" {:foo2 :bar2}})
      (swap! db merge {"3" {:foo3 :bar3}})
      (swap! db merge {"4" [1 2 3]})
      (swap! db (fn [a]
                  (merge-with into a {"4" [44]})))
      (is (= {"2" {:foo2 :bar2}
              "4" [1 2 3 44]
              "1" {:foo1 {:bar :baz}}
              "3" {:foo3 :bar3}}
             (tu/materialize @db))))))

(deftest IntoTest
  (with-db [db (tu/test-db)]
    (reset! db [0 1 2 3 4])
    (swap! db #(filterv even? %))
    (is (= [0 2 4]
           @db))))

;;; Tests below were AI generated

(deftest UpdateTest
  (with-db [db (tu/test-db)]
    (testing "update map value"
      (reset! db {:count 5 :name "test"})
      (swap! db update :count inc)
      (is (= {:count 6 :name "test"} @db)))

    (testing "update with multiple args"
      (reset! db {:list [1 2 3]})
      (swap! db update :list conj 4 5)
      (is (= {:list [1 2 3 4 5]} @db)))

    (is (tu/db-equal-to-atom? db))))

(deftest SelectKeysTest
  (with-db [db (tu/test-db)]
    (reset! db {:a 1 :b 2 :c 3 :d 4})
    (swap! db select-keys [:a :c])
    (is (= {:a 1 :c 3} @db))
    (is (tu/db-equal-to-atom? db))))

(deftest FilterRemoveTest
  (with-db [db (tu/test-db)]
    (testing "filter"
      (reset! db {:a 1 :b 2 :c 3 :d 4})
      (swap! db #(into {} (filter (fn [[_ v]] (even? v)) %)))
      (is (= {:b 2 :d 4} @db)))

    (testing "remove with vector"
      (reset! db [1 2 3 4 5])
      (swap! db #(vec (remove odd? %)))
      (is (= [2 4] @db)))

    (is (tu/db-equal-to-atom? db))))

(deftest MapReduceTest
  (with-db [db (tu/test-db)]
    (testing "map over vector"
      (reset! db [1 2 3 4])
      (swap! db #(vec (map inc %)))
      (is (= [2 3 4 5] @db)))

    (testing "map over map values"
      (reset! db {:a 1 :b 2})
      (swap! db #(zipmap (keys %) (map inc (vals %))))
      (is (= {:a 2 :b 3} @db)))

    (testing "reduce"
      (reset! db [1 2 3 4 5])
      (swap! db #(vector (reduce + %)))
      (is (= [15] @db)))

    (is (tu/db-equal-to-atom? db))))

(deftest SequenceOpsTest
  (with-db [db (tu/test-db)]
    (testing "concat"
      (reset! db [1 2 3])
      (swap! db #(vec (concat % [4 5 6])))
      (is (= [1 2 3 4 5 6] @db)))

    (testing "take/drop"
      (reset! db [1 2 3 4 5])
      (swap! db #(vec (take 3 %)))
      (is (= [1 2 3] @db))

      (reset! db [1 2 3 4 5])
      (swap! db #(vec (drop 2 %)))
      (is (= [3 4 5] @db)))

    (testing "partition"
      (reset! db [1 2 3 4 5 6])
      (swap! db #(vec (map vec (partition 2 %))))
      (is (= [[1 2] [3 4] [5 6]] @db)))

    (is (tu/db-equal-to-atom? db))))

(deftest EmptyOnArray
  (with-db [db (tu/test-db)]
    (reset! db [1 2 3])
    (swap! db empty)
    (is (= [] @db))
    (reset! db {:one [1 2 4]})
    (swap! db update :one empty)
    (is (= {:one []} @db))
    @db))

(deftest MiscOpsTest
  (with-db [db (tu/test-db)]
    (testing "empty"
      (reset! db {:a 1 :b 2})
      (swap! db empty)
      (is (= {} @db))

      (reset! db [1 2 3])
      (swap! db empty)
      (is (= [] @db)))

    (is (tu/db-equal-to-atom? db))))

(deftest JuxtTest
  (with-db [db (tu/test-db)]
    (testing "juxt"
      (reset! db {:users [{:name "John" :age 30} {:name "Alice" :age 25}]})
      (swap! db update-in [:users] #(mapv (juxt :name :age) %))
      (is (= {:users [["John" 30] ["Alice" 25]]} @db)))
    (is (tu/db-equal-to-atom? db))))

(deftest EmptyTest
  (with-db [db (tu/test-db)]
    (reset! db {})
    (is (empty? @db))

    (reset! db [])
    (is (empty? @db))

    (swap! db conj 1)
    (is (false? (empty? @db)))

    (swap! db butlast)
    (is (empty? @db))

    (is (tu/db-equal-to-atom? db))))

(deftest CountTest
  (with-db [db (tu/test-memory-db-raw)]
    (reset! db {:a :b :c :d :e :f})
    (is (= 3 (count @db)))

    (swap! db assoc :x :y)
    (is (= 4 (count @db)))

    (swap! db dissoc :x :a :b :c :e)
    (is (= 0 (count @db)))
    (is (empty? @db))

    (is (tu/db-equal-to-atom? db))))

(comment
  (with-db [db (tu/test-memory-db-raw)]
    (reset! db {:raw-songs-swap [1 2 3 4]})
    (swap! db update :raw-songs-swap into [5 6 7])
    nil))


(deftest NilTest
  (testing "nil values"
    (with-db [db (tu/test-db)]
      (reset! db {:one nil})
      (is (= {:one nil} @db))

      (swap! db update :one conj 1)
      (is (= {:one [1]} @db))

      (reset! db [1 nil nil 2 3 nil])
      (is (= [1 nil nil 2 3 nil] @db))

      (reset! db nil)
      (is (= nil @db))
      @db))

  (testing "nil keys"
    (with-db [db (tu/test-db)]
      (reset! db {:one nil})
      (is (= {:one nil} @db))
      (swap! db merge {nil nil})

      (is (= {:one nil nil nil} @db))
      (is (= {nil nil} (select-keys @db [nil]))))))

(deftest InstAndDateTest
  (with-db [db (tu/test-db)]
    (let [d (java.util.Date.)
          instant (.toInstant d)]
      (reset! db {:foo instant
                  :bar d})
      (is (= instant (:foo @db)))
      (is (= d (:bar @db)))

      (is (instance? java.util.Date (:bar @db))))))

(deftest IntoEfficiency
  (with-db [db (tu/test-db)]
    (reset! db [1 2 3])
    (swap! db into [4 5])
    (swap! db into [5 6])
    (is (= [1 2 3 4 5 5 6] @db))))

(deftest LazySeqTest
  (with-db [db (tu/test-db)]
    (reset! db '(1 2 3))
    (swap! db conj 44)

    (testing "Throws on lazy seqs by concat"
      (is (thrown? IllegalArgumentException (swap! db concat [1 2])))
      (swap! db (comp vec concat) [1 2])
      (is (= [44 1 2 3 1 2] @db))
      (swap! db (comp seq concat) [99])
      (is (= '(44 1 2 3 1 2 99) @db)))

    (testing "Throws on take or drop"
      (is (thrown? IllegalArgumentException (swap! db #(drop 3 %))))
      (is (thrown? IllegalArgumentException (swap! db #(take 3 %))))
      (swap! db #(vec (drop 3 %)))
      (is (= @db [3 1 2 99])))))

(deftest PopTest
  (with-db [db (tu/test-db)]
    (reset! db '(1 2 3 4 5))
    (swap! db pop)
    (is (= '(2 3 4 5) @db))
    (is (tu/db-equal-to-atom? db))))





