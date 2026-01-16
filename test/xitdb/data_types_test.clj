(ns xitdb.data-types-test
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]
    [xitdb.common :as common]
    [xitdb.test-utils :as tu :refer [with-db]]))


(deftest KeywordsAndStrings
  (testing "Should correctly handle key and string keys and values"
    (with-db [db (tu/test-db)]
      (reset! db {:foo "foo"
                  "foo" "more-foo"
                  ":foo" :foo})
      (is (= {:foo "foo"
              "foo" "more-foo"
              ":foo" :foo} @db))
      (swap! db dissoc "foo" ":foo")
      (is (= {:foo "foo"} @db))
      (swap! db dissoc :foo)
      (is (= {} @db)))))

(deftest KeywordsAndStringsInSets
  (testing "Should correctly handle keywords and strings in sets"
    (with-db [db (tu/test-db)]
      (reset! db #{:foo "foo" ":foo"})
      (is (= #{:foo "foo" ":foo"} @db))

      (swap! db conj :bar)
      (is (= #{:foo "foo" ":foo" :bar} @db))

      (swap! db disj :foo ":foo")
      (is (= #{"foo" :bar} @db))

      (swap! db disj "foo")
      (is (= #{:bar} @db)))))

(deftest KeywordsAndStringsInVectors
  (testing "Should correctly handle keywords and strings in vectors"
    (with-db [db (tu/test-db)]
      (reset! db [:foo "foo" ":foo"])
      (is (= [:foo "foo" ":foo"] @db))

      (swap! db conj :bar)
      (is (= [:foo "foo" ":foo" :bar] @db))

      (swap! db assoc 0 "changed")
      (is (= ["changed" "foo" ":foo" :bar] @db))

      (swap! db #(into [] (remove #{":foo"} %)))
      (is (= ["changed" "foo" :bar] @db)))))

(deftest KeywordsAndStringsInLists
  (testing "Should correctly handle keywords and strings in lists"
    (with-db [db (tu/test-db)]
      (reset! db '(:foo "foo" ":foo"))
      (is (= '(:foo "foo" ":foo") @db))

      (swap! db conj :bar)
      (is (= '(:bar :foo "foo" ":foo") @db)))))

(deftest NilValuesInMaps
  (testing "Should correctly handle nil values and distinguish from missing keys"
    (with-db [db (tu/test-db)]
      ;; Test nil values vs missing keys
      (reset! db {:existing-key nil :other-key "value"})
      (is (contains? @db :existing-key))
      (is (= nil (get @db :existing-key)))
      (is (= nil (get @db :missing-key)))
      (is (not (contains? @db :missing-key)))
      
      ;; Test entryAt with nil values
      (is (= [:existing-key nil] (find @db :existing-key)))
      (is (= nil (find @db :missing-key)))
      
      ;; Test nil as a key
      (reset! db {nil "nil-value" :other "other-value"})
      (is (contains? @db nil))
      (is (= "nil-value" (get @db nil)))
      (is (= [nil "nil-value"] (find @db nil))))))

;; =============================================================================
;; Materialize tests
;; =============================================================================

(deftest materialize-shallow-map-test
  (testing "materialize-shallow on XITDBHashMap returns a regular map with XITDB values"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:outer {:inner {:deep "value"}}})

      (let [db-val @db
            shallow (common/-materialize-shallow db-val)]
        ;; Should be a regular clojure map
        (is (map? shallow))
        (is (not (instance? xitdb.hash_map.XITDBHashMap shallow)))

        ;; But inner values may still be XITDB types
        (is (= :inner (first (keys (:outer shallow)))))))))

(deftest materialize-shallow-vector-test
  (testing "materialize-shallow on XITDBArrayList returns a regular vector"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db [[1 2] [3 4] [5 6]])

      (let [db-val @db
            shallow (common/-materialize-shallow db-val)]
        ;; Should be a regular clojure vector
        (is (vector? shallow))
        (is (not (instance? xitdb.array_list.XITDBArrayList shallow)))

        ;; Values inside are preserved
        (is (= 3 (count shallow)))))))

(deftest materialize-shallow-set-test
  (testing "materialize-shallow on XITDBHashSet returns a regular set"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db #{:a :b :c})

      (let [db-val @db
            shallow (common/-materialize-shallow db-val)]
        ;; Should be a regular clojure set
        (is (set? shallow))
        (is (not (instance? xitdb.hash_set.XITDBHashSet shallow)))

        ;; Values are preserved
        (is (= #{:a :b :c} shallow))))))

(deftest materialize-shallow-list-test
  (testing "materialize-shallow on XITDBLinkedArrayList returns a vector"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db '(1 2 3))

      (let [db-val @db
            shallow (common/-materialize-shallow db-val)]
        ;; Should be a list
        (is (list? shallow))

        ;; Values are preserved
        (is (= [1 2 3] shallow))))))

(deftest materialize-vs-materialize-shallow-test
  (testing "materialize recursively materializes while materialize-shallow does not"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:level1 {:level2 {:level3 "deep"}}})

      (let [db-val @db
            full (common/materialize db-val)
            shallow (common/-materialize-shallow db-val)]
        ;; Full materialization returns plain clojure maps all the way down
        (is (= {:level1 {:level2 {:level3 "deep"}}} full))
        (is (map? (get full :level1)))
        (is (map? (get-in full [:level1 :level2])))

        ;; Shallow materialization only materializes the outer layer
        (is (map? shallow))
        (is (contains? shallow :level1))))))

;; =============================================================================
;; Read-only vector/list operations returning Clojure types
;; =============================================================================

(deftest read-only-vector-assoc-test
  (testing "assoc on XITDBArrayList returns a regular vector"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db [1 2 3])
      (let [db-val @db
            result (assoc db-val 1 20)]
        ;; Result should be a regular Clojure vector
        (is (vector? result))
        (is (not (instance? xitdb.array_list.XITDBArrayList result)))
        (is (= [1 20 3] result))))))

(deftest read-only-vector-cons-test
  (testing "cons on XITDBArrayList returns a seq with element prepended"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db [1 2 3])
      (let [db-val @db
            result (cons 0 db-val)]
        (is (= '(0 1 2 3) result))))))

(deftest read-only-list-cons-test
  (testing "cons on XITDBLinkedArrayList prepends element"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db '(2 3 4))
      (let [db-val @db
            result (cons 1 db-val)]
        (is (= '(1 2 3 4) result))))))

;; =============================================================================
;; Additional nil tests
;; =============================================================================

(deftest nil-in-map-storage-test
  (testing "nil values in maps are stored and retrieved correctly"
    (with-db [db (tu/test-db)]
      (reset! db {:a nil :b "value" :c nil})
      (is (= {:a nil :b "value" :c nil} @db))
      (is (nil? (get @db :a)))
      (is (nil? (get @db :c)))
      (is (= "value" (get @db :b))))))

(deftest nil-in-vector-storage-test
  (testing "nil values in vectors are stored and retrieved correctly"
    (with-db [db (tu/test-db)]
      (reset! db [nil 1 nil 2 nil])
      (is (= [nil 1 nil 2 nil] @db))
      (is (nil? (nth @db 0)))
      (is (nil? (nth @db 2)))
      (is (nil? (nth @db 4)))
      (is (= 1 (nth @db 1)))
      (is (= 2 (nth @db 3))))))

(deftest nil-in-nested-structures-test
  (testing "nil values in nested structures work correctly"
    (with-db [db (tu/test-db)]
      (reset! db {:outer {:inner nil}})
      (is (= {:outer {:inner nil}} @db))
      (is (nil? (get-in @db [:outer :inner])))

      (swap! db assoc-in [:outer :inner] {:deep nil})
      (is (nil? (get-in @db [:outer :inner :deep]))))))

(deftest nil-as-map-key-storage-test
  (testing "nil can be used as a map key"
    (with-db [db (tu/test-db)]
      (reset! db {nil "null-key-value" :other "other-value"})
      (is (= "null-key-value" (get @db nil)))
      (is (= "other-value" (get @db :other))))))

(deftest swap-with-nil-test
  (testing "swap! operations with nil values"
    (with-db [db (tu/test-db)]
      (reset! db {:value 1})
      (swap! db assoc :value nil)
      (is (= {:value nil} @db))

      (swap! db assoc :another nil)
      (is (= {:value nil :another nil} @db)))))

(deftest complex-nil-operations-test
  (testing "complex operations involving nil values"
    (with-db [db (tu/test-db)]
      ;; Start with nil values
      (reset! db {:users [{:name "Alice" :age nil}
                          {:name nil :age 30}
                          nil]})

      ;; Verify structure
      (is (nil? (get-in @db [:users 0 :age])))
      (is (nil? (get-in @db [:users 1 :name])))
      (is (nil? (get-in @db [:users 2])))

      ;; Update nil to non-nil
      (swap! db assoc-in [:users 0 :age] 25)
      (is (= 25 (get-in @db [:users 0 :age])))

      ;; Update non-nil to nil
      (swap! db assoc-in [:users 1 :name] nil)
      (is (nil? (get-in @db [:users 1 :name]))))))
