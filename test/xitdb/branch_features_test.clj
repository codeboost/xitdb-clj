(ns xitdb.branch-features-test
  "Tests for features added in the current branch:
   - deref-at: access historical versions
   - materialize-shallow: shallow materialization
   - ISlot implementations for read-only types
   - reset! using slot if available
   - nil storage as Tag/NONE
   - read-only types returning in-memory data for write operations
   - ArrayList handling for List implementations"
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]
    [xitdb.common :as common]
    [xitdb.test-utils :as tu :refer [with-db]]))

;; =============================================================================
;; deref-at tests
;; =============================================================================

(deftest deref-at-basic-test
  (testing "deref-at returns the version of data at a specific index"
    (with-open [db (xdb/xit-db :memory)]
      ;; Create a sequence of transactions
      (reset! db {:version 1})
      (swap! db assoc :version 2)
      (swap! db assoc :version 3)
      (swap! db assoc :version 4)

      ;; Current state should be version 4
      (is (= {:version 4} @db))

      ;; Access historical versions (0-indexed)
      (is (= {:version 1} (xdb/deref-at db 0)))
      (is (= {:version 2} (xdb/deref-at db 1)))
      (is (= {:version 3} (xdb/deref-at db 2)))
      (is (= {:version 4} (xdb/deref-at db 3)))

      ;; Using -1 should return the latest version
      (is (= {:version 4} (xdb/deref-at db -1))))))

(deftest deref-at-complex-data-test
  (testing "deref-at works with complex nested data"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:users []})
      (swap! db update :users conj {:name "Alice"})
      (swap! db update :users conj {:name "Bob"})
      (swap! db assoc :metadata {:count 2})

      ;; Check historical versions
      (is (= {:users []} (xdb/deref-at db 0)))
      (is (= {:users [{:name "Alice"}]} (tu/materialize (xdb/deref-at db 1))))
      (is (= {:users [{:name "Alice"} {:name "Bob"}]}
             (tu/materialize (xdb/deref-at db 2))))
      (is (= {:users [{:name "Alice"} {:name "Bob"}] :metadata {:count 2}}
             (tu/materialize (xdb/deref-at db 3)))))))

(deftest deref-at-with-vectors-test
  (testing "deref-at works with vector data"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db [1])
      (swap! db conj 2)
      (swap! db conj 3)

      (is (= [1] (xdb/deref-at db 0)))
      (is (= [1 2] (xdb/deref-at db 1)))
      (is (= [1 2 3] (xdb/deref-at db 2))))))

(deftest history-index-test
  (testing "history-index returns the current transaction count"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:a 1})
      (is (= 1 (xdb/history-index db)))

      (swap! db assoc :b 2)
      (is (= 2 (xdb/history-index db)))

      (swap! db assoc :c 3)
      (is (= 3 (xdb/history-index db))))))

;; =============================================================================
;; materialize-shallow tests
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
        ;; Should be a vector (shallow materialization converts to vector)
        (is (vector? shallow))

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
;; ISlot implementation tests for read-only types
;; =============================================================================

(deftest islot-read-only-map-test
  (testing "XITDBHashMap implements ISlot"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:foo :bar})
      (let [db-val @db]
        (is (satisfies? common/ISlot db-val))
        (is (some? (common/-slot db-val)))))))

(deftest islot-read-only-vector-test
  (testing "XITDBArrayList implements ISlot"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db [1 2 3])
      (let [db-val @db]
        (is (satisfies? common/ISlot db-val))
        (is (some? (common/-slot db-val)))))))

(deftest islot-read-only-set-test
  (testing "XITDBHashSet implements ISlot"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db #{:a :b :c})
      (let [db-val @db]
        (is (satisfies? common/ISlot db-val))
        (is (some? (common/-slot db-val)))))))

(deftest islot-read-only-list-test
  (testing "XITDBLinkedArrayList implements ISlot"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db '(1 2 3))
      (let [db-val @db]
        (is (satisfies? common/ISlot db-val))
        (is (some? (common/-slot db-val)))))))

;; =============================================================================
;; reset! with ISlot values tests
;; =============================================================================

(deftest reset-with-islot-value-test
  (testing "reset! can use ISlot values directly"
    (with-open [db1 (xdb/xit-db :memory)
                db2 (xdb/xit-db :memory)]
      ;; Set up first database with some data
      (reset! db1 {:users [{:name "Alice"} {:name "Bob"}]
                   :config {:theme "dark"}})

      ;; Get the value from db1 (which implements ISlot)
      (let [val-from-db1 @db1]
        ;; Reset db2 with the ISlot value
        (reset! db2 val-from-db1)

        ;; db2 should now have the same data
        (is (= (tu/materialize @db1) (tu/materialize @db2)))))))

(deftest reset-with-nested-islot-value-test
  (testing "reset! with nested ISlot values preserves structure"
    (with-open [db1 (xdb/xit-db :memory)
                db2 (xdb/xit-db :memory)]
      (reset! db1 {:data [[1 2 3] [4 5 6] [7 8 9]]})

      ;; Get a nested value that implements ISlot
      (let [nested-val (get @db1 :data)]
        (reset! db2 nested-val)
        (is (= [[1 2 3] [4 5 6] [7 8 9]] (tu/materialize @db2)))))))

;; =============================================================================
;; nil storage tests (stored as Tag/NONE)
;; =============================================================================

(deftest nil-in-map-test
  (testing "nil values in maps are stored and retrieved correctly"
    (with-db [db (tu/test-db)]
      (reset! db {:a nil :b "value" :c nil})
      (is (= {:a nil :b "value" :c nil} @db))
      (is (nil? (get @db :a)))
      (is (nil? (get @db :c)))
      (is (= "value" (get @db :b))))))

(deftest nil-in-vector-test
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

(deftest nil-as-map-key-test
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

;; =============================================================================
;; Read-only types returning in-memory data for write operations
;; =============================================================================

(deftest read-only-map-assoc-test
  (testing "assoc on XITDBHashMap returns a regular map"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:a 1 :b 2})
      (let [db-val @db
            result (assoc db-val :c 3)]
        ;; Result should be a regular Clojure map
        (is (map? result))
        (is (not (instance? xitdb.hash_map.XITDBHashMap result)))
        (is (= {:a 1 :b 2 :c 3} result))))))

(deftest read-only-map-dissoc-test
  (testing "dissoc on XITDBHashMap returns a regular map"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:a 1 :b 2 :c 3})
      (let [db-val @db
            result (dissoc db-val :b)]
        ;; Result should be a regular Clojure map
        (is (map? result))
        (is (not (instance? xitdb.hash_map.XITDBHashMap result)))
        (is (= {:a 1 :c 3} result))))))

(deftest read-only-map-conj-test
  (testing "conj on XITDBHashMap returns a regular map"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:a 1})
      (let [db-val @db
            result (conj db-val [:b 2])]
        (is (map? result))
        (is (= {:a 1 :b 2} result))))))

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

(deftest read-only-set-disj-test
  (testing "disj on XITDBHashSet returns a regular set"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db #{:a :b :c})
      (let [db-val @db
            result (disj db-val :b)]
        ;; Result should be a regular Clojure set
        (is (set? result))
        (is (not (instance? xitdb.hash_set.XITDBHashSet result)))
        (is (= #{:a :c} result))))))

(deftest read-only-set-conj-test
  (testing "conj on XITDBHashSet returns a result with element added"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db #{:a :b})
      (let [db-val @db
            result (cons :c db-val)]
        (is (= :c (first result)))))))

(deftest read-only-list-cons-test
  (testing "cons on XITDBLinkedArrayList prepends element"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db '(2 3 4))
      (let [db-val @db
            result (cons 1 db-val)]
        (is (= '(1 2 3 4) result))))))

;; =============================================================================
;; ArrayList handling for other List implementations
;; =============================================================================

(deftest java-arraylist-storage-test
  (testing "java.util.ArrayList is stored as XITDB ArrayList"
    (with-db [db (tu/test-db)]
      (let [java-list (java.util.ArrayList. [1 2 3 4 5])]
        (reset! db {:items java-list})
        ;; Should be retrievable as a vector-like structure
        (is (= [1 2 3 4 5] (vec (get @db :items))))))))

(deftest java-linkedlist-storage-test
  (testing "java.util.LinkedList is stored as XITDB ArrayList"
    (with-db [db (tu/test-db)]
      (let [java-list (java.util.LinkedList. [1 2 3])]
        (reset! db java-list)
        (is (= [1 2 3] @db))))))

(deftest nested-java-list-storage-test
  (testing "nested java Lists are stored correctly"
    (with-db [db (tu/test-db)]
      (let [outer (java.util.ArrayList.)
            inner1 (java.util.ArrayList. [1 2])
            inner2 (java.util.ArrayList. [3 4])]
        (.add outer inner1)
        (.add outer inner2)
        (reset! db outer)
        (is (= [[1 2] [3 4]] @db))))))

(deftest sublist-storage-test
  (testing "List.subList is stored as XITDB ArrayList"
    (with-db [db (tu/test-db)]
      (let [java-list (java.util.ArrayList. [1 2 3 4 5])
            sub (.subList java-list 1 4)]
        (reset! db sub)
        (is (= [2 3 4] @db))))))

;; =============================================================================
;; Integration tests combining multiple features
;; =============================================================================

(deftest deref-at-with-nil-values-test
  (testing "deref-at works correctly with nil values in history"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:value nil})
      (swap! db assoc :value 1)
      (swap! db assoc :value nil)
      (swap! db assoc :value 2)

      (is (nil? (get (xdb/deref-at db 0) :value)))
      (is (= 1 (get (xdb/deref-at db 1) :value)))
      (is (nil? (get (xdb/deref-at db 2) :value)))
      (is (= 2 (get (xdb/deref-at db 3) :value))))))

(deftest reset-preserves-slot-efficiency-test
  (testing "reset! with ISlot values uses slot directly"
    (with-open [db1 (xdb/xit-db :memory)
                db2 (xdb/xit-db :memory)]
      ;; Create large nested data
      (reset! db1 {:data (vec (range 100))
                   :nested {:more (vec (range 50))}})

      ;; Reset db2 with the value (should use slot efficiently)
      (reset! db2 @db1)

      ;; Verify data integrity
      (is (= (tu/materialize @db1) (tu/materialize @db2)))

      ;; Verify history works
      (swap! db2 assoc :added "new")
      (is (= (tu/materialize @db1) (tu/materialize (xdb/deref-at db2 0)))))))

(deftest materialize-shallow-for-write-operations-test
  (testing "write operations use materialize-shallow correctly"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:a 1 :b {:nested "value"} :c 3})

      (let [result (assoc @db :d 4)]
        ;; The result should be a regular map
        (is (map? result))
        ;; And should contain all keys
        (is (= #{:a :b :c :d} (set (keys result))))))))

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
