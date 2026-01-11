(ns xitdb.map-test
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]
    [xitdb.test-utils :as tu :refer [with-db]]))

(deftest map-with-complex-keys
  (with-db [db (tu/test-db)]
    (testing "Composite values as keys"
      (reset! db {:foo {{:bar :baz} 42}})
      (is (= {:foo {{:bar :baz} 42}}
             (tu/materialize @db)))

      (reset! db {:foo {[1 :bar] 31
                        [2 :baz] 42}})
      (is (= {:foo {[1 :bar] 31
                    [2 :baz] 42}}
             (tu/materialize @db)))

      (swap! db update :foo dissoc [2 :baz])
      
      (is (= {:foo {[1 :bar] 31}}
             (tu/materialize @db))))))

(deftest KeysTest
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {})
    (is (= nil (keys @db)))
    (is (= 0 (count (keys @db))))))

(deftest KeysTestSet
  (with-open [db (xdb/xit-db :memory)]
    (reset! db #{})
    (is (= 0 (count (keys @db))))
    (is (= nil (keys @db)))))

;; =============================================================================
;; Read-only map operations returning Clojure types
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

(deftest materialize-shallow-for-write-operations-test
  (testing "write operations use materialize-shallow correctly"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:a 1 :b {:nested "value"} :c 3})

      (let [result (assoc @db :d 4)]
        ;; The result should be a regular map
        (is (map? result))
        ;; And should contain all keys
        (is (= #{:a :b :c :d} (set (keys result))))))))
