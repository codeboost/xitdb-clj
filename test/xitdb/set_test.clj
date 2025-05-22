(ns xitdb.set-test
  (:require
    [clojure.set :as set]
    [clojure.test :refer :all]
    [xitdb.common :as common]
    [xitdb.db :as xdb]
    [xitdb.test-utils :as tu :refer [with-db]]))

(deftest SetTest
  (testing "Set works"
    (with-db [db (tu/test-db)]
      (reset! db #{1 2 3 4 5})

      (swap! db conj 6)
      (swap! db disj 2 3)

      (is (= #{1 4 6 5} @db))
      (is (= 4 (count @db)))
      (is (tu/db-equal-to-atom? db))))

  (testing "Basic operations"
    (with-db [db (tu/test-db)]
      (testing "Creation"
        (reset! db #{1 2 3 4 5})
        (is (= #{1 2 3 4 5} @db)))

      (testing "Membership"
        (is (= true (contains? @db 2)))
        (is (= 3 (get @db 3))))

      (testing "Adding/Removing"
        (swap! db conj 7)
        (is (= #{1 2 3 4 5 7} @db))
        (swap! db disj 3)
        (is (= #{1 2 4 5 7} @db))

        (swap! db conj 7 5 1 4 4 4)
        (is (= #{1 2 4 5 7} @db)))

      (testing "Emptying"
        (swap! db empty)
        (is (= #{} @db))
        (is (= 0 (count @db)))
        (is (tu/db-equal-to-atom? db))))))

(deftest DataTypes
  (testing "Supports nested types"
    (with-db [db (tu/test-db)]
      (reset! db #{1 {:foo :bar} [1 2 3 4] '(7 89)})
      (is (= #{1 '(7 89) [1 2 3 4] {:foo :bar}}
             @db))

      (testing "Adding a set to the set"
        (swap! db conj #{1 [3 4] {:new :map}})
        (is (= #{1 '(7 89) [1 2 3 4] #{1 [3 4] {:new :map}} {:foo :bar}}
               @db)))
      (is (tu/db-equal-to-atom? db)))))


(deftest SetOperations
  (testing "Union"
    (with-db [db (tu/test-db)]
      (reset! db #{1 2 3 4 5})
      (swap! db set/union #{4 5 8})
      (is (= #{1 4 3 2 5 8} @db))
      (is (tu/db-equal-to-atom? db))))

  (testing "Intersection"
    (with-db [db (tu/test-db)]
      (reset! db #{1 2 3 4 5})
      (swap! db set/intersection #{4 5 8})
      (is (= #{4 5} @db))
      (is (tu/db-equal-to-atom? db))))

  (testing "Union of two dbs"
    (let [db1 (tu/test-memory-db-raw)
          db2 (tu/test-memory-db-raw)]
      (reset! db1 {:sweets #{:tiramisu :mochi}})
      (reset! db2 {:candies #{:chupa-chups :regular-candy}})
      (swap! db1 update :sweets set/union (:candies @db2))
      (is (= {:sweets #{:regular-candy :chupa-chups, :mochi :tiramisu}}
             (tu/materialize @db1))))))

(deftest nil-test
  (with-db [db (tu/test-db)]
    (reset! db {:sweets #{nil :mochi}})
    (testing "Handles nil correctly"
      (let [sweets (:sweets @db)]
        (is (true? (contains? sweets nil)))))))



