(ns xitdb.linked-list-test
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]))

(deftest LinkedListTest
  (testing "Linked list works"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db '(1 2 3 4 5))
      (swap! db conj 6)
      (swap! db assoc (count @db) 7)

      (is (= [6 1 2 3 4 5 7] @db))
      (is (= 7 (count @db)))))

  (testing "Basic operations"
    (with-open [db (xdb/xit-db :memory)]
      (testing "Creation"
        (reset! db '(1 2 3 4 5))
        (is (= '(1 2 3 4 5) @db)))

      (testing "Membership"
        (is (= 4 (get @db 3))))

      (testing "Adding/Removing"
        (swap! db conj 7)
        (is (= '(7 1 2 3 4 5) @db))
        (swap! db dissoc 0)
        (is (= '(1 2 3 4 5) @db)))

      (testing "Adding to read-only list"
        (is (= [6 1 2 3 4 5]
               (conj @db 6))))

      (testing "Emptying"
        (swap! db empty)
        (is (= '() @db))
        (is (= 0 (count @db)))))))

(deftest DataTypes
  (testing "Supports nested types"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db '(1 {:foo :bar} [1 2 3 4] (7 89)))
      (is (= '(1 {:foo :bar} [1 2 3 4] (7 89))
             (xdb/materialize @db)))

      (testing "Adding a list to the list"
        (swap! db conj '(1 [3 4] {:new :map}))
        (is (= '((1 [3 4] {:new :map}) 1 {:foo :bar} [1 2 3 4] (7 89))
               (xdb/materialize @db)))))))

