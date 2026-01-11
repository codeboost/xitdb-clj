(ns xitdb.java-interop-test
  "Tests for Java interoperability:
   - java.util.ArrayList storage
   - java.util.LinkedList storage
   - Nested Java List handling
   - List.subList support"
  (:require
    [clojure.test :refer :all]
    [xitdb.test-utils :as tu :refer [with-db]]))

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

(deftest java-list-in-map-test
  (testing "java.util.List as map values"
    (with-db [db (tu/test-db)]
      (let [list1 (java.util.ArrayList. ["a" "b" "c"])
            list2 (java.util.LinkedList. [1 2 3])]
        (reset! db {:strings list1 :numbers list2})
        (is (= ["a" "b" "c"] (vec (get @db :strings))))
        (is (= [1 2 3] (vec (get @db :numbers))))))))

(deftest empty-java-list-test
  (testing "Empty java.util.List is stored correctly"
    (with-db [db (tu/test-db)]
      (let [empty-list (java.util.ArrayList.)]
        (reset! db empty-list)
        (is (= [] @db))
        (is (empty? @db))))))
