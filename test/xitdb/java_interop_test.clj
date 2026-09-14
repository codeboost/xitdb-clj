(ns xitdb.java-interop-test
  "Tests for Java interoperability:
   - java.util.ArrayList storage
   - java.util.LinkedList storage
   - Nested Java List handling
   - List.subList support"
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]
    [xitdb.test-utils :as tu :refer [with-db]]))

(defn- check-read-only-list-contract [^java.util.List view]
  (is (= 4 (.size view)))
  (is (not (.isEmpty view)))
  (is (= "a" (.get view 0)))
  (is (nil? (.get view 1)))
  (is (thrown? IndexOutOfBoundsException (.get view -1)))
  (is (thrown? IndexOutOfBoundsException (.get view 4)))
  (is (.containsAll view [nil "b"]))
  (is (= 0 (.indexOf view "a")))
  (is (= 3 (.lastIndexOf view "a")))
  (is (= -1 (.indexOf view "missing")))
  (is (= ["a" nil "b" "a"] (vec (.toArray view))))
  (let [result (.toArray view (make-array String 0))]
    (is (= (class (make-array String 0)) (class result)))
    (is (= ["a" nil "b" "a"] (vec result))))
  (let [buffer (into-array String ["old" "old" "old" "old" "old" "untouched"])]
    (is (identical? buffer (.toArray view buffer)))
    (is (= ["a" nil "b" "a" nil "untouched"] (vec buffer))))
  (let [it (.listIterator view 2)]
    (is (= 2 (.nextIndex it)))
    (is (nil? (.previous it)))
    (is (nil? (.next it)))
    (is (= "b" (.next it)))
    (is (thrown? UnsupportedOperationException (.remove it)))
    (is (thrown? UnsupportedOperationException (.set it "changed")))
    (is (thrown? UnsupportedOperationException (.add it "changed"))))
  (is (thrown? IndexOutOfBoundsException (.listIterator view 5)))
  (is (= [nil "b"] (vec (.subList view 1 3))))
  (is (thrown? IndexOutOfBoundsException (.subList view -1 3)))
  (is (thrown? UnsupportedOperationException (.clear (.subList view 1 3))))
  (is (thrown? UnsupportedOperationException (.add view "changed")))
  (is (thrown? UnsupportedOperationException (.add view 0 "changed")))
  (is (thrown? UnsupportedOperationException (.set view 0 "changed")))
  (is (thrown? UnsupportedOperationException (.remove view (int 0))))
  (is (thrown? UnsupportedOperationException (.remove view "a")))
  (is (thrown? UnsupportedOperationException (.addAll view ["changed"])))
  (is (thrown? UnsupportedOperationException (.addAll view 0 ["changed"])))
  (is (thrown? UnsupportedOperationException (.removeAll view ["a"])))
  (is (thrown? UnsupportedOperationException (.retainAll view ["a"])))
  (is (thrown? UnsupportedOperationException (.clear view))))

(deftest stored-and-transaction-sequences-implement-read-only-java-lists
  (doseq [native [["a" nil "b" "a"] '("a" nil "b" "a")]]
    (with-open [d (xdb/xit-db :memory)]
      (reset! d native)
      (check-read-only-list-contract @d)
      (swap! d (fn [view]
                 (check-read-only-list-contract view)
                 (let [sub (.subList ^java.util.List view 0 2)]
                   (assoc view 0 "updated")
                   (is (= ["updated" nil] (vec sub))))
                 view))
      (is (= "updated" (first @d))))))

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
