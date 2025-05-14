(ns xitdb.transient-test
  (:require
    [clojure.test :refer :all]
    [xitdb.test-utils :as tu :refer [with-db]]))


(deftest ConjTest
  (with-db [db (tu/test-memory-db-raw)]
    (reset! db [1 2 3 4 5])
    (swap! db (fn [V]
                (let [tv (transient V)]
                  (conj! tv 42)
                  (persistent! tv))))

    (is (= [1 2 3 4 5 42] @db))))


(deftest TransientTest
  (with-db [db (tu/test-memory-db-raw)]
    (testing "conj!"
      (reset! db [1 2 3 4 5])
      (swap! db (fn [V]
                  (let [tv (transient V)]
                    (conj! tv 42)
                    (persistent! tv))))

      (is (= [1 2 3 4 5 42] @db)))

    (testing "pop!"
      (reset! db [1 2 3 4 5])
      (swap! db (fn [V]
                  (let [tv (transient V)]
                    (pop! tv)
                    (pop! tv)
                    (pop! tv)
                    (persistent! tv))))
      (is (= [1 2] @db)))))

(let [tv (transient [1 2 3 4 5])]
  (pop! tv)
  (pop! tv)
  (pop! tv)
  (persistent! tv))