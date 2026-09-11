(ns xitdb.collection-equality-test
  (:require [clojure.test :refer :all]
            [xitdb.db :as db]))

(deftest stored-sequences-work-as-query-keys
  (doseq [make-value [vector list]]
    (with-open [d (db/xit-db :memory)]
      (let [values (mapv make-value (range 20))]
        (reset! d (into values values)))
      (is (= 20 (count (distinct @d))))
      (is (= 20 (count (set @d))))
      (is (= 20 (count (group-by identity @d)))))
    (with-open [d (db/xit-db :memory)]
      (let [native (make-value 1)]
        (reset! d native)
        (is (= native @d))
        (is (= @d native))
        (is (= (hash native) (hash @d)))
        (is (= (.hashCode native) (.hashCode @d)))
        (is (= :found (get (hash-map @d :found) native)))
        (is (= :found (get (hash-map native :found) @d)))))))

(deftest stored-maps-obey-native-equality-and-hashing
  (doseq [native [{:x [1 2]} (sorted-map :x [1 2]) {} (sorted-map)]]
    (with-open [d (db/xit-db :memory)]
      (reset! d native)
      (let [view @d]
        (is (= native view))
        (is (= view native))
        (is (= (hash native) (hash view)))
        (is (= (.hashCode native) (.hashCode view)))
        (is (.equals native view))
        (is (.equals view native))
        (is (= :found (get (hash-map native :found) view)))
        (is (= :found (get (hash-map view :found) native)))))))

(deftest stored-sets-obey-native-equality-and-hashing
  (doseq [native [#{[1 2] [3 4]} (sorted-set 1 2) #{} (sorted-set)]]
    (with-open [d (db/xit-db :memory)]
      (reset! d native)
      (let [view @d]
        (is (= native view))
        (is (= view native))
        (is (= (hash native) (hash view)))
        (is (= (.hashCode native) (.hashCode view)))
        (is (.equals native view))
        (is (.equals view native))
        (is (= :found (get (hash-map native :found) view)))
        (is (= :found (get (hash-map view :found) native)))))))

(deftest transaction-views-track-value-equality-and-hash-after-mutation
  (doseq [[native mutate] [[[1] #(conj % 2)]
                           ['(1) #(conj % 2)]
                           [{:x 1} #(assoc % :y 2)]
                           [(sorted-map :x 1) #(assoc % :y 2)]
                           [#{1} #(conj % 2)]
                           [(sorted-set 1) #(conj % 2)]]]
    (with-open [d (db/xit-db :memory)]
      (reset! d native)
      (swap! d (fn [view]
                 (is (= native view))
                 (is (= view native))
                 (is (= (hash native) (hash view)))
                 (is (= (.hashCode native) (.hashCode view)))
                 (let [changed (mutate view)
                       expected (mutate native)]
                   (is (= expected changed))
                   (is (= changed expected))
                   (is (= (hash expected) (hash changed)))
                   (is (= (.hashCode expected) (.hashCode changed)))
                   changed))))))
