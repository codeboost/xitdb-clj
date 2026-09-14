(ns xitdb.collection-equality-test
  (:require [clojure.test :refer :all]
            [xitdb.db :as db]))

(defn- check-unsupported-map-keys [view]
  (doseq [k ['a (Object.) 1/2 ['a]]]
    (let [native {k 1}
          java-map (java.util.HashMap. native)]
      (is (false? (= native view)))
      (is (false? (= view native)))
      (is (false? (.equals native view)))
      (is (false? (.equals view native)))
      (is (false? (.equals java-map view)))
      (is (false? (.equals view java-map)))
      (is (false? (.containsKey ^java.util.Map view k)))
      (is (nil? (.get ^java.util.Map view k)))))
  (is (.containsKey ^java.util.Map view :a))
  (is (= 1 (.get ^java.util.Map view :a))))

(deftest unsupported-map-keys-compare-unequal-in-both-directions
  (doseq [native [{:a 1} (sorted-map :a 1)]]
    (with-open [d (db/xit-db :memory)]
      (reset! d native)
      (check-unsupported-map-keys @d)
      (swap! d (fn [view]
                 (check-unsupported-map-keys view)
                 (assoc view :b 2)))
      (is (= {:a 1 :b 2} (db/materialize @d))))))

(defn- check-java-list-equality [view]
  (let [java-list (java.util.ArrayList. [1 2])]
    (is (.equals view java-list))
    (is (.equals java-list view))
    (is (= (.hashCode view) (.hashCode java-list)))
    (is (.contains (java.util.HashSet. [view]) java-list))
    (is (.contains (java.util.HashSet. [java-list]) view))))

(deftest java-list-equality-is-symmetric-for-read-and-write-sequences
  (doseq [native [[1 2] '(1 2)]]
    (with-open [d (db/xit-db :memory)]
      (reset! d native)
      (check-java-list-equality @d)
      (swap! d (fn [view]
                 (check-java-list-equality view)
                 view)))))

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

(deftest java-map-views-follow-seq-order-and-check-membership
  (let [entries (map (fn [i] [i (* 10 i)]) (shuffle (range 20)))]
    (doseq [native [(into {} entries) (into (sorted-map) entries)]]
      (with-open [d (db/xit-db :memory)]
        (reset! d native)
        (let [view  @d
              ks    (.keySet view)
              vs    (.values view)
              es    (.entrySet view)]
          (is (= (keys view) (seq ks)))
          (is (= (vals view) (seq vs)))
          (is (= (seq view) (seq es)))
          (is (= 20 (.size ks) (.size vs) (.size es)))
          (is (.contains ks 7))
          (is (not (.contains ks 99)))
          (is (.contains vs 70))
          (is (not (.contains vs 7)))
          (is (.contains es (clojure.lang.MapEntry. 7 70)))
          (is (not (.contains es (clojure.lang.MapEntry. 7 71))))
          (is (not (.contains es 7)))
          (is (= (set (keys native)) (set ks)))
          (is (thrown? UnsupportedOperationException (.add ks 1)))))))
  (with-open [d (db/xit-db :memory)]
    (reset! d (into (sorted-map) (map (fn [i] [i i]) (shuffle (range 20)))))
    (is (= (range 20) (seq (.keySet @d))))
    (is (= (range 20) (map key (.entrySet @d))))))

(deftest java-map-views-stay-live-inside-transactions
  (doseq [native [{:a 1} (sorted-map :a 1)]]
    (with-open [d (db/xit-db :memory)]
      (reset! d native)
      (swap! d (fn [view]
                 (let [ks (.keySet view)
                       es (.entrySet view)]
                   (is (= #{:a} (set ks)))
                   (assoc view :b 2)
                   (is (= 2 (.size ks) (.size es)))
                   (is (= #{:a :b} (set ks)))
                   (is (.contains es (clojure.lang.MapEntry. :b 2)))
                   (dissoc view :a)
                   (is (= #{:b} (set ks)))
                   view)))
      (is (= {:b 2} (db/materialize @d))))))

(deftest write-wrappers-are-iterable-inside-transactions
  (doseq [native [{:x 1 :y 2} (sorted-map :x 1 :y 2) '(1 2 3)]]
    (with-open [d (db/xit-db :memory)]
      (reset! d native)
      (swap! d (fn [view]
                 (let [expected (vec (seq view))]
                   (is (= (set native) (set expected)))
                   (is (= expected (into [] view)))
                   (is (= expected (reduce conj [] view)))
                   (is (= expected (iterator-seq (.iterator view))))
                   (is (thrown? UnsupportedOperationException
                                (let [it (.iterator view)] (.next it) (.remove it))))
                   view))))))
