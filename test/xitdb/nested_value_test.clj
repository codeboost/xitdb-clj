(ns xitdb.nested-value-test
  "Values read from the database and then written back nested inside plain
  Clojure collections must keep their on-disk type."
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]))

(deftest sorted-map-nested-in-plain-map-stays-sorted
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {:idx (sorted-map 3 :c 1 :a 2 :b)})
    (swap! db assoc :copy {:inner (get @db :idx)})
    (let [copy (get-in @db [:copy :inner])]
      (is (sorted? copy))
      (is (= [[2 :b] [3 :c]] (subseq copy >= 2)))
      (is (= {1 :a 2 :b 3 :c} (xdb/materialize copy))))))

(deftest sorted-map-nested-in-vector-stays-sorted
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {:idx (sorted-map 3 :c 1 :a 2 :b)})
    (swap! db assoc :copies [(get @db :idx)])
    (let [copy (get-in @db [:copies 0])]
      (is (sorted? copy))
      (is (= [[2 :b] [3 :c]] (subseq copy >= 2))))))

(deftest sorted-map-nested-in-list-stays-sorted
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {:idx (sorted-map 3 :c 1 :a 2 :b)})
    (swap! db assoc :copies (list (get @db :idx)))
    (let [copy (first (get @db :copies))]
      (is (sorted? copy))
      (is (= [[2 :b] [3 :c]] (subseq copy >= 2))))))

(deftest sorted-set-nested-in-plain-collections-stays-sorted
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {:tags (sorted-set "c" "a" "b")})
    (swap! db assoc :copies {:in-map (get @db :tags)
                             :in-vec [(get @db :tags)]
                             :in-list (list (get @db :tags))})
    (doseq [copy [(get-in @db [:copies :in-map])
                  (get-in @db [:copies :in-vec 0])
                  (first (get-in @db [:copies :in-list]))]]
      (is (sorted? copy))
      (is (= ["b" "c"] (subseq copy >= "b"))))))
