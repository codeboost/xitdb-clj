(ns xitdb.cursor-test
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]))

(deftest CursorTest
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {:foo {:bar [1 2 3 {:hidden true} 5]}})
    (let [cursor1 (xdb/xdb-cursor db [:foo :bar])
          cursor2 (xdb/xdb-cursor db [:foo :bar 2])
          cursor3 (xdb/xdb-cursor db [:foo :bar 3 :hidden])]
      (testing "Cursors return the value at keypath"
        (is (= [1 2 3 {:hidden true} 5] (xdb/materialize @cursor1)))
        (is (= 3 @cursor2))
        (is (= true @cursor3)))

      (testing "reset! on the cursor changes the underlying database"
        (reset! cursor3 :changed)
        (is (= :changed @cursor3))
        (is (= :changed (get-in @db [:foo :bar 3 :hidden])))
        (is (= [1 2 3 {:hidden :changed} 5]) (xdb/materialize @cursor1)))

      (testing "swap! mutates the value at cursor"
        (swap! cursor1 assoc-in [3 :hidden] :changed-by-swap!)
        (is (= [1 2 3 {:hidden :changed-by-swap!} 5]) (xdb/materialize @cursor1))
        (is (= :changed-by-swap! @cursor3))
        (is (= 3 @cursor2)))

      (testing "Correctly handles invalid cursor path"
        (is (thrown? IndexOutOfBoundsException @(xdb/xdb-cursor db [:foo :bar 999])))))))

(deftest cursor-into-sorted-map
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {:idx (sorted-map 1 {:name "a"} 2 {:name "b"})})
    (let [c (xdb/xdb-cursor db [:idx 1 :name])]
      (testing "read through a sorted-map key"
        (is (= "a" @c)))
      (testing "reset! through a sorted-map key writes back to the db"
        (reset! c "A")
        (is (= "A" @c))
        (is (= "A" (get-in (xdb/materialize @db) [:idx 1 :name])))
        (is (= "b" (get-in (xdb/materialize @db) [:idx 2 :name])))))))

(deftest cursor-into-sorted-set
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {:tags (sorted-set "a" "b" "c")})
    (let [c (xdb/xdb-cursor db [:tags])]
      (testing "read a sorted set through a cursor"
        (is (= ["a" "b" "c"] (seq (xdb/materialize @c)))))
      (testing "swap! mutates the sorted set at the cursor"
        (swap! c conj "d")
        (is (= ["a" "b" "c" "d"] (seq (xdb/materialize @c))))))))
