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
