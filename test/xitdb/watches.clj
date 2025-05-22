(ns xitdb.watches
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]
    [xitdb.test-utils :as tu :refer [with-db]]))

#_(deftest WatchTest
    (testing "Watch fn is being called correctly"
      (let [calls (atom [])
            db (xdb/xit-db :memory (fn [idx value]
                                     (swap! calls conj [idx (tu/materialize value)])))]
        (reset! db {:foo :bar})
        (swap! db assoc :ma :to)
        (swap! db assoc :mo :bo)
        (is (= [[1 {:foo :bar, :ma :to}]
                [2 {:foo :bar, :mo :bo, :ma :to}]]
               @calls))

        (swap! db update :ma (constantly 3))
        (swap! db update :ma inc)
        (is (= [[1 {:foo :bar, :ma :to}]
                [2 {:foo :bar, :mo :bo, :ma :to}]
                [3 {:foo :bar, :mo :bo, :ma 3}]
                [4 {:foo :bar, :mo :bo, :ma 4}]]
               @calls)))))