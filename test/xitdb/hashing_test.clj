(ns xitdb.hashing-test
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]))

(deftest rejected-lookup-does-not-poison-subsequent-writes
  (with-open [source (xdb/xit-db :memory)
              other (xdb/xit-db :memory)]
    (reset! source {})
    (reset! other {})
    (doseq [target [source other]]
      (testing (if (identical? source target) "same database" "another database")
        (is (thrown? IllegalArgumentException
                     (get @source (map identity [1]))))
        (swap! target assoc :saved 42)
        (is (= 42 (get @target :saved)))
        (swap! target assoc :saved 43)
        (is (= 43 (get @target :saved)))
        (is (= 1 (count @target)) "updating the key must not create a duplicate entry")
        (is (= {:saved 43} (xdb/materialize @target)))))))
