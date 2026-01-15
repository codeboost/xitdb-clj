(ns xitdb.history-test
  "Tests for database history and versioning features:
   - deref-at: access historical versions
   - count: get current transaction count"
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]
    [xitdb.test-utils :as tu]))

(deftest deref-at-basic-test
  (testing "deref-at returns the version of data at a specific index"
    (with-open [db (xdb/xit-db :memory)]
      ;; Create a sequence of transactions
      (reset! db {:version 1})
      (swap! db assoc :version 2)
      (swap! db assoc :version 3)
      (swap! db assoc :version 4)

      ;; Current state should be version 4
      (is (= {:version 4} (tu/materialize @db)))

      ;; Access historical versions (0-indexed)
      (is (= {:version 1} (tu/materialize (xdb/deref-at db 0))))
      (is (= {:version 2} (tu/materialize (xdb/deref-at db 1))))
      (is (= {:version 3} (tu/materialize (xdb/deref-at db 2))))
      (is (= {:version 4} (tu/materialize (xdb/deref-at db 3))))

      ;; Using -1 should return the latest version
      (is (= {:version 4} (tu/materialize (xdb/deref-at db -1)))))))

(deftest deref-at-complex-data-test
  (testing "deref-at works with complex nested data"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:users []})
      (swap! db update :users conj {:name "Alice"})
      (swap! db update :users conj {:name "Bob"})
      (swap! db assoc :metadata {:count 2})

      ;; Check historical versions
      (is (= {:users []} (tu/materialize (xdb/deref-at db 0))))
      (is (= {:users [{:name "Alice"}]} (tu/materialize (xdb/deref-at db 1))))
      (is (= {:users [{:name "Alice"} {:name "Bob"}]}
             (tu/materialize (xdb/deref-at db 2))))
      (is (= {:users [{:name "Alice"} {:name "Bob"}] :metadata {:count 2}}
             (tu/materialize (xdb/deref-at db 3)))))))

(deftest deref-at-with-vectors-test
  (testing "deref-at works with vector data"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db [1])
      (swap! db conj 2)
      (swap! db conj 3)

      (is (= [1] (tu/materialize (xdb/deref-at db 0))))
      (is (= [1 2] (tu/materialize (xdb/deref-at db 1))))
      (is (= [1 2 3] (tu/materialize (xdb/deref-at db 2)))))))

(deftest count-test
  (testing "count returns the current transaction count"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:a 1})
      (is (= 1 (count db)))

      (swap! db assoc :b 2)
      (is (= 2 (count db)))

      (swap! db assoc :c 3)
      (is (= 3 (count db))))))

(deftest deref-at-with-nil-values-test
  (testing "deref-at works correctly with nil values in history"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:value nil})
      (swap! db assoc :value 1)
      (swap! db assoc :value nil)
      (swap! db assoc :value 2)

      (is (nil? (get (xdb/deref-at db 0) :value)))
      (is (= 1 (get (xdb/deref-at db 1) :value)))
      (is (nil? (get (xdb/deref-at db 2) :value)))
      (is (= 2 (get (xdb/deref-at db 3) :value))))))
