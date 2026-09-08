(ns xitdb.cross-database-test
  "A value read from one database is a pointer into that database's storage.
  Writing it into a different database must be refused up front instead of
  committing a dangling pointer."
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]))

(deftest reset-with-value-from-another-database-is-refused
  (with-open [source (xdb/xit-db :memory)
              target (xdb/xit-db :memory)]
    (reset! source {:payload [1 2 3]})
    (reset! target {:ok true})
    (let [ex (is (thrown? IllegalArgumentException (reset! target (get @source :payload))))]
      (is (re-find #"materialize" (.getMessage ex)) "the error tells the caller how to copy the value"))
    (is (= 1 (count target)) "nothing was committed")
    (is (= {:ok true} (xdb/materialize @target)) "the target is still readable")))

(defn- temp-db-file []
  (let [f (java.io.File/createTempFile "xitdb-cross" ".db")]
    (.delete f)
    (.deleteOnExit f)
    (.getAbsolutePath f)))

(deftest values-from-the-same-file-database-are-accepted
  ;; A file database reads through a per-thread read-only handle and writes
  ;; through a separate writer handle. Values must still be recognised as the
  ;; database's own, whichever thread read them.
  (with-open [db (xdb/xit-db (temp-db-file))]
    (reset! db {:v [1 2]})
    (swap! db assoc :v [3])
    (testing "reverting to an earlier version writes a value read from history"
      (reset! db (xdb/deref-at db 0))
      (is (= {:v [1 2]} (xdb/materialize @db))))
    (testing "a value read on another thread can be written on this one"
      (let [read-elsewhere @(future (get @db :v))]
        (swap! db assoc :copy read-elsewhere)
        (is (= {:v [1 2] :copy [1 2]} (xdb/materialize @db)))))))

(deftest foreign-value-nested-in-a-plain-collection-is-refused
  (with-open [source (xdb/xit-db :memory)
              target (xdb/xit-db :memory)]
    (reset! source {:payload [1 2 3]})
    (reset! target {:ok true})
    (is (thrown? IllegalArgumentException
                 (swap! target assoc :imported {:wrapped (get @source :payload)})))
    (is (= 1 (count target)) "nothing was committed")
    (is (= {:ok true} (xdb/materialize @target)))))

(deftest materialized-foreign-value-is-accepted
  (with-open [source (xdb/xit-db :memory)
              target (xdb/xit-db :memory)]
    (reset! source {:payload [1 2 3]})
    (reset! target (xdb/materialize @source))
    (is (= {:payload [1 2 3]} (xdb/materialize @target)))))
