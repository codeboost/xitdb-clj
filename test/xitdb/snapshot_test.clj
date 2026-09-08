(ns xitdb.snapshot-test
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]
    [xitdb.snapshot :as snapshot]))

(defn- temp-db-file []
  (let [f (java.io.File/createTempFile "xitdb-snapshot" ".db")]
    (.delete f)
    (.deleteOnExit f)
    (.getAbsolutePath f)))

(deftest snapshot-memory-db-copies-a-file-database-into-memory
  (let [file (temp-db-file)]
    (with-open [db (xdb/xit-db file)]
      (reset! db {:users {"alice" {:age 30}} :tags #{:a :b} :order (sorted-map 2 :b 1 :a)}))
    (testing "the whole database"
      (with-open [mem (snapshot/snapshot-memory-db file [])]
        (is (= {:users {"alice" {:age 30}} :tags #{:a :b} :order {1 :a 2 :b}}
               (xdb/materialize @mem)))
        (is (sorted? (get @mem :order)))))
    (testing "a nested keypath"
      (with-open [mem (snapshot/snapshot-memory-db file [:users "alice"])]
        (is (= {:age 30} (xdb/materialize @mem)))))))

(deftest snapshot-memory-db-copies-collection-keys
  (let [file (temp-db-file)]
    (with-open [db (xdb/xit-db file)]
      (reset! db {[1 2] {"nested" [3]} {:k 1} :v}))
    (with-open [mem (snapshot/snapshot-memory-db file [])]
      (is (= {[1 2] {"nested" [3]} {:k 1} :v} (xdb/materialize @mem))))))
