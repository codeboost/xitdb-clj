(ns xitdb.multi-threaded-test
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]))

(comment
  (with-open [db (xdb/xit-db "testing.xdb")]
    (reset! db {:foo {:bar {:baz {:vroo {:com 4}}}}})
    (tu/materialize @db))
  (let [db (xitdb.db/xit-db "testing.xdb")]
    (reset! db {:foo {:bar {:baz {:vroo {:com 4}}}}})
    (future
      (try
        (println "thread:" (tu/materialize @db))
        (swap! db assoc-in [:foo :bar :baz] 42)
        (println "thread after:" (tu/materialize @db))
        (catch Exception e
          (println "exception" e))))
    (future
      (try
        (Thread/sleep 200)
        (println "thread 2 :" (tu/materialize @db))
        (catch Exception e
          (println "exception" e))))
    (future
      (try
        (println "thread 3:" (tu/materialize @db))
        (catch Exception e
          (println "exception" e))))
    (tu/materialize @db)))