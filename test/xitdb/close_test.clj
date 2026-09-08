(ns xitdb.close-test
  "Closing a database releases every thread's reader handle, not only the
  closing thread's."
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]))

(defn- temp-db-file []
  (let [f (java.io.File/createTempFile "xitdb-close" ".db")]
    (.delete f)
    (.deleteOnExit f)
    (.getAbsolutePath f)))

(defn- on-new-thread
  "Runs `f` on a fresh (non-pooled) thread and returns its result or the
  Throwable it threw."
  [f]
  (let [result (promise)
        thread (Thread. (fn [] (deliver result (try (f) (catch Throwable t t)))))]
    (.start thread)
    (.join thread)
    @result))

(deftest close-releases-reader-handles-of-other-threads
  (let [file (temp-db-file)
        db   (xdb/xit-db file)]
    (reset! db {:a 1})
    (let [held   (promise)
          go     (promise)
          after  (promise)
          reader (Thread. (fn []
                            (deliver held @db)
                            @go
                            (deliver after (try (get @held :a) (catch Throwable t t)))))]
      (.start reader)
      (is (= 1 (get @held :a)) "the worker thread read through its own handle")
      (.close db)
      (deliver go true)
      (.join reader)
      (is (instance? java.io.IOException @after)
          "the worker's handle was closed by the main thread's close")
      (is (instance? IllegalStateException (on-new-thread #(deref db)))
          "a thread that first touches the database after close gets a clear error, not a new handle"))))
