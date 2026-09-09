(ns xitdb.close-test
  "Closing a database releases every thread's reader handle, not only the
  closing thread's."
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]
    [xitdb.util.db-context :as db-context])
  (:import
    [io.github.radarroark.xitdb CoreBufferedFile]
    [java.io IOException]))

(defn- temp-db-file []
  (let [f (java.io.File/createTempFile "xitdb-close" ".db")]
    (.delete f)
    (.deleteOnExit f)
    (.getAbsolutePath f)))

(deftest reader-open-failure-closes-writer
  (let [file   (temp-db-file)
        writer (xdb/open-database file "rw")
        core   (.-core writer)]
    (try
      ;; The writer is open, but the reader's path cannot be opened because
      ;; its parent is a regular file. This fails before context creation.
      (with-redefs [xdb/open-database (fn [& _] writer)]
        (is (thrown? IOException (xdb/xit-db (str file "/missing.db")))))
      (is (thrown? IOException (.sync core)) "the writer descriptor was closed")
      (finally
        (.close core)
        (.delete (java.io.File. file))))))

(deftest context-creation-failure-closes-reader-and-writer
  (let [file    (temp-db-file)
        writer  (xdb/open-database file "rw")
        core    (.-core writer)
        reader  (atom nil)
        failure (IOException. "context initialization failed")]
    (try
      (with-redefs [xdb/open-database (fn [& _] writer)
                    db-context/create (fn [reader-core & _]
                                        (reset! reader reader-core)
                                        (throw failure))]
        (is (identical? failure
                        (try (xdb/xit-db file) (catch IOException t t)))))
      (is (thrown? IllegalStateException (.length @reader)) "the reader was closed")
      (is (thrown? IOException (.sync core)) "the writer descriptor was closed")
      (finally
        (when @reader (.close @reader))
        (.close core)
        (.delete (java.io.File. file))))))

(deftest initialization-failure-survives-writer-close-failure
  (let [file          (temp-db-file)
        writer        (xdb/open-database file "rw")
        core          (.-core writer)
        failure       (IOException. "context initialization failed")
        close-failure (IOException. "writer close failed")]
    (set! (.-core writer)
          (proxy [CoreBufferedFile] [(.-file ^CoreBufferedFile core)]
            (close []
              (proxy-super close)
              (throw close-failure))))
    (try
      (with-redefs [xdb/open-database (fn [& _] writer)
                    db-context/create (fn [& _] (throw failure))]
        (is (identical? failure
                        (try (xdb/xit-db file) (catch IOException t t)))))
      (is (= [close-failure] (vec (.getSuppressed failure))))
      (is (thrown? IOException (.sync core)) "cleanup still closed the writer")
      (finally
        (.close core)
        (.delete (java.io.File. file))))))

(defn- on-new-thread
  "Runs `f` on a fresh (non-pooled) thread and returns its result or the
  Throwable it threw."
  [f]
  (let [result (promise)
        thread (Thread. (fn [] (deliver result (try (f) (catch Throwable t t)))))]
    (.start thread)
    (.join thread 5000)
    (when (.isAlive thread)
      (.interrupt thread)
      (throw (ex-info "Reader thread did not finish" {})))
    @result))

(defn- collected-within?
  "Requests collection until `pred` holds, allowing time for file cleaners."
  [pred]
  (let [deadline (+ (System/nanoTime) 5000000000)]
    (loop []
      (System/gc)
      (Thread/sleep 50)
      (cond
        (pred) true
        (< (System/nanoTime) deadline) (recur)
        :else false))))

(deftest terminated-reader-threads-do-not-retain-file-handles
  (let [os (java.lang.management.ManagementFactory/getOperatingSystemMXBean)]
    (when (instance? com.sun.management.UnixOperatingSystemMXBean os)
      (let [file (temp-db-file)
            fds #(.getOpenFileDescriptorCount ^com.sun.management.UnixOperatingSystemMXBean os)]
        (try
          (with-open [db (xdb/xit-db file)]
            (reset! db {:a 1})
            ;; Clear handles left for collection by earlier tests before measuring.
            (collected-within? (constantly true))
            (let [before (fds)]
              (dotimes [_ 32]
                (is (= 1 (on-new-thread #(get @db :a)))))
              (is (collected-within? #(<= (fds) before))
                  "terminated threads' descriptors are reclaimed while the database stays open")
              (is (= 1 (get @db :a)) "the live reader is still usable")))
          (finally
            (.delete (java.io.File. file))))))))

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
      (is (instance? IllegalStateException @after)
          "the worker's handle was closed by the main thread's close")
      (is (instance? IllegalStateException (on-new-thread #(deref db)))
          "a thread that first touches the database after close gets a clear error, not a new handle"))))
