(ns xitdb.file-lock-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [xitdb.db :as db])
  (:import [java.util.concurrent TimeUnit]))

(defn- with-temp-db [f]
  (let [file (java.io.File/createTempFile "xitdb-lock-" ".db")]
    (try (f (.getAbsolutePath file)) (finally (.delete file)))))

(deftest overlapping-writes-are-rejected-and-locks-are-released
  (with-temp-db
    (fn [path]
      (with-open [a (db/xit-db path)
                  b (db/xit-db path)]
        (reset! a {:initial 1})
        (swap! a (fn [m]
                   (is (thrown? IllegalStateException
                                (swap! b (fn [_] (is false "callback must not run")))))
                   (is (thrown? IllegalStateException (reset! b {:lost true})))
                   (is (thrown? IllegalStateException
                                (reset! (db/xdb-cursor b [:initial]) 99)))
                   (assoc m :a 3)))
        (swap! b assoc :b 2)
        (is (thrown? clojure.lang.ExceptionInfo
                     (swap! a (fn [m]
                                (assoc m :aborted [1 2 3])
                                (throw (ex-info "abort" {}))))))
        (reset! b (db/materialize @b)))
      (with-open [reopened (db/xit-db path)]
        (is (= {:initial 1 :a 3 :b 2} (db/materialize @reopened)))
        (is (= 4 (count reopened)))))))

(deftest foreign-process-lock-prevents-swap-and-reset
  (with-temp-db
    (fn [path]
      (with-open [d (db/xit-db path)]
        (reset! d {:safe 1})
        (let [code (str "(with-open [f (java.io.RandomAccessFile. " (pr-str path)
                        " \"rw\") held (.tryLock (.getChannel f))]"
                        " (println (boolean held)) (flush) (read-line))")
              process (.start (ProcessBuilder.
                                ^java.util.List
                                [(str (System/getProperty "java.home") "/bin/java")
                                 "-cp" (System/getProperty "java.class.path")
                                 "clojure.main" "-e" code]))]
          (try
            (with-open [reader (io/reader (.getInputStream process))
                        writer (io/writer (.getOutputStream process))]
              (is (= "true" (deref (future (.readLine reader)) 10000 ::timeout)))
              (is (thrown? IllegalStateException (swap! d assoc :lost true)))
              (is (thrown? IllegalStateException (reset! d {})))
              (is (= {:safe 1} (db/materialize @d)))
              (is (= 1 (count d)))
              (.write writer "\n")
              (.flush writer))
            (finally
              (when-not (.waitFor process 5 TimeUnit/SECONDS)
                (.destroyForcibly process)
                (.waitFor process 5 TimeUnit/SECONDS)))))
        (swap! d assoc :after true)
        (is (= {:safe 1 :after true} (db/materialize @d)))))))
