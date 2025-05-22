(ns xitdb.multi-threaded-test
  (:require
    [clojure.test :refer :all]
    [xitdb.common :as common]
    [xitdb.db :as xdb]))

(deftest ReturnsHistoryTest
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {:foo {:bar {:baz {:vroo {:com 4}}}}})

    (testing "should return new value by default"
      (let [retval (swap! db assoc-in [:foo :bar :baz :vroo] 5)]
        (is (= {:foo {:bar {:baz {:vroo 5}}}}
               (common/materialize retval)))))

    (testing "should return history when binding is set"
      (binding [xdb/*return-history?* true]
        (let [retval (swap! db assoc-in [:foo :bar] 42)]
          (is (= [3 {:foo {:bar {:baz {:vroo 5}}}} {:foo {:bar 42}}]
                 (common/materialize retval))))))))

(defn temp-db-file []
  (let [temp-file (java.io.File/createTempFile "xitdb-" ".db")
        temp-path (.getAbsolutePath temp-file)]
    (.deleteOnExit temp-file)
    temp-path))

(deftest multi-threaded-operations-test
  (testing "Concurrent reads and visible writes in multithreaded environment"
    (with-open [db (xdb/xit-db (temp-db-file))]
      ;; Initialize database
      (reset! db {:data {:value 0}})

      ;; Create promises to track completion
      (let [read-before-promise (promise)
            write-complete-promise (promise)
            read-after-promise (promise)
            num-readers 5
            reader-before-values (atom [])
            reader-after-values (atom [])]

        ;; Start multiple reader threads (before write)
        (dotimes [i num-readers]
          (future
            (try
              (let [value (get-in @db [:data :value])]
                (swap! reader-before-values conj value))
              (when (= (count @reader-before-values) num-readers)
                (deliver read-before-promise true))
              (catch Exception e
                (println "Reader error:" e)))))

        ;; Wait for initial reads to complete
        (deref read-before-promise 1000 false)

        ;; Start writer thread
        (future
          (try
            (swap! db assoc-in [:data :value] 42)
            (deliver write-complete-promise true)
            (catch Exception e
              (println "Writer error:" e))))

        ;; Wait for write to complete
        (deref write-complete-promise 1000 false)

        ;; Start multiple reader threads (after write)
        (dotimes [i num-readers]
          (future
            (try
              (let [value (get-in @db [:data :value])]
                (swap! reader-after-values conj value))
              (when (= (count @reader-after-values) num-readers)
                (deliver read-after-promise true))
              (catch Exception e
                (println "Reader error:" e)))))

        ;; Wait for final reads to complete
        (deref read-after-promise 1000 false)

        ;; Test assertions
        (is (= num-readers (count @reader-before-values)) "All initial readers should complete")
        (is (every? #(= 0 %) @reader-before-values) "All initial readers should see original value")

        (is (= num-readers (count @reader-after-values)) "All post-write readers should complete")
        (is (every? #(= 42 %) @reader-after-values) "All post-write readers should see updated value")

        (is (= 42 (get-in (common/materialize @db) [:data :value])) "Final value should be updated")))))

(deftest concurrent-readers-with-active-writer-test
  (testing "Continuous readers with periodic writes"
    (with-open [db (xdb/xit-db (temp-db-file))]
      ;; Initialize database
      (reset! db {:data {:value 0}})

      (let [num-readers 5
            num-writes 10
            final-value 999
            running (atom true)
            reader-seen-values (atom (mapv (fn [_] #{}) (range num-readers)))
            writer-done (promise)]

        ;; Start reader threads that continuously read
        (dotimes [reader-id num-readers]
          (future
            (try
              (while @running
                (let [value (get-in @db [:data :value])]
                  ;; Record each unique value this reader sees
                  (swap! reader-seen-values update reader-id conj value)
                  (Thread/sleep (+ 10 (rand-int 20)))))
              (catch Exception e
                (println "Reader" reader-id "error:" e)))))

        ;; Start writer thread that makes multiple updates
        (future
          (try
            (dotimes [i num-writes]
              (let [new-value (if (= i (dec num-writes))
                                final-value  ; Final write has special value
                                (+ 100 i))]  ; Other writes have different values
                (swap! db assoc-in [:data :value] new-value)
                (Thread/sleep 50)))
            (deliver writer-done true)
            (catch Exception e
              (println "Writer error:" e))))

        ;; Wait for writer to complete
        (deref writer-done 5000 false)

        ;; Let readers continue a bit longer to see the final value
        (Thread/sleep 200)

        ;; Signal readers to stop
        (reset! running false)

        ;; Wait for readers to fully stop
        (Thread/sleep 100)

        ;; Test assertions
        (let [all-expected-values (conj (set (map #(+ 100 %) (range (dec num-writes)))) 0 final-value)]
          ;; Check that all readers saw the final value
          (is (every? #(contains? % final-value) @reader-seen-values)
              "All readers should eventually see the final value")

          ;; Check that readers collectively saw all intermediate values
          (is (= all-expected-values
                 (reduce clojure.set/union #{} @reader-seen-values))
              "Readers collectively should see all written values"))

        (is (= final-value (get-in @db [:data :value]))
            "Database should contain the final value")))))

(comment
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