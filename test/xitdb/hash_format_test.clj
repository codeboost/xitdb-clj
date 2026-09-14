(ns xitdb.hash-format-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [xitdb.db :as db])
  (:import [java.nio.file Files]
           [java.util Arrays Date]))

(deftest legacy-hashes-are-rejected-without-changing-the-file
  ;; Written by master at fdebd313 with xitdb-java 0.38.0. Contains two
  ;; history entries, vector/Date map keys, and vector/Date set members.
  (let [file (java.io.File/createTempFile "xitdb-legacy-hashes-" ".db")]
    (try
      (io/copy (io/file "test-resources/legacy-key-hashes.db") file)
      (let [before (Files/readAllBytes (.toPath file))]
        (is (thrown-with-msg? IllegalArgumentException #"key hash format.*[Ee]xport"
                             (with-open [d (db/xit-db (.getPath file))]
                               (swap! d assoc [1 2] :updated))))
        (is (Arrays/equals before (Files/readAllBytes (.toPath file)))))
      (finally (.delete file)))))

(deftest current-hash-format-survives-reopen-and-compaction
  (let [file (java.io.File/createTempFile "xitdb-current-hashes-" ".db")
        target (java.io.File/createTempFile "xitdb-compacted-hashes-" ".db")
        value {[1 2] :vector (Date. 1000) :date :members #{[3 4] (Date. 2000)}}]
    (.delete target)
    (try
      (with-open [d (db/xit-db (.getPath file))]
        (reset! d value))
      (with-open [d (db/xit-db (.getPath file))]
        (is (= :vector (get @d [1 2])))
        (is (= :date (get @d (Date. 1000))))
        (with-open [compacted (db/compact d (.getPath target))]
          (is (= value (db/materialize @compacted)))))
      (with-open [d (db/xit-db (.getPath target))]
        (swap! d assoc [1 2] :updated)
        (is (= 3 (count @d)))
        (is (= :updated (get @d [1 2]))))
      (finally
        (.delete file)
        (.delete target)))))
