(ns xitdb.replication-test
  (:require
    [clojure.java.io :as io]
    [clojure.test :refer :all]
    [xitdb.db :as xdb])
  (:import
    [io.github.radarroark.xitdb CoreMemory Database Database$EndOfStreamException Database$InvalidRangeException
     Hasher RandomAccessMemory]
    [java.io ByteArrayInputStream DataInputStream]
    [java.nio ByteBuffer]
    [java.security MessageDigest]
    [java.nio.file Files]
    [java.nio.file.attribute FileAttribute]
    [java.util UUID]))

(def ^:dynamic *temp-dir* nil)

(use-fixtures :each
  (fn [f]
    (let [dir (.toFile (Files/createTempDirectory "xitdb-replication" (make-array FileAttribute 0)))]
      (try
        (binding [*temp-dir* dir] (f))
        (finally
          (doseq [file (reverse (file-seq dir))]
            (io/delete-file file)))))))

(defn- location [kind]
  (if (= :memory kind)
    :memory
    (.getPath (io/file *temp-dir* (str (UUID/randomUUID) ".xdb")))))

(defn- sync! [source replica]
  (with-open [in  (io/input-stream source :since (xdb/replica-state replica))
              out (io/output-stream replica)]
    (io/copy in out)))

(defn- update-bytes
  "The bytes that `source` would send to `replica`."
  ^bytes [source replica]
  (with-open [in (io/input-stream source :since (xdb/replica-state replica))]
    (.readAllBytes in)))

(defn- frame-start [^bytes update]
  (.readLong (DataInputStream. (ByteArrayInputStream. update))))

(deftest replicates-between-storage-kinds
  (doseq [source-kind [:memory :file]
          replica-kind [:memory :file]]
    (testing (str source-kind " to " replica-kind)
      (with-open [source  (xdb/xit-db (location source-kind))
                  replica (xdb/xit-db (location replica-kind))]
        (reset! source {:users {"alice" {:age 30}} :tags #{:a :b}})
        (swap! source assoc-in [:users "bob"] {:age 40})

        ;; a new replica is replaced by a full copy
        (is (zero? (frame-start (update-bytes source replica))))
        (sync! source replica)
        (is (= (xdb/materialize @source) (xdb/materialize @replica)))
        (is (= (count source) (count replica)))
        (is (= (xdb/replica-state source) (xdb/replica-state replica)))

        ;; later syncs only send the new bytes
        (swap! source update :tags conj :c)
        (is (= (:committed-size (xdb/replica-state replica))
               (frame-start (update-bytes source replica))))
        (sync! source replica)
        (is (= #{:a :b :c} (xdb/materialize (:tags @replica))))
        (is (= (count source) (count replica)))
        (is (= (xdb/materialize (xdb/deref-at source 1)) (xdb/materialize (xdb/deref-at replica 1))))

        ;; an up to date replica gets nothing
        (let [update (update-bytes source replica)]
          (is (= 16 (alength update))))
        (sync! source replica)
        (is (= (xdb/replica-state source) (xdb/replica-state replica)))))))

(deftest without-since-the-whole-database-is-copied
  (with-open [source (xdb/xit-db :memory)
              copy   (xdb/xit-db (location :file))]
    (reset! source {:a 1})
    (swap! source assoc :b 2)
    (with-open [in  (io/input-stream source)
                out (io/output-stream copy)]
      (io/copy in out))
    (is (= {:a 1 :b 2} (xdb/materialize @copy)))
    (is (= 2 (count copy)))))

(deftest replica-is-replaced-when-it-cannot-be-extended
  (with-open [source  (xdb/xit-db :memory)
              replica (xdb/xit-db (location :file))]
    (reset! source {:from :source})
    (sync! source replica)
    ;; a compacted source is a new file, so the replica gets a full copy
    (swap! source assoc :more true)
    (with-open [compacted (xdb/compact source :memory)]
      (is (zero? (frame-start (update-bytes compacted replica))))
      (sync! compacted replica)
      (is (= {:from :source :more true} (xdb/materialize @replica)))
      (is (= 1 (count replica)))
      ;; and it can be extended again afterwards
      (swap! compacted assoc :after :compaction)
      (is (pos? (frame-start (update-bytes compacted replica))))
      (sync! compacted replica)
      (is (= :compaction (:after @replica)))
      (is (= 2 (count replica))))))

(deftest interrupted-sync-leaves-the-replica-at-its-last-commit
  (with-open [source  (xdb/xit-db :memory)
              replica (xdb/xit-db (location :file))]
    (reset! source {:n 1})
    (sync! source replica)
    (swap! source assoc :n 2)
    (let [update (update-bytes source replica)
          state  (xdb/replica-state replica)]
      ;; the stream ends early, so closing it throws instead of committing
      (is (thrown? Database$EndOfStreamException
                   (with-open [out (io/output-stream replica)]
                     (.write out update 0 (dec (alength update))))))
      (is (= state (xdb/replica-state replica)))
      (is (= {:n 1} (xdb/materialize @replica)))
      ;; it can still be written to and synced
      (sync! source replica)
      (is (= {:n 2} (xdb/materialize @replica)))
      ;; extra bytes are rejected
      (swap! source assoc :n 3)
      (let [update (update-bytes source replica)
            state  (xdb/replica-state replica)]
        (is (thrown? Database$InvalidRangeException
                     (with-open [out (io/output-stream replica)]
                       (.write out update)
                       (.write out (byte-array 1)))))
        (is (= state (xdb/replica-state replica))))
      ;; the stream ends before the frame does
      (is (thrown? Database$EndOfStreamException
                   (with-open [out (io/output-stream replica)]
                     (.write out update 0 8))))
      (swap! replica assoc :n 4)
      (is (= {:n 4} (xdb/materialize @replica))))))

(deftest replica-stream-holds-the-write-lock
  (with-open [source  (xdb/xit-db :memory)
              replica (xdb/xit-db :memory)]
    (reset! source {:a 1})
    (is (thrown? IllegalStateException
                 (swap! replica (fn [v]
                                  (io/output-stream replica)
                                  v))))
    (let [out     (io/output-stream replica)
          writer  (future (reset! replica {:b 2}))]
      (Thread/sleep 100)
      (is (not (realized? writer)))
      (io/copy (update-bytes source replica) out)
      (.close out)
      @writer
      (is (= {:b 2} (xdb/materialize @replica)))
      (is (= 2 (count replica))))))

(defn- staging-files []
  (filter #(.endsWith (.getName ^java.io.File %) ".tmp") (.listFiles ^java.io.File *temp-dir*)))

(deftest interrupted-full-copy-leaves-the-replica-as-it-was
  (doseq [kind [:memory :file]
          ;; cut before the root header is complete, and one byte before the end
          cut  [24 dec]]
    (testing (str (name kind) " replica, cut at " (if (fn? cut) "the last byte" cut))
      (with-open [source  (xdb/xit-db :memory)
                  replica (xdb/xit-db (location kind))]
        (reset! source {:n 1})
        (sync! source replica)
        ;; a compacted source is a new file, so the next sync is a full copy
        (with-open [compacted (xdb/compact source :memory)]
          (swap! compacted assoc :n 2)
          (let [update (update-bytes compacted replica)
                len    (if (fn? cut) (cut (alength update)) cut)
                state  (xdb/replica-state replica)]
            (is (zero? (frame-start update)))
            (is (thrown? Database$EndOfStreamException
                         (with-open [out (io/output-stream replica)]
                           (.write out update 0 len))))
            (is (= state (xdb/replica-state replica)))
            (is (= {:n 1} (xdb/materialize @replica)))
            (is (empty? (staging-files)))
            (sync! compacted replica)
            (is (= {:n 2} (xdb/materialize @replica)))
            (is (= (xdb/replica-state compacted) (xdb/replica-state replica)))
            ;; and it can be extended again afterwards
            (swap! compacted assoc :n 3)
            (is (pos? (frame-start (update-bytes compacted replica))))
            (sync! compacted replica)
            (is (= {:n 3} (xdb/materialize @replica)))))))))

(deftest replica-can-be-read-during-a-full-copy
  (doseq [kind [:memory :file]]
    (testing (name kind)
      (with-open [source  (xdb/xit-db :memory)
                  replica (xdb/xit-db (location kind))]
        (reset! replica {:old true})
        (reset! source {:new true})
        (let [update (update-bytes source replica)
              half   (quot (alength update) 2)]
          (is (zero? (frame-start update)))
          (with-open [out (io/output-stream replica)]
            (.write out update 0 half)
            (is (= {:old true} @(future (xdb/materialize @replica))))
            (.write out update half (- (alength update) half)))
          (is (= {:new true} (xdb/materialize @replica)))
          (is (empty? (staging-files))))))))

(deftest values-read-before-a-full-copy-never-read-the-copy
  (testing "a memory replica's old values keep the old data"
    (with-open [source  (xdb/xit-db :memory)
                replica (xdb/xit-db :memory)]
      (reset! replica {:old [1 2 3]})
      (reset! source {:new [4 5 6]})
      (let [old @replica]
        (sync! source replica)
        (is (= {:new [4 5 6]} (xdb/materialize @replica)))
        (is (= {:old [1 2 3]} (xdb/materialize old))))))
  (testing "a file replica's old values throw"
    (with-open [source  (xdb/xit-db :memory)
                replica (xdb/xit-db (location :file))]
      (reset! replica {:old [1 2 3]})
      (reset! source {:new [4 5 6]})
      (let [old @replica]
        (sync! source replica)
        (is (= {:new [4 5 6]} (xdb/materialize @replica)))
        (is (thrown? IllegalStateException (xdb/materialize old)))
        ;; the replica can still be written to and extended
        (swap! source assoc :more true)
        (sync! source replica)
        (is (= {:new [4 5 6] :more true} (xdb/materialize @replica)))))))

(deftest failed-sync-leaves-nothing-to-write-later
  ;; two handles on one replica file: a sync through `a` fails, then one
  ;; through `b` commits other bytes at the same place. closing `a` must not
  ;; write its leftovers over them.
  (let [path (location :file)]
    (with-open [source (xdb/xit-db :memory)
                fork   (xdb/xit-db :memory)]
      (reset! source {:n 1})
      (let [a (xdb/xit-db path)]
        (try
          (sync! source a)
          ;; fork is a copy of source, so both can extend the replica
          (sync! source fork)
          (swap! source assoc :from :source)
          (swap! fork assoc :from :fork)
          (let [update (update-bytes source a)]
            (is (thrown? Database$EndOfStreamException
                         (with-open [out (io/output-stream a)]
                           (.write out update 0 (dec (alength update)))))))
          (with-open [b (xdb/xit-db path)]
            (sync! fork b))
          (finally
            (.close a))))
      (with-open [replica (xdb/xit-db path)]
        (is (= {:n 1 :from :fork} (xdb/materialize @replica)))
        (is (= (xdb/replica-state fork) (xdb/replica-state replica)))))))

(defn- full-copy-bytes
  "A replication stream holding the whole of the engine database `db`."
  ^bytes [^Database db]
  (let [end (.committedSize db)]
    (with-open [in (.openCommitted db 0 end)]
      (byte-array (concat (.array (doto (ByteBuffer/allocate 16) (.putLong 0) (.putLong end)))
                          (.readAllBytes in))))))

(deftest full-copy-with-another-key-hash-format-is-refused
  (doseq [kind [:memory :file]]
    (testing (name kind)
      (with-open [replica (xdb/xit-db (location kind))]
        (reset! replica {:a 1})
        (let [state   (xdb/replica-state replica)
              ;; a database made with xitdb-java's default hasher, not ours
              foreign (Database. (CoreMemory. (RandomAccessMemory.))
                                 (Hasher. (MessageDigest/getInstance "SHA-1")))]
          (is (thrown-with-msg? IllegalArgumentException #"key hash format"
                                (with-open [out (io/output-stream replica)]
                                  (.write out (full-copy-bytes foreign)))))
          (is (= state (xdb/replica-state replica)))
          (is (= {:a 1} (xdb/materialize @replica)))
          (is (empty? (staging-files))))))))

(deftest since-must-be-a-replica-state
  (with-open [source (xdb/xit-db :memory)]
    (reset! source {:a 1})
    (doseq [since [{}
                   {:committed-size nil :checksum nil}
                   {:committed-size "1" :checksum 2}
                   {:committed-size -1 :checksum 0}
                   [0 0]]]
      (is (thrown-with-msg? IllegalArgumentException #":since must be a map"
                            (io/input-stream source :since since))
          (pr-str since)))))

(deftest readers-and-writers-are-refused
  (with-open [db (xdb/xit-db :memory)]
    (is (thrown? IllegalArgumentException (io/reader db)))
    (is (thrown? IllegalArgumentException (io/writer db)))
    ;; neither took the write lock
    (reset! db {:a 1})
    (is (= {:a 1} (xdb/materialize @db)))))
