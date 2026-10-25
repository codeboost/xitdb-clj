(ns xitdb.replication-test
  (:require
    [clojure.java.io :as io]
    [clojure.test :refer :all]
    [xitdb.db :as xdb])
  (:import
    [io.github.radarroark.xitdb Database$EndOfStreamException Database$InvalidRangeException]
    [java.io ByteArrayInputStream DataInputStream]
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
