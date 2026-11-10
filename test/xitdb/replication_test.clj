(ns xitdb.replication-test
  (:require
    [clojure.java.io :as io]
    [clojure.test :refer :all]
    [xitdb.db :as xdb]
    [xitdb.replication :as replication])
  (:import
    [io.github.radarroark.xitdb CoreMemory Database Database$EndOfStreamException Database$InvalidRangeException]
    [java.io ByteArrayInputStream DataInputStream InputStream IOException SequenceInputStream]
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
  (with-open [in  (io/input-stream source :since (replication/replica-state replica))
              out (io/output-stream replica)]
    (io/copy in out)))

(defn- update-bytes
  "The bytes that `source` would send to `replica`."
  ^bytes [source replica]
  (with-open [in (io/input-stream source :since (replication/replica-state replica))]
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
        (is (= (replication/replica-state source) (replication/replica-state replica)))

        ;; later syncs only send the new bytes
        (swap! source update :tags conj :c)
        (is (= (:committed-size (replication/replica-state replica))
               (frame-start (update-bytes source replica))))
        (sync! source replica)
        (is (= #{:a :b :c} (xdb/materialize (:tags @replica))))
        (is (= (count source) (count replica)))
        (is (= (xdb/materialize (xdb/deref-at source 1)) (xdb/materialize (xdb/deref-at replica 1))))

        ;; an up to date replica gets nothing
        (let [update (update-bytes source replica)]
          (is (= 16 (alength update))))
        (sync! source replica)
        (is (= (replication/replica-state source) (replication/replica-state replica)))))))

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
          state  (replication/replica-state replica)]
      ;; the stream ends early, so closing it throws instead of committing
      (is (thrown? Database$EndOfStreamException
                   (with-open [out (io/output-stream replica)]
                     (.write out update 0 (dec (alength update))))))
      (is (= state (replication/replica-state replica)))
      (is (= {:n 1} (xdb/materialize @replica)))
      ;; it can still be written to and synced
      (sync! source replica)
      (is (= {:n 2} (xdb/materialize @replica)))
      ;; extra bytes are rejected
      (swap! source assoc :n 3)
      (let [update (update-bytes source replica)
            state  (replication/replica-state replica)]
        (is (thrown? Database$InvalidRangeException
                     (with-open [out (io/output-stream replica)]
                       (.write out update)
                       (.write out (byte-array 1)))))
        (is (= state (replication/replica-state replica))))
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

(deftest interrupted-full-copy-is-recovered-by-the-next-sync
  (doseq [kind [:memory :file]
          ;; cuts count the 16-byte frame. cut once the engine has begun
          ;; replacing the replica (past the source's 32 bytes of headers),
          ;; and one byte before the end
          cut  [(+ 16 48) dec]]
    (testing (str (name kind) " replica, cut at " (if (fn? cut) "the last byte" cut))
      (with-open [source  (xdb/xit-db :memory)
                  replica (xdb/xit-db (location kind))]
        (reset! source {:n 1})
        (sync! source replica)
        ;; a compacted source is a new file, so the next sync is a full copy
        (with-open [compacted (xdb/compact source :memory)]
          (let [update (update-bytes compacted replica)
                len    (if (fn? cut) (cut (alength update)) cut)]
            (is (zero? (frame-start update)))
            (is (thrown? Database$EndOfStreamException
                         (with-open [out (io/output-stream replica)]
                           (.write out update 0 len))))
            ;; the replica is broken, so it is described as empty
            (is (= {:committed-size 0 :checksum 0} (replication/replica-state replica)))
            (is (zero? (frame-start (update-bytes compacted replica))))
            (sync! compacted replica)
            (is (= {:n 1} (xdb/materialize @replica)))
            (is (= (replication/replica-state compacted) (replication/replica-state replica)))
            ;; and it can be extended again afterwards
            (swap! compacted assoc :n 2)
            (is (pos? (frame-start (update-bytes compacted replica))))
            (sync! compacted replica)
            (is (= {:n 2} (xdb/materialize @replica)))))))))

(deftest full-copy-cut-within-the-headers-leaves-the-replica-untouched
  ;; the engine holds a full copy's headers (the header and the file size, 32
  ;; bytes in format version 2) until they have all arrived and only then
  ;; truncates the replica, so a copy cut before that changes nothing
  (doseq [kind [:memory :file]]
    (testing (str (name kind) " replica")
      (with-open [source  (xdb/xit-db :memory)
                  replica (xdb/xit-db (location kind))]
        (reset! source {:n 1})
        (sync! source replica)
        (let [state (replication/replica-state replica)]
          (with-open [compacted (xdb/compact source :memory)]
            (let [update (update-bytes compacted replica)]
              (is (zero? (frame-start update)))
              (is (thrown? Database$EndOfStreamException
                           (with-open [out (io/output-stream replica)]
                             (.write out update 0 (+ 16 24)))))
              (is (= state (replication/replica-state replica)))
              (is (= {:n 1} (xdb/materialize @replica)))
              (sync! compacted replica)
              (is (= (replication/replica-state compacted) (replication/replica-state replica)))
              (is (= {:n 1} (xdb/materialize @replica))))))))))

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
        (is (= (replication/replica-state fork) (replication/replica-state replica)))))))

(defn- failing-stream
  "Delivers the first `n` bytes of `bs`, then fails like a dropped connection."
  ^InputStream [^bytes bs n]
  (SequenceInputStream.
    (ByteArrayInputStream. bs 0 n)
    (proxy [InputStream] []
      (read
        ([] (throw (IOException. "connection reset")))
        ([_ _ _] (throw (IOException. "connection reset")))))))

(deftest replicate!-applies-updates
  (with-open [source  (xdb/xit-db :memory)
              replica (xdb/xit-db (location :file))]
    (reset! source {:n 1})
    (with-open [in (io/input-stream source :since (replication/replica-state replica))]
      (is (= {:full-copy? true :replica-state (replication/replica-state source)}
             (replication/replicate! replica in))))
    (is (= {:n 1} (xdb/materialize @replica)))
    (swap! source assoc :n 2)
    (with-open [in (io/input-stream source :since (replication/replica-state replica))]
      (is (= {:full-copy? false :replica-state (replication/replica-state source)}
             (replication/replicate! replica in))))
    (is (= {:n 2} (xdb/materialize @replica)))
    (is (= (replication/replica-state source) (replication/replica-state replica)))))

(deftest replicate!-throws-the-read-error
  (doseq [full-copy? [true false]]
    (testing (if full-copy? "full copy" "extension")
      (with-open [source  (xdb/xit-db :memory)
                  replica (xdb/xit-db (location :file))]
        (reset! source {:n 1})
        (when-not full-copy?
          (sync! source replica))
        (swap! source assoc :n 2)
        (let [update (update-bytes source replica)
              state  (replication/replica-state replica)
              before (xdb/materialize @replica)
              error  (try
                       (replication/replicate! replica (failing-stream update (quot (alength update) 2)))
                       nil
                       (catch Throwable t t))]
          (is (= (zero? (frame-start update)) full-copy?))
          (is (instance? IOException error))
          (is (= "connection reset" (some-> error .getMessage)))
          (is (some #(instance? Database$EndOfStreamException %) (some-> error .getSuppressed)))
          (if full-copy?
            ;; a full copy had already truncated the replica
            (is (= {:committed-size 0 :checksum 0} (replication/replica-state replica)))
            (do
              (is (= state (replication/replica-state replica)))
              (is (= before (xdb/materialize @replica)))))
          ;; the write lock was released
          (sync! source replica)
          (is (= {:n 2} (xdb/materialize @replica))))))))

(deftest can-extend?-tells-whether-the-next-sync-is-incremental
  (with-open [source  (xdb/xit-db :memory)
              replica (xdb/xit-db (location :file))]
    (reset! source {:n 1})
    (testing "a new replica needs a full copy"
      (is (false? (replication/can-extend? source (replication/replica-state replica)))))
    (sync! source replica)
    (swap! source assoc :n 2)
    (testing "a synced replica can be extended"
      (is (true? (replication/can-extend? source (replication/replica-state replica)))))
    (testing "a compacted source is a new file"
      (with-open [compacted (xdb/compact source :memory)]
        (is (false? (replication/can-extend? compacted (replication/replica-state replica))))))
    (testing "a replica with its own commit has diverged"
      (swap! replica assoc :local true)
      (is (false? (replication/can-extend? source (replication/replica-state replica)))))
    (testing "state must be a replica-state"
      (is (thrown? IllegalArgumentException (replication/can-extend? source {:committed-size nil}))))))

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

(deftest rejected-update-leaves-the-replica-intact
  ;; an update whose start isn't the replica's committed size, such as one
  ;; replayed from a stale replica-state, is refused by the engine before any
  ;; byte is written, so nothing may be discarded afterwards
  (doseq [kind [:memory :file]]
    (testing (name kind)
      (with-open [source  (xdb/xit-db :memory)
                  replica (xdb/xit-db (location kind))]
        (reset! source {:n 1})
        (sync! source replica)
        (swap! source assoc :n 2)
        (let [update (update-bytes source replica)]
          (sync! source replica)
          (let [state (replication/replica-state replica)]
            (is (thrown? Database$InvalidRangeException
                         (with-open [out (io/output-stream replica)]
                           (.write out update))))
            (is (= state (replication/replica-state replica)))
            (is (= {:n 2} (xdb/materialize @replica)))))))))

(defn- failing-second-sync
  "A memory core over the storage of `core` whose second `sync` fails, as a disk
  might after an update's bytes are written but when its header is."
  ^CoreMemory [^CoreMemory core]
  (let [calls (atom 0)]
    (proxy [CoreMemory] [(.-memory core)]
      (sync []
        (when (= 2 (swap! calls inc))
          (throw (IOException. "sync failed")))))))

(deftest failed-commit-sync-does-not-discard-the-update
  ;; the engine writes the update's header before its final sync, so if that
  ;; sync fails the update is committed and must not be truncated away
  (with-open [source  (xdb/xit-db :memory)
              replica (xdb/xit-db :memory)]
    (reset! source {:n 1})
    (sync! source replica)
    (swap! source assoc :n 2)
    (let [^Database writer (.-rwdb replica)]
      (set! (.-core writer) (failing-second-sync (.-core writer)))
      (is (thrown-with-msg? IOException #"sync failed" (sync! source replica)))
      (is (= (replication/replica-state source) (replication/replica-state replica)))
      (is (= {:n 2} (xdb/materialize @replica))))))
