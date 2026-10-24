(ns xitdb.db
  (:require
    [clojure.java.io :as io]
    [xitdb.common :as common]
    [xitdb.util.conversion :as conversion]
    [xitdb.util.db-context :as db-context]
    [xitdb.xitdb-types :as xtypes])
  (:import
    [io.github.radarroark.xitdb
     Core CoreBufferedFile CoreFile CoreMemory CoreReadOnlyFile Database Database$ContextFunction
     Database$EndOfStreamException FileOffsetMap Hasher RandomAccessBufferedFile RandomAccessMemory
     ReadArrayList ReadCursor ReplicaState WriteArrayList WriteCursor WriteHashMap]
    [java.io ByteArrayInputStream ByteArrayOutputStream File IOException InputStream OutputStream
     RandomAccessFile SequenceInputStream]
    [java.nio ByteBuffer]
    [java.nio.channels FileLock]
    [java.nio.file Files]
    [java.nio.file.attribute FileAttribute]
    [java.security MessageDigest]
    [java.util.concurrent.locks ReentrantLock]))

;; When set to true, swap! returns [history-count value-before value-after]
;; instead of the new value. history-count is the number of history entries
;; after the write, so the written version is (deref-at db (dec history-count)).
;; On a cursor the values are those at its keypath.
(defonce ^:dynamic *return-history?* false)

;; Avoid extra require in your ns
(def materialize common/materialize)

(def ^:private key-hash-format-id
  ;; Version the Clojure key encoding, not just the underlying SHA-1 digest.
  ;; Unversioned files used printer-dependent hashes and cannot be reopened
  ;; safely with the canonical encoding. Compaction preserves this header ID.
  (Hasher/stringToId "clj1"))

(defn open-database
  "Opens database `filename`.
  If `filename` is `:memory`, returns a memory based db.
  open-mode can be `r` or `rw`."
  [filename ^String open-mode]
  (let [^Core core (if (= filename :memory)
                     (CoreMemory. (RandomAccessMemory.))
                     (CoreBufferedFile. (RandomAccessBufferedFile. (File. ^String filename) open-mode)))]
    (try
      (let [db (Database. core (Hasher. (MessageDigest/getInstance "SHA-1") key-hash-format-id))]
        (when-not (= key-hash-format-id (.hashId (.-header db)))
          (throw (IllegalArgumentException.
                   (str "Unsupported key hash format. Export the data using the xitdb-clj version "
                        "that created this file, then import it into a new database. "
                        "The original file has not been modified."))))
        db)
      (catch Throwable t
        (.close core)
        (throw t)))))


(defn ^WriteArrayList db-history [^Database db]
  (WriteArrayList. (.rootCursor db)))

(defn append-context!
  "Appends a new history context and calls `fn` with a write cursor.
  Returns the history count after the append; the new entry is at `(dec count)`."
  [^WriteArrayList history slot fn]
  (.appendContext
    history
    slot
    (reify Database$ContextFunction
      (^void run [_ ^WriteCursor cursor]
        (fn cursor)
        nil)))
  (.count history))

(defn xitdb-reset!
  "Sets the value of the database to `new-value`.
  Returns the new history count."
  [^WriteArrayList history new-value]
  (append-context! history nil (fn [^WriteCursor cursor]
                                 (.write cursor (conversion/v->slot! cursor new-value)))))

(defn v->slot!
  "Converts a value to a slot which can be written to a cursor.
  XITDB* types are written by reference (structural sharing) when they belong
  to the same database as `cursor`; values from another database are refused.
  All other types are converted by `conversion/v->slot!`."
  [^WriteCursor cursor v]
  (conversion/v->slot! cursor v))

(defn xitdb-swap!
  "Starts a new transaction and calls `f` with the value at `base-keypath`.
  If `base-keypath` is nil, will use the root cursor.
  `f` will receive a XITDBWrite* type with the value at `base-keypath` and `args`.
  Actions on the XITDBWrite* type (like `assoc`) will mutate it.
  Return value of `f` is written at `base-keypath` (or root) cursor.
  Returns the new history count."
  [db base-keypath f & args]
  (let [history (db-history db)
        slot    (.getSlot history -1)]
    (append-context!
      history
      slot
      (fn [^WriteCursor root-cursor]
        (let [cursor (conversion/keypath-cursor root-cursor base-keypath)
              obj    (xtypes/read-from-cursor cursor true)
              retval (apply f (into [obj] args))
              ;; the callback may have frozen the original destination
              cursor (conversion/keypath-cursor root-cursor base-keypath)]
          (.write cursor (conversion/v->slot! cursor retval)))))))

(defn- lock-file
  "Locks the file of `db`, if it has one. Returns the lock, or nil."
  ^FileLock [^Database db]
  (let [core (.-core db)]
    (when (instance? CoreBufferedFile core)
      (or (.tryLock (.getChannel (.-file ^CoreBufferedFile core)))
          (throw (IllegalStateException. "Database is locked for writing"))))))

(defn- with-file-lock [^Database db f]
  (if-let [held (lock-file db)]
    (with-open [held held]
      (f))
    (f)))

(defn xitdb-swap-with-lock!
  "Performs the 'swap!' operation while locking `db.lock`.
  Returns the new value of the database.
  If the binding `*return-history?*` is true, returns
  `[history-count value-before value-after]`, where the written version is
  `(deref-at xitdb (dec history-count))`.
  If `keypath` is not empty, the result of `f` will be written to the db at `keypath` rather
  than db root.
  Similarly, if `keypath` is not empty, the returned value will be the value at `keypath`."
  [xitdb base-keypath f & args]
  (let [^ReentrantLock lock (.-lock xitdb)]
    (when (.isHeldByCurrentThread lock)
      (throw (IllegalStateException. "swap! should not be called from swap! or reset!")))
    (try
      (.lock lock)
      (with-file-lock (.-rwdb xitdb)
        (fn []
          (let [old-value (when *return-history?* (get-in (deref xitdb) base-keypath))
                n         (apply xitdb-swap! (into [(-> xitdb .rwdb) base-keypath f] args))
                new-value (get-in (deref xitdb) base-keypath)]
            (if *return-history?*
              [n old-value new-value]
              new-value))))
      (finally
        (.unlock lock)))))

(defn- close-db-internal!
  "Closes the underlying core of `db` (the file handle for file databases,
  the in-memory buffer for memory databases)."
  [^Database db]
  (.close ^Core (.-core db)))

(defn- close-after-failure!
  "Closes a core without replacing the failure that triggered cleanup."
  [^Core core ^Throwable failure]
  (try
    (.close core)
    (catch Throwable close-error
      (.addSuppressed failure close-error))))

(defn- delete-after-failure!
  "Deletes a file without replacing the failure that triggered cleanup."
  [^File file ^Throwable failure]
  (try
    (Files/deleteIfExists (.toPath file))
    (catch Throwable delete-error
      (.addSuppressed failure delete-error))))


(defn ^ReadArrayList read-history
  "Returns the read only transaction history array."
  [^Database db]
  (ReadArrayList. (-> db .rootCursor)))

(def ^:deprecated history-index count)

(defn deref-at
  "Returns the version of the data at the specified index."
  [xdb index]
  (let [history (read-history (.-rodb xdb))
        cursor  (.getCursor history index)]
    (xtypes/read-from-cursor cursor false)))

(deftype XITDBDatabase [rodb rwdb lock]

  java.io.Closeable
  (close [this]
    (close-db-internal! rodb)
    (close-db-internal! rwdb))

  clojure.lang.IDeref
  (deref [this]
    (deref-at this -1))

  clojure.lang.Counted
  (count [this]
    (.count (read-history rodb)))

  clojure.lang.IAtom

  (reset [this new-value]

    (when (.isHeldByCurrentThread lock)
      (throw (IllegalStateException. "reset! should not be called from swap! or reset!")))

    (try
      (.lock lock)
      (with-file-lock rwdb
        (fn []
          (let [history (db-history rwdb)]
            (xitdb-reset! history new-value)
            (deref this))))
      (finally
        (.unlock lock))))

  (swap [this f]
    (xitdb-swap-with-lock! this nil f))

  (swap [this f a]
    (xitdb-swap-with-lock! this nil f a))

  (swap [this f a1 a2]
    (xitdb-swap-with-lock! this nil f a1 a2))

  (swap [this f x y args]
    (apply xitdb-swap-with-lock! (concat [this nil f x y] args))))

(defn- wrap-db
  "Wraps writer handle `rwdb` into an XITDBDatabase, opening its reader handle.

  Reads go through one shared read-only handle, so a value read on one thread
  can be used from any other. The engine's read path only touches the handle's
  Core and its immutable header: the in-memory Core keeps a per-thread position
  and `CoreReadOnlyFile` keeps a per-thread file handle, and key hashing
  uses independent engine digests (see `conversion/db-key-hash`). Writes go through
  `rwdb` under the lock.

  The writer refers to its reader so values from that reader are accepted
  when written back and values from other databases are not."
  [filename ^Database rwdb]
  (let [^Core ro-core (if (= :memory filename)
                        (.-core rwdb)
                        (CoreReadOnlyFile. (File. ^String filename)))
        context       (try
                        (db-context/create ro-core (.-core rwdb) (.hasher rwdb))
                        (catch Throwable t
                          (when-not (= :memory filename)
                            (close-after-failure! ro-core t))
                          (throw t)))]
    (->XITDBDatabase (:reader context) (:writer context) (ReentrantLock.))))

(defn xit-db
  "Returns a new XITDBDatabase which can be used to query and transact data.
  `filename` can be `:memory` or the name of a file on the filesystem.
  If the file does not exist, it will be created.
  The returned database handle can be used from multiple threads, and so can the
  values read from it. Reads can run in parallel, transactions (eg. `swap!`) will
  only allow one writer at a time."
  [filename]
  (let [^Database writer (open-database filename "rw")]
    (try
      (wrap-db filename writer)
      (catch Throwable t
        (close-after-failure! (.-core writer) t)
        (throw t)))))

(defn- create-compact-target [filename]
  (if (= :memory filename)
    {:core (CoreMemory. (RandomAccessMemory.))}
    (let [file (File. ^String filename)]
      (Files/createFile (.toPath file) (make-array FileAttribute 0))
      (try
        {:core (CoreBufferedFile. (RandomAccessBufferedFile. file "rw"))
         :file file}
        (catch Throwable t
          (Files/deleteIfExists (.toPath file))
          (throw t))))))

(defn compact
  "Compacts the latest value of `xdb` into a new database at `target`.
  `target` can be `:memory` or the name of a file that does not exist.
  Returns an open XITDBDatabase containing at most one history entry. The source
  database is unchanged.

  Holds the source's write lock for the whole copy, so `swap!` and `reset!` on
  `xdb` block until compaction finishes. Must not be called from inside a
  `swap!` or `reset!` on `xdb`; doing so throws IllegalStateException.

  Refreshes the source writer's header before copying, so a handle opened
  before another handle initialized the file compacts the current value
  instead of publishing an empty database."
  [^XITDBDatabase xdb target]
  (let [^ReentrantLock lock (.-lock xdb)]
    (when (.isHeldByCurrentThread lock)
      (throw (IllegalStateException. "compact should not be called from swap! or reset!")))
    (try
      (.lock lock)
      (let [target-info       (create-compact-target target)
            ^Core target-core (:core target-info)]
        (try
          (let [^Database source (.-rwdb xdb)
                _                (.rootCursor source)
                offsets-file     (File/createTempFile "compact_offsets" ".db")
                compacted        (try
                                   (with-open [offsets-core (CoreFile. (RandomAccessFile. offsets-file "rw"))]
                                     ;; compact inside one transaction on the scratch database,
                                     ;; so the offsets aren't each committed separately
                                     (let [offsets-db (Database. offsets-core (Hasher. (MessageDigest/getInstance "SHA-256")))
                                           result     (volatile! nil)]
                                       (.runContext offsets-db
                                                    (reify Database$ContextFunction
                                                      (^void run [_ ^WriteCursor cursor]
                                                        (vreset! result (.compact source target-core (FileOffsetMap. (WriteHashMap. cursor))))
                                                        nil)))
                                       @result))
                                   (catch Throwable t
                                     (delete-after-failure! offsets-file t)
                                     (throw t)))]
            (Files/deleteIfExists (.toPath offsets-file))
            (wrap-db target compacted))
          (catch Throwable t
            ;; Clean up the target without hiding the original error
            (close-after-failure! target-core t)
            (when-let [^File file (:file target-info)]
              (delete-after-failure! file t))
            (throw t))))
      (finally
        (.unlock lock)))))


;; Replication

(defn replica-state
  "Describes what the replica `xdb` already has, so a source database can send
  it only the bytes it is missing. Returns a map of plain values,
  `{:committed-size n :checksum c}`, which can be sent to the source any way you
  like and passed to `(io/input-stream source :since state)`."
  [^XITDBDatabase xdb]
  (let [state (.replicaState ^Database (.-rodb xdb))]
    {:committed-size (.committedSize state)
     :checksum       (.checksum state)}))

(def ^:private frame-length
  ;; replication streams begin with two big-endian longs: where the bytes
  ;; start in the database and how many there are
  16)

(defn- updates-input-stream
  "Opens a stream of the bytes that the replica described by `since` is missing,
  preceded by their start and length. Without `since`, or if the replica can't
  be extended, it contains the whole database. Only committed data is read, so
  this doesn't block writers."
  ^InputStream [^XITDBDatabase xdb since]
  (let [^Database db (.-rodb xdb)
        start        (if since
                       (.replicationStart db (ReplicaState. (:committed-size since) (:checksum since)))
                       0)
        end          (.committedSize db)
        frame        (doto (ByteBuffer/allocate frame-length)
                       (.putLong start)
                       (.putLong (- end start)))]
    (SequenceInputStream. (ByteArrayInputStream. (.array frame)) (.openCommitted db start end))))

(defn- updates-output-stream
  "Opens a stream that applies bytes from `updates-input-stream` to the replica
  `xdb` when it is closed. Holds the write lock until then, like `swap!`, so it
  must be closed on the thread that opened it."
  ^OutputStream [^XITDBDatabase xdb]
  (let [^ReentrantLock lock (.-lock xdb)
        ^Database writer    (.-rwdb xdb)
        ^Database reader    (.-rodb xdb)]
    (when (.isHeldByCurrentThread lock)
      (throw (IllegalStateException. "a replica should not be written to from swap! or reset!")))
    (.lock lock)
    (let [file-lock (try
                      (lock-file writer)
                      (catch Throwable t
                        (.unlock lock)
                        (throw t)))
          ;; holds the start of the stream until the frame has arrived
          prefix    (ByteArrayOutputStream.)
          out       (volatile! nil)
          failed?   (volatile! false)
          closed?   (volatile! false)
          write!    (fn [^bytes buffer off len]
                      (when @closed?
                        (throw (IOException. "stream closed")))
                      (try
                        (if-let [^OutputStream o @out]
                          (.write o buffer (int off) (int len))
                          (let [_         (.write prefix buffer (int off) (int len))
                                ^bytes bs (.toByteArray prefix)]
                            (when (>= (alength bs) frame-length)
                              (let [bb (ByteBuffer/wrap bs)
                                    o  (.openReplicated writer (.getLong bb 0) (.getLong bb 8))]
                                (vreset! out o)
                                (.write o bs frame-length (- (alength bs) frame-length))))))
                        (catch Throwable t
                          (vreset! failed? true)
                          (throw t))))]
      (proxy [OutputStream] []
        (write
          ([b]
           (if (bytes? b)
             (write! b 0 (alength ^bytes b))
             (write! (byte-array [(unchecked-byte b)]) 0 1)))
          ([buffer off len]
           (write! buffer off len)))
        (close []
          (when-not @closed?
            (vreset! closed? true)
            (try
              ;; after a failed write, the replica stays at its last commit
              (when-not @failed?
                (let [^OutputStream o (or @out (throw (Database$EndOfStreamException.)))]
                  ;; commits, or throws if the stream ended early
                  (.close o)
                  ;; if the replica is now a copy of a different file, the reader
                  ;; handle needs its header and file ID too
                  (set! (.-header reader) (.-header writer))
                  (set! (.-fileId reader) (.-fileId writer))))
              (finally
                (try
                  (some-> ^FileLock file-lock .close)
                  (finally
                    (.unlock lock)))))))))))

;; (io/input-stream source :since state) opens the bytes the replica described
;; by `state` is missing, and (io/output-stream replica) applies them, so
;; (io/copy in out) replicates. see the README.
(extend XITDBDatabase
  io/IOFactory
  (assoc io/default-streams-impl
    :make-input-stream (fn [xdb opts] (updates-input-stream xdb (:since opts)))
    :make-output-stream (fn [xdb _opts] (updates-output-stream xdb))))


(deftype XITDBCursor [xdb keypath]

  java.io.Closeable
  (close [this])

  clojure.lang.IDeref
  (deref [this]
    (let [v (deref xdb)]
      (get-in v keypath)))

  clojure.lang.IAtom

  (reset [this new-value]
    (xitdb-swap-with-lock! xdb keypath (constantly new-value)))

  (swap [this f]
    (xitdb-swap-with-lock! xdb keypath f))

  (swap [this f a]
    (xitdb-swap-with-lock! xdb keypath f a))

  (swap [this f a1 a2]
    (xitdb-swap-with-lock! xdb keypath f a1 a2))

  (swap [this f x y args]
    (apply xitdb-swap-with-lock! (concat [xdb keypath f x y] args))))

(defn xdb-cursor [xdb keypath]
  (cond
    (instance? XITDBCursor xdb)
    (XITDBCursor. (.-xdb xdb) (vec (concat (.-keypath xdb) keypath)))

    (instance? XITDBDatabase xdb)
    (XITDBCursor. xdb keypath)

    :else
    (throw (IllegalArgumentException. (str "xdb must be an instance of XITDBCursor or XITDBDatabase, got: " (type xdb))))))

(defn freeze!
  "Prevents all data written in the current transaction from
  being mutated by any remaining changes. Throws if called
  outside of a transaction. Returns a read-only version of the
  given writeable data structure."
  [x]
  (let [^ReadCursor cursor (when (common/wrapper? x)
                             (-> x common/-unwrap .cursor))]
    (when-not (instance? WriteCursor cursor)
      (throw (IllegalArgumentException.
               (str "freeze! requires a writeable XITDB data structure, got: " (type x)))))
    (let [^Database writer (.-db cursor)
          ^Database reader (db-context/reader-database writer)]
      (.freeze writer)
      (.flush ^Core (.-core writer))
      ;; rebuild on the reader handle so the value can be shared across threads
      (xtypes/read-from-cursor (ReadCursor. (.-slotPtr cursor) reader) false))))
