(ns xitdb.replication
  "Keeps a copy of a database (a replica) up to date by sending it only the
  bytes it is missing. See doc/REPLICATION.md."
  (:require
    [clojure.java.io :as io]
    [xitdb.db :as db])
  (:import
    [io.github.radarroark.xitdb
     Core Database Database$EndOfStreamException Database$InvalidDatabaseException
     Database$TruncatedDatabaseException Database$UnexpectedTagException ReplicaState]
    [java.io ByteArrayInputStream ByteArrayOutputStream EOFException IOException InputStream
     OutputStream SequenceInputStream]
    [java.nio ByteBuffer]
    [java.nio.channels FileLock]
    [java.util.concurrent.locks ReentrantLock]
    [xitdb.db XITDBDatabase]))

(defn replica-state
  "Describes what the replica `xdb` already has, so a source database can send
  it only the bytes it is missing. Returns a map of plain values,
  `{:committed-size n :checksum c}`, which can be sent to the source any way you
  like and passed to `(io/input-stream source :since state)`.

  A replica whose file is broken, such as one left behind by an interrupted
  full copy, has nothing a source could extend, so it is described as empty,
  `{:committed-size 0 :checksum 0}`, and the next sync sends a full copy."
  [^XITDBDatabase xdb]
  (let [^ReplicaState state (try
                              (.replicaState ^Database (.-rodb xdb))
                              (catch EOFException _ nil)
                              (catch Database$InvalidDatabaseException _ nil)
                              (catch Database$TruncatedDatabaseException _ nil)
                              (catch Database$UnexpectedTagException _ nil))]
    (if state
      {:committed-size (.committedSize state)
       :checksum       (.checksum state)}
      {:committed-size 0 :checksum 0})))

(defn- since->replica-state
  "Converts a `replica-state` map to the engine's `ReplicaState`, refusing
  anything else so that a malformed state, such as one built from missing
  query parameters, fails with a clear message instead of a NullPointerException."
  ^ReplicaState [since]
  (let [{:keys [committed-size checksum]} since]
    (when-not (and (map? since) (nat-int? committed-size) (int? checksum))
      (throw (IllegalArgumentException.
               (str ":since must be a map of :committed-size and :checksum as returned by replica-state, got "
                    (pr-str since)))))
    (ReplicaState. (long committed-size) (long checksum))))

(defn can-extend?
  "Returns true when `source` can extend a replica in `state`, a `replica-state`
  map, by sending it only the bytes it is missing, and false when the next update
  would be a full copy that replaces the replica. This lets an app find out
  before syncing that the replica will be replaced, so it can stage the copy
  itself: sync into a new database and switch to it."
  [^XITDBDatabase source state]
  (pos? (.replicationStart ^Database (.-rodb source) (since->replica-state state))))

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
                       (.replicationStart db (since->replica-state since))
                       0)
        end          (.committedSize db)
        frame        (doto (ByteBuffer/allocate frame-length)
                       (.putLong start)
                       (.putLong (- end start)))]
    (SequenceInputStream. (ByteArrayInputStream. (.array frame)) (.openCommitted db start end))))

(defn- discard-uncommitted!
  "Truncates `db` back to `committed`, its size when the update began. This drops
  the bytes of an update that failed, including any still in the writer's buffer,
  which would otherwise be written to the file whenever the buffer is next
  flushed, such as when the handle is closed, over whatever another handle has
  committed there by then."
  [^Database db committed]
  (let [^Core core (.-core db)]
    (when (> (.length core) committed)
      (.setLength core committed))))

(defn- updates-output-stream
  "Opens a stream that applies bytes from `updates-input-stream` to the replica
  `xdb` and commits them when it is closed. Holds the write lock until then, like
  `swap!`, so it must be closed on the thread that opened it.

  Once the frame has arrived, the bytes go straight to the replica. Bytes that
  start at the beginning of a database are a full copy, which truncates the
  replica first. If a write fails or the stream ends early, an update that
  extends the replica is discarded (see `discard-uncommitted!`), leaving it at
  its last commit, while a full copy leaves it broken until the next one (see
  `replica-state`). Closing the stream throws in the second case. An update
  the engine refuses, such as one replayed from a stale `replica-state`, never
  touches the replica, so nothing is discarded then either. Nor is anything
  discarded once the engine has begun committing: if that fails, the replica
  is left as the engine left it.

  When the update is committed, `replicate!`'s return value is put in the
  volatile `result` while the write lock is still held."
  (^OutputStream [xdb]
   (updates-output-stream xdb (volatile! nil)))
  (^OutputStream [^XITDBDatabase xdb result]
   (let [^ReentrantLock lock (.-lock xdb)
         ^Database writer    (.-rwdb xdb)
         ^Database reader    (.-rodb xdb)]
     (when (.isHeldByCurrentThread lock)
       (throw (IllegalStateException. "a replica should not be written to from swap! or reset!")))
     (.lock lock)
     (let [file-lock (try
                       (db/lock-file writer)
                       (catch Throwable t
                         (.unlock lock)
                         (throw t)))
           ;; holds the start of the stream until the frame has arrived
           prefix      (ByteArrayOutputStream.)
           frame-start (volatile! nil)
           out         (volatile! nil)
           failed?     (volatile! false)
           closed?     (volatile! false)
           ;; only an update the engine accepted has written anything
           discard!    (fn []
                         (let [start @frame-start]
                           (when (and @out (pos? start))
                             (discard-uncommitted! writer start))))
           write!      (fn [^bytes buffer off len]
                         (when @closed?
                           (throw (IOException. "stream closed")))
                         (try
                           (if-let [^OutputStream o @out]
                             (.write o buffer (int off) (int len))
                             (let [_         (.write prefix buffer (int off) (int len))
                                   ^bytes bs (.toByteArray prefix)]
                               (when (>= (alength bs) frame-length)
                                 (let [bb    (ByteBuffer/wrap bs)
                                       start (vreset! frame-start (.getLong bb 0))
                                       o     (vreset! out (.openReplicated writer start (.getLong bb 8)))]
                                   (.write ^OutputStream o bs frame-length (- (alength bs) frame-length))))))
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
               (if @failed?
                 (discard!)
                 (let [^OutputStream o (or @out (throw (Database$EndOfStreamException.)))]
                   (try
                     ;; commits, or throws if the stream ended early
                     (.close o)
                     (catch Database$EndOfStreamException t
                       (try (discard!) (catch Throwable e (.addSuppressed t e)))
                       (throw t)))
                   ;; if the replica is now a copy of a different file, the reader
                   ;; handle needs its header too, which holds the file ID
                   (set! (.-header reader) (.-header writer))
                   (vreset! result {:full-copy?    (zero? @frame-start)
                                    :replica-state (replica-state xdb)})))
               (finally
                 (try
                   (some-> ^FileLock file-lock .close)
                   (finally
                     (.unlock lock))))))))))))

;; (io/input-stream source :since state) opens the bytes the replica described
;; by `state` is missing, and (io/output-stream replica) applies them, so
;; (io/copy in out) replicates. see the README.
(extend XITDBDatabase
  io/IOFactory
  (assoc io/default-streams-impl
    :make-input-stream (fn [xdb opts] (updates-input-stream xdb (:since opts)))
    :make-output-stream (fn [xdb _opts] (updates-output-stream xdb))
    ;; the streams are binary, so a Reader or Writer over them would be a mistake
    :make-reader (fn [_xdb _opts]
                   (throw (IllegalArgumentException. "Cannot open a database as a Reader; use io/input-stream")))
    :make-writer (fn [_xdb _opts]
                   (throw (IllegalArgumentException. "Cannot open a database as a Writer; use io/output-stream")))))

(defn replicate!
  "Applies the updates read from `in` to the replica `xdb` and commits them.
  `in` is an InputStream of the bytes from `(io/input-stream source :since
  state)`, such as that stream itself or an HTTP response body carrying it. It
  is read to the end but not closed. Returns `{:full-copy? b :replica-state s}`,
  where `:full-copy?` is true when the update replaced the replica with a full
  copy and `:replica-state` is the replica's `replica-state` right after the
  update, read before another sync can change it.

  This is `(io/copy in (io/output-stream xdb))`, except that if reading `in`
  fails, such as when a connection drops, that exception is thrown, with the
  output stream's failure to commit the partial update attached as suppressed.
  `with-open` would throw the latter instead. Unless the update had already
  arrived in full, the replica stays at its last commit, or, if the update was a
  full copy, is left broken until the next sync (see `replica-state`)."
  [xdb ^InputStream in]
  (let [result            (volatile! nil)
        ^OutputStream out (updates-output-stream xdb result)]
    (try
      (io/copy in out)
      (catch Throwable t
        (try
          (.close out)
          (catch Throwable close-error
            (.addSuppressed t close-error)))
        (throw t)))
    (.close out)
    @result))

