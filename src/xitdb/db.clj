(ns xitdb.db
  (:require
    [xitdb.common :as common]
    [xitdb.util.conversion :as conversion]
    [xitdb.util.db-registry :as db-registry]
    [xitdb.xitdb-types :as xtypes])
  (:import
    [io.github.radarroark.xitdb
     Core CoreBufferedFile CoreMemory Database Database$ContextFunction Hasher
     RandomAccessBufferedFile RandomAccessMemory ReadArrayList WriteArrayList WriteCursor]
    [java.io File]
    [java.nio.file Files]
    [java.nio.file.attribute FileAttribute]
    [java.security MessageDigest]
    [java.util WeakHashMap]
    [java.util.concurrent.locks ReentrantLock]))

;; When set to true,
;; swap! will return [current-history-index old-dbval new-dbval]
(defonce ^:dynamic *return-history?* false)

;; Avoid extra require in your ns
(def materialize common/materialize)

(defn open-database
  "Opens database `filename`.
  If `filename` is `:memory`, returns a memory based db.
  open-mode can be `r` or `rw`."
  [filename ^String open-mode]
  (let [^Core core (if (= filename :memory)
                     (CoreMemory. (RandomAccessMemory.))
                     (CoreBufferedFile. (RandomAccessBufferedFile. (File. ^String filename) open-mode)))]
    (try
      (Database. core (Hasher. (MessageDigest/getInstance "SHA-1")))
      (catch Throwable t
        (.close core)
        (throw t)))))


(defn ^WriteArrayList db-history [^Database db]
  (WriteArrayList. (.rootCursor db)))

(defn append-context!
  "Appends a new history context and calls `fn` with a write cursor.
  Returns the new history index."
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
  Returns new history index."
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
  Returns the transaction history index."
  [db base-keypath f & args]
  (let [history (db-history db)
        slot (.getSlot history -1)]
    (append-context!
      history
      slot
      (fn [^WriteCursor cursor]
        (let [cursor (conversion/keypath-cursor cursor base-keypath)
              obj (xtypes/read-from-cursor cursor true)]
          (let [retval (apply f (into [obj] args))]
            (.write cursor (conversion/v->slot! cursor retval))))))))

(defn xitdb-swap-with-lock!
  "Performs the 'swap!' operation while locking `db.lock`.
  Returns the new value of the database.
  If the binding `*return-history?*` is true, returns
  `[current-history-index db-before db-after]`.
  If `keypath` is not empty, the result of `f` will be written to the db at `keypath` rather
  than db root.
  Similarly, if `keypath` is not empty, the returned value will be the value at `keypath`."
  [xitdb base-keypath f & args]
  (let [^ReentrantLock lock (.-lock xitdb)]
    (when (.isHeldByCurrentThread lock)
      (throw (IllegalStateException. "swap! should not be called from swap! or reset!")))
    (try
      (.lock lock)
      (let [old-value (when *return-history?* (deref xitdb))
            index     (apply xitdb-swap! (into [(-> xitdb .rwdb) base-keypath f] args))
            new-value (deref xitdb)]
        (if *return-history?*
          [index old-value new-value]
          new-value))
      (finally
        (.unlock lock)))))

(defn- close-db-internal!
  "Closes the underlying core of `db` (the file handle for file databases,
  the in-memory buffer for memory databases)."
  [^Database db]
  (.close ^Core (.-core db)))


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
      (let [history (db-history rwdb)]
        (xitdb-reset! history new-value)
        (deref this))
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

(defn- ^Core thread-local-file-core
  "A read-only Core over `filename` that runs every operation against a file
  handle private to the calling thread. A buffered file handle keeps a position
  and a write buffer, so one handle cannot serve two threads; giving each thread
  its own handle behind a single Core lets one reader `Database` be shared by
  every thread. Handles are tracked weakly so terminated threads' handles can
  be collected and their file descriptors released while the database is open.
  Closing the Core closes every remaining handle. Registration and close share
  a lock so a newly opened handle cannot escape cleanup; a thread that first
  touches the Core after close gets an IllegalStateException."
  [^String filename]
  (let [closed? (atom false)
        handles (WeakHashMap.)
        tl      (proxy [ThreadLocal] []
                  (initialValue []
                    (locking handles
                      (when @closed?
                        (throw (IllegalStateException. "Database is closed")))
                      (let [core (CoreBufferedFile. (RandomAccessBufferedFile. (File. filename) "r"))]
                        (.put handles core true)
                        core))))
        current (fn ^Core [] (.get ^ThreadLocal tl))]
    (reify Core
      (reader [_] (.reader (current)))
      (writer [_] (.writer (current)))
      (length [_] (.length (current)))
      (seek [_ pos] (.seek (current) pos))
      (position [_] (.position (current)))
      (setLength [_ len] (.setLength (current) len))
      (flush [_] (.flush (current)))
      (sync [_] (.sync (current)))
      (close [_]
        (locking handles
          (reset! closed? true)
          (doseq [^Core core (.keySet handles)]
            (.close core))
          (.clear handles))))))

(defn- wrap-db
  "Wraps writer handle `rwdb` into an XITDBDatabase, opening its reader handle.

  Reads go through one shared read-only handle, so a value read on one thread
  can be used from any other. The engine's read path only touches the handle's
  Core and its immutable header: the in-memory Core keeps a per-thread position
  and `thread-local-file-core` keeps a per-thread file handle, and key hashing
  uses per-thread digests (see `conversion/db-key-hash`). Writes go through
  `rwdb` under the lock.

  Both handles are registered under one token so values read through the
  reader are accepted when written back and values from other databases are
  not."
  [filename ^Database rwdb]
  (let [algorithm (.getAlgorithm (.-md rwdb))
        ^Core ro-core (if (= :memory filename)
                        (.-core rwdb)
                        (thread-local-file-core filename))
        rodb      (try
                    (Database. ro-core (Hasher. (MessageDigest/getInstance algorithm)))
                    (catch Throwable t
                      (when-not (= :memory filename)
                        (.close ro-core))
                      (throw t)))
        token     (Object.)]
    (db-registry/register-database! rwdb token)
    (db-registry/register-database! rodb token)
    (->XITDBDatabase rodb rwdb (ReentrantLock.))))

(defn xit-db
  "Returns a new XITDBDatabase which can be used to query and transact data.
  `filename` can be `:memory` or the name of a file on the filesystem.
  If the file does not exist, it will be created.
  The returned database handle can be used from multiple threads, and so can the
  values read from it. Reads can run in parallel, transactions (eg. `swap!`) will
  only allow one writer at a time."
  [filename]
  (wrap-db filename (open-database filename "rw")))

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

  xitdb 0.34.0 shares the source's mutable digest with the copy. The two
  handles are written under independent locks, so the copy is given a digest
  of its own."
  [^XITDBDatabase xdb target]
  (let [^ReentrantLock lock (.-lock xdb)]
    (when (.isHeldByCurrentThread lock)
      (throw (IllegalStateException. "compact should not be called from swap! or reset!")))
    (try
      (.lock lock)
      (let [target-info (create-compact-target target)
            ^Core target-core (:core target-info)]
        (try
          (let [compacted (.compact ^Database (.-rwdb xdb) target-core)]
            (set! (.-md compacted)
                  (MessageDigest/getInstance (.getAlgorithm (.-md compacted))))
            (wrap-db target compacted))
          (catch Throwable t
            ;; Clean up the target without hiding the original error
            (try
              (.close target-core)
              (catch Throwable close-error
                (.addSuppressed ^Throwable t close-error)))
            (when-let [^File file (:file target-info)]
              (try
                (Files/deleteIfExists (.toPath file))
                (catch Throwable delete-error
                  (.addSuppressed ^Throwable t delete-error))))
            (throw t))))
      (finally
        (.unlock lock)))))


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
    (xitdb-swap-with-lock! xdb keypath  f))

  (swap [this f a]
    (xitdb-swap-with-lock! xdb keypath  f a))

  (swap [this f a1 a2]
    (xitdb-swap-with-lock! xdb keypath  f a1 a2))

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
  (when-not (satisfies? common/IReadOnly x)
    (throw (IllegalArgumentException.
            (str "freeze! requires a writeable XITDB data structure, got: " (type x)))))
  (-> x common/-unwrap .cursor .db .freeze)
  (common/-read-only x))
