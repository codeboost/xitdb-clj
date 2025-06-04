(ns xitdb.db
  (:require
    [xitdb.common :as common]
    [xitdb.util.conversion :as conversion]
    [xitdb.xitdb-types :as xtypes])
  (:import
    [io.github.radarroark.xitdb
     CoreBufferedFile CoreFile CoreMemory Database Database$ContextFunction Hasher
     RandomAccessBufferedFile RandomAccessMemory ReadArrayList WriteArrayList WriteCursor]
    [java.io File RandomAccessFile]
    [java.security MessageDigest]
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
  (let [core   (if (= filename :memory)
                 (CoreMemory. (RandomAccessMemory.))
                 (CoreBufferedFile. (RandomAccessBufferedFile. (File. ^String filename) open-mode)))
        hasher (Hasher. (MessageDigest/getInstance "SHA-1"))]
    (Database. core hasher)))


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
                                 (conversion/v->slot! cursor new-value))))

(defn v->slot!
  "Converts a value to a slot which can be written to a cursor.
  For XITDB* types (which support ISlot), will return `-slot`,
  for all other types `conversion/v->slot!`"
  [^WriteCursor cursor v]
  (if (satisfies? common/ISlot v)
    (common/-slot v)
    (conversion/v->slot! cursor v)))

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
            (.write cursor (v->slot! cursor retval))))))))

(defn xitdb-swap-with-lock!
  "Performs the 'swap!' operation while locking `db.lock`.
  Returns the new value of the database.
  If the binding `*return-history?*` is true, returns
  `[current-history-index db-before db-after]`."
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
  "Closes the db file. Does nothing if it's a memory db"
  [^Database db]
  (let [core (-> db .-core)]
    (when (instance? CoreFile core)
      (.close ^RandomAccessFile (-> db .-core .file)))))


(defn ^ReadArrayList read-history
  "Returns the read only transaction history array."
  [^Database db]
  (ReadArrayList. (-> db .rootCursor)))

(defn history-index
  "Returns the current size of the transaction history array."
  [xdb]
  (.count (read-history (-> xdb .tldbro .get))))

(deftype XITDBDatabase [tldbro rwdb lock]

  java.io.Closeable
  (close [this]
    (close-db-internal! (.get tldbro))
    (close-db-internal! rwdb))

  clojure.lang.IDeref
  (deref [this]
    (let [history (read-history (.get tldbro))
          cursor  (.getCursor history -1)]
      (xtypes/read-from-cursor cursor false)))

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

(defn xit-db
  "Returns a new XITDBDatabase which can be used to query and transact data.
  `filename` can be `:memory` or the name of a file on the filesystem.
  If the file does not exist, it will be created.
  The returned database handle can be used from multiple threads.
  Reads can run in parallel, transactions (eg. `swap!`) will only allow one writer at a time."
  [filename]
  (if (= :memory filename)
    (let [memdb  (open-database :memory "rw")
          tdbmem (proxy [ThreadLocal] []
                   (initialValue []
                     memdb))]
      (->XITDBDatabase tdbmem memdb (ReentrantLock.)))

    (let [tldb (proxy [ThreadLocal] []
                 (initialValue []
                   (open-database filename "r")))
          rwdb (open-database filename "rw")]
      (->XITDBDatabase tldb rwdb (ReentrantLock.)))))



(deftype XITDBCursor [xdb keypath]

  java.io.Closeable
  (close [this])

  clojure.lang.IDeref
  (deref [this]
    (let [v (deref xdb)]
      (get-in v keypath)))

  clojure.lang.IAtom

  (reset [this new-value]
    (swap! xdb update-in keypath (constantly new-value)))

  (swap [this f]
    (xitdb-swap-with-lock! xdb keypath  f))

  (swap [this f a]
    (xitdb-swap-with-lock! xdb keypath  f a))

  (swap [this f a1 a2]
    (xitdb-swap-with-lock! xdb keypath  f a1 a2))

  (swap [this f x y args]
    (apply xitdb-swap-with-lock! (concat [xdb keypath f x y] args))))

(defn xdb-cursor [^XITDBDatabase xdb keypath]
  (XITDBCursor. xdb keypath))
