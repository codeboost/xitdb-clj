(ns xitdb.db
  (:require
    [xitdb.xitdb-types :as xtypes]
    [xitdb.xitdb-util :as util])
  (:import
    [io.github.radarroark.xitdb
     CoreFile CoreMemory Hasher Database Database$ContextFunction
     RandomAccessMemory ReadArrayList WriteArrayList WriteHashMap Tag WriteCursor]
    [java.io File RandomAccessFile]
    [java.security MessageDigest]
    [java.util.concurrent.locks ReentrantLock]))

(defn ^WriteArrayList db-history [^Database db]
  (WriteArrayList. (.rootCursor db)))

(defn append-context [^WriteArrayList history fn]
  (.appendContext
    history
    (.getSlot history -1)
    (reify Database$ContextFunction
      (^void run [_ ^WriteCursor cursor]
        (fn cursor)
        nil)))
  (.count history))

(defn xitdb-reset! [^WriteArrayList history new-value]
  (.appendContext
    history
    nil
    (reify Database$ContextFunction
      (^void run [_ ^WriteCursor cursor]
        (util/v->slot! cursor new-value)
        nil))))

(defn open-database
  [filename ^String open-mode]
  (let [core (if (= filename :memory)
               (CoreMemory. (RandomAccessMemory.))
               (CoreFile. (RandomAccessFile. (File. ^String filename) open-mode)))
        hasher (Hasher. (MessageDigest/getInstance "SHA-1"))]
    (Database. core hasher)))

(defn xitdb-swap!
  "Returns history index."
  [db f & args]
  (let [history (db-history db)]
    (append-context history (fn [^WriteCursor cursor]
                              (let [obj (xtypes/read-from-cursor cursor true)]
                                (let [retval (apply f (concat [obj] args))]
                                  (.write cursor (xtypes/slot-for-value! cursor retval))))))))

(defonce ^:dynamic *return-history?* false)

(defn xitdb-swap-and-call-watch!
  "Performs the 'swap!' operation with locking and calls the watch function (if any)
  with [history-index current-value-of-database (@db)].

  While the watch function runs, the db is guaranteed not to change.
  The watch function *must not* call swap! on the database and should
  move fast, because it's executed under the global db lock."
  [xitdb f & args]
  (let [^ReentrantLock lock (.-lock xitdb)]
    (when (.isHeldByCurrentThread lock)
      ;;TODO: I might change my mind about this...
      (throw (IllegalStateException. "swap! should not be called from the swap! function or watch.")))
    (try
      (.lock lock)
      (let [old-value (when *return-history?* (deref xitdb))
            index (apply xitdb-swap! (into [(-> xitdb .rwdb) f] args))
            new-value (deref xitdb)]
        (if *return-history?*
          [index old-value new-value]
          new-value))
      (finally
        (.unlock lock)))))

(defn- close-db-internal! [^Database db]
  (let [core (-> db .-core)]
    (when (instance? CoreFile core)
      ;;TODO: is this the best way to do it?
      (let [field (.getDeclaredField CoreFile "file")
            _ (.setAccessible field true)
            ^RandomAccessFile file (.get field core)]
        (.close file)))))

(defn read-history [db]
  (ReadArrayList. (-> db .rootCursor)))

(defn history-index [xdb]
  (.count (read-history (-> xdb .tldbro .get))))

(deftype XITDBDatabase [tldbro rwdb watch lock]

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
    (let [history (db-history rwdb)]
      (xitdb-reset! history new-value)
      new-value))

  (swap [this f]
    (xitdb-swap-and-call-watch! this f))

  (swap [this f a]
    (xitdb-swap-and-call-watch! this f a))

  (swap [this f a1 a2]
    (xitdb-swap-and-call-watch! this f a1 a2))

  (swap [this f x y args]
    (apply xitdb-swap-and-call-watch! (concat [this f x y] args))))

(defn xit-db [filename & [watch]]

  (if (= :memory filename)
    (let [memdb (open-database :memory "rw")
          tdbmem (proxy [ThreadLocal] []
                   (initialValue []
                     memdb))]
      (->XITDBDatabase tdbmem memdb watch (ReentrantLock.)))

    (let [tldb (proxy [ThreadLocal] []
                 (initialValue []
                   (open-database filename "r")))
          rwdb (open-database filename "rw")]
      (->XITDBDatabase tldb rwdb watch (ReentrantLock.)))))



