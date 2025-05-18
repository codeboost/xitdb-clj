(ns xitdb.db
  (:require
    [xitdb.xitdb-types :as xtypes]
    [xitdb.xitdb-util :as util])
  (:import
    [io.github.radarroark.xitdb
     CoreFile CoreMemory Hasher Database Database$ContextFunction
     RandomAccessMemory WriteArrayList WriteHashMap Tag WriteCursor]
    [java.io File RandomAccessFile]
    [java.security MessageDigest]))

(defn ^WriteArrayList db-history [^Database db]
  (WriteArrayList. (.rootCursor db)))

(defn append-context [^WriteArrayList history fn]
  (let [history-index (.count history)]
    (.appendContext
      history
      (.getSlot history -1)
      (reify Database$ContextFunction
        (^void run [_ ^WriteCursor cursor]
          (fn cursor)
          nil)))
    history-index))

(defn xitdb-reset! [^WriteArrayList history new-value]
  (.appendContext
    history
    nil
    (reify Database$ContextFunction
      (^void run [_ ^WriteCursor cursor]
        (util/v->slot! cursor new-value)
        nil))))

(defn open-database [filename]
  (let [core (if (= filename :memory)
               (CoreMemory. (RandomAccessMemory.))
               (CoreFile. (RandomAccessFile. (File. ^String filename) "rw")))
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

(defn xitdb-swap-and-call-watch!
  "Performs the 'swap!' operation and calls the watch function (if any)
  with [history-index current-value-of-database (@db)].
  Returns the current value of the db (deref db).
  The watch function *must not* call swap! on the database."
  [xitdb f & args]
  (let [index (apply xitdb-swap! (into [(-> xitdb .db) f] args))
        watch-fn (-> xitdb .watch)
        derefed (deref xitdb)]
    (when watch-fn
      (watch-fn index derefed))
    derefed)

(defn- close-db-internal! [^Database db]
  (let [core (-> db .-core)]
    (when (instance? CoreFile core)
      ;;TODO: is this the best way to do it?
      (let [field (.getDeclaredField CoreFile "file")
            _ (.setAccessible field true)
            ^RandomAccessFile file (.get field core)]
        (.close file)))))

(defprotocol IHistory
  (history [this]))

(defprotocol ICloseDB
  (close-db! [this]))

(deftype XITDBDatabase [db watch]
  ICloseDB
  (close-db! [this]
    (close-db-internal! db))

  IHistory
  (history [this]
    (db-history db))

  clojure.lang.IDeref
  (deref [_]
    (let [history (db-history db)
          cursor (.getCursor history -1)]
      (xtypes/read-from-cursor cursor false)))

  clojure.lang.IAtom
  (reset [this new-value]
    (let [history (db-history db)]
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
  (->XITDBDatabase (open-database filename) watch))



