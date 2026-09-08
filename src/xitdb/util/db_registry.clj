(ns xitdb.util.db-registry
  "Identity of engine handles.

  An XITDBDatabase owns several engine `Database` handles: one writer plus one
  reader per thread. They are registered here under a shared token so a value
  read through any of them is recognised as belonging to the same database
  when it is written back, and a value from any other database is not."
  (:import
    [java.util Collections Map WeakHashMap]))

(defonce ^:private database-registry
  (Collections/synchronizedMap (WeakHashMap.)))

(defn register-database!
  "Registers engine handle `db` as belonging to the database identified by
  `token`. Returns `db`."
  [db token]
  (.put ^Map database-registry db token)
  db)

(defn database-token
  "Identity of the database `db` belongs to: its registered token, or `db`
  itself when it was never registered (a bare handle from `open-database`)."
  [db]
  (or (.get ^Map database-registry db) db))

(defn same-database?
  "True when engine handles `db-a` and `db-b` belong to the same database."
  [db-a db-b]
  (identical? (database-token db-a) (database-token db-b)))
