(ns xitdb.util.db-registry
  "Identity of engine handles.

  An XITDBDatabase owns two engine `Database` handles, a reader and a writer.
  They are registered here under a shared token so a value read through the
  reader is recognised as belonging to the same database when it is written
  back through the writer, and a value from any other database is not."
  (:import
    [java.util Collections Map WeakHashMap]))

(defonce ^:private database-registry
         (Collections/synchronizedMap (WeakHashMap.)))

(defn register-database!
  "Registers engine handle `db` as belonging to the database identified by
  `token`, optionally with its reader handle. Returns `db`."
  ([db token]
   (register-database! db token nil))
  ([db token reader]
   (.put ^Map database-registry db {:token token :reader reader})
   db))

(defn database-token
  "Identity of the database `db` belongs to: its registered token, or `db`
  itself when it was never registered (a bare handle from `open-database`)."
  [db]
  (or (:token (.get ^Map database-registry db)) db))

(defn same-database?
  "True when engine handles `db-a` and `db-b` belong to the same database."
  [db-a db-b]
  (identical? (database-token db-a) (database-token db-b)))

(defn reader-database
  "Reader handle for `db`, or `db` itself for an unregistered handle."
  [db]
  (or (:reader (.get ^Map database-registry db)) db))
