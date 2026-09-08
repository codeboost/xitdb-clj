(ns xitdb.snapshot
  (:require [xitdb.db :as xdb])
  (:import [java.io File]))

(defn xit-db-existing [filename]
  (when-not (or (= filename :memory) (.exists (File. ^String filename)))
    (throw (IllegalArgumentException. "Database file does not exist")))
  (xdb/xit-db filename))

(defn snapshot-memory-db
  "Returns a memory database with the value of `keypath` in the database at `filename`
  When keypath is [], returns a memdb with all the data in the db `filename`.
  Useful for REPL-based investigation and testing."
  [filename keypath]
  (with-open [db (xit-db-existing filename)]
    (let [memdb (xdb/xit-db :memory)]
      ;; The value read from `db` is a pointer into that file; it has to be
      ;; copied (materialized) before it can be written into another database.
      (reset! memdb (xdb/materialize (get-in @db keypath)))
      memdb)))