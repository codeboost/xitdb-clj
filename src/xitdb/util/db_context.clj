(ns xitdb.util.db-context
  "Reader and writer handles access the same storage but have different identities.
  Linking them lets the wrapper reuse stored pointers within one database, reject
  pointers from other databases, and route frozen values through the reader so
  they can be shared across threads without using the writer's mutable file state."
  (:import
    [io.github.radarroark.xitdb Database]))

(definterface DatabaseOwner
  (readerDatabase []))

(defn create
  "Creates a reader and a writer that refers directly to it."
  [reader-core writer-core hasher]
  (let [reader (Database. reader-core hasher)
        writer (proxy [Database DatabaseOwner] [writer-core hasher]
                 (readerDatabase [] reader))]
    {:reader reader :writer writer}))

(defn reader-database
  "Reader associated with a writer, or the handle itself."
  [db]
  (if (instance? DatabaseOwner db)
    (.readerDatabase ^DatabaseOwner db)
    db))

(defn same-database?
  "True when handles share a reader, or are the same bare Java handle."
  [db-a db-b]
  (identical? (reader-database db-a) (reader-database db-b)))
