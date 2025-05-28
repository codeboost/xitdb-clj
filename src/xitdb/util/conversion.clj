(ns xitdb.util.conversion
  (:require
    [xitdb.util.validation :as validation])
  (:import
    [io.github.radarroark.xitdb
     Database Database$Bytes Database$Float Database$Int
     ReadArrayList ReadCountedHashSet ReadCursor ReadHashMap ReadCountedHashMap
     ReadHashSet Slot Tag WriteArrayList WriteCountedHashSet WriteCursor WriteCountedHashMap
     WriteHashMap WriteHashSet WriteLinkedArrayList]
    [java.nio ByteBuffer]))

(defn xit-tag->keyword
  "Converts a XitDB Tag enum to a corresponding Clojure keyword."
  [tag]
  (cond
    (= tag Tag/NONE) :none
    (= tag Tag/INDEX) :index
    (= tag Tag/ARRAY_LIST) :array-list
    (= tag Tag/LINKED_ARRAY_LIST) :linked-array-list
    (= tag Tag/HASH_MAP) :hash-map
    (= tag Tag/KV_PAIR) :kv-pair
    (= tag Tag/BYTES) :bytes
    (= tag Tag/SHORT_BYTES) :short-bytes
    (= tag Tag/UINT) :uint
    (= tag Tag/INT) :int
    (= tag Tag/FLOAT) :float
    :else :unknown))

;; map of logical tag -> string used as formatTag in the Bytes record.
(def fmt-tag-value
  {:keyword     "kw"
   :boolean     "bl"
   :key-integer "ki"
   :nil         "nl"                                        ;; TODO: Could use Tag/NONE instead
   :inst        "in"
   :date        "da"})

(def true-str "#t")
(def false-str "#f")

(defn ^Database$Bytes database-bytes
  ([^String s]
   (Database$Bytes. s))
  ([^String s ^String tag]
   (Database$Bytes. s tag)))

(defn ^String keyname [key]
  (if (keyword? key)
    (if (namespace key)
      (str (namespace key) "/" (name key))
      (name key))
    key))

(defn db-key-hash
  "Returns a byte array representing the stable hash digest of (Clojure) value `v`.
  Uses the MessageDigest from the database."
  ^bytes [^Database jdb v]
  (if (nil? v)
    (byte-array (-> jdb .md .getDigestLength))
    (do
      ;; add type name
      (.update (.md jdb) (-> v .getClass .getCanonicalName (.getBytes "UTF-8")))
      ;; add null byte as separator
      (.update (.md jdb) (byte-array 1))
      ;; add the value
      (cond
        (validation/lazy-seq? v)
        (throw (IllegalArgumentException. "Lazy sequences can be infinite and not allowed!"))

        (bytes? v)
        (.update (.md jdb) v)

        (instance? Database$Bytes v)
        (.update (.md jdb) (.value v))

        (coll? v)
        (with-open [os (java.security.DigestOutputStream. (java.io.OutputStream/nullOutputStream) (.md jdb))]
          (with-open [writer (java.io.OutputStreamWriter. os)]
            (binding [*out* writer]
              (pr v))))

        :else
        (.update (.md jdb) (.getBytes (str v) "UTF-8")))
      ;; finish hash
      (.digest (.md jdb)))))

(defn ^Slot primitive-for
  "Converts a Clojure primitive value to its corresponding XitDB representation.
  Handles strings, keywords, integers, booleans, and floats.
  Throws an IllegalArgumentException for unsupported types."
  [v]
  (cond

    (validation/lazy-seq? v)
    (throw (IllegalArgumentException. "Lazy sequences can be infinite and not allowed!"))

    (string? v)
    (database-bytes v)

    (keyword? v)
    (database-bytes (keyname v) (fmt-tag-value :keyword))

    (integer? v)
    (Database$Int. v)

    (boolean? v)
    (database-bytes (if v true-str false-str) (fmt-tag-value :boolean))

    (double? v)
    (Database$Float. v)

    (nil? v)
    (database-bytes "" (fmt-tag-value :nil))

    (instance? java.time.Instant v)
    (database-bytes (str v) (fmt-tag-value :inst))

    (instance? java.util.Date v)
    (database-bytes (str (.toInstant ^java.util.Date v)) (fmt-tag-value :date))

    :else
    (throw (IllegalArgumentException. (str "Unsupported type: " (type v) v)))))

(declare ^WriteCursor map->WriteHashMapCursor!)
(declare ^WriteCursor coll->ArrayListCursor!)
(declare ^WriteCursor list->LinkedArrayListCursor!)
(declare ^WriteCursor set->WriteCursor!)

(defn ^Slot v->slot!
  "Converts a value to a XitDB slot.
  Handles WriteArrayList and WriteHashMap instances directly.
  Recursively processes Clojure maps and collections.
  Falls back to primitive conversion for other types."
  [^WriteCursor cursor v]
  (cond

    (instance? WriteArrayList v)
    (-> ^WriteArrayList v .cursor .slot)

    (instance? WriteLinkedArrayList v)
    (-> ^WriteLinkedArrayList v .cursor .slot)

    (instance? WriteHashMap v)
    (-> ^WriteHashMap v .cursor .slot)

    (instance? ReadHashMap v)
    (-> ^ReadHashMap v .cursor .slot)

    (instance? ReadCountedHashMap v)
    (-> ^ReadCountedHashMap v .cursor .slot)

    (instance? WriteCountedHashMap v)
    (-> ^WriteCountedHashMap v .cursor .slot)

    (instance? ReadArrayList v)
    (-> ^ReadArrayList v .cursor .slot)

    (instance? ReadHashSet v)
    (-> ^ReadHashSet v .cursor .slot)

    (instance? ReadCountedHashSet v)
    (-> ^ReadCountedHashSet v .cursor .slot)

    (instance? WriteHashSet v)
    (-> ^WriteHashSet v .cursor .slot)

    (instance? WriteCountedHashSet v)
    (-> ^WriteCountedHashSet v .cursor .slot)

    (map? v)
    (do
      (.write cursor nil)
      (.slot (map->WriteHashMapCursor! cursor v)))

    (validation/list-or-cons? v)
    (do
      (.write cursor nil)
      (.slot (list->LinkedArrayListCursor! cursor v)))

    (validation/vector-or-chunked? v)
    (do
      (.write cursor nil)
      (.slot (coll->ArrayListCursor! cursor v)))

    (set? v)
    (do
      (.write cursor nil)
      (.slot (set->WriteCursor! cursor v)))

    :else
    (primitive-for v)))

(defn ^Database$Bytes db-key
  "Converts k from a Clojure type to a Database$Bytes representation to be used in
  cursor functions."
  [k]
  (cond
    (integer? k)
    (database-bytes (str k) "ki")                           ;integer keys are stored as strings with 'ki' format tag
    :else
    (primitive-for k)))

(defn read-bytes-with-format-tag [^ReadCursor cursor]
  (let [bytes-obj (.readBytesObject cursor nil)
        str       (String. (.value bytes-obj))
        fmt-tag   (some-> bytes-obj .formatTag String.)]
    (cond

      (= fmt-tag (fmt-tag-value :keyword))
      (keyword str)

      (= fmt-tag (fmt-tag-value :boolean))
      (= str true-str)

      (= fmt-tag (fmt-tag-value :key-integer))
      (Integer/parseInt str)

      (= fmt-tag (fmt-tag-value :inst))
      (java.time.Instant/parse str)

      (= fmt-tag (fmt-tag-value :date))
      (java.util.Date/from
        (java.time.Instant/parse str))

      (= fmt-tag (fmt-tag-value :nil))
      nil

      :else
      str)))

(def ^:dynamic *debug?* false)

(defn ^WriteCursor coll->ArrayListCursor!
  "Converts a Clojure collection to a XitDB ArrayList cursor.
  Handles nested maps and collections recursively.
  Returns the cursor of the created WriteArrayList."
  [^WriteCursor cursor coll]
  (when *debug?* (println "Write array" (type coll)))
  (let [write-array (WriteArrayList. cursor)]
    (doseq [v coll]
      (cond
        (map? v)
        (let [v-cursor (.appendCursor write-array)]
          (map->WriteHashMapCursor! v-cursor v))

        (validation/list-or-cons? v)
        (let [v-cursor (.appendCursor write-array)]
          (list->LinkedArrayListCursor! v-cursor v))

        (validation/vector-or-chunked? v)
        (let [v-cursor (.appendCursor write-array)]
          (coll->ArrayListCursor! v-cursor v))

        :else
        (.append write-array (primitive-for v))))
    (.-cursor write-array)))

(defn ^WriteCursor list->LinkedArrayListCursor!
  "Converts a Clojure list or seq-like collection to a XitDB LinkedArrayList cursor.
   Optimized for sequential access collections rather than random access ones."
  [^WriteCursor cursor coll]
  (when *debug?* (println "Write list" (type coll)))
  (let [write-list (WriteLinkedArrayList. cursor)]
    (doseq [v coll]
      (when *debug?* (println "v=" v))
      (cond
        (map? v)
        (let [v-cursor (.appendCursor write-list)]
          (map->WriteHashMapCursor! v-cursor v))

        (validation/lazy-seq? v)
        (throw (IllegalArgumentException. "Lazy sequences can be infinite and not allowed !"))

        (validation/list-or-cons? v)
        (let [v-cursor (.appendCursor write-list)]
          (list->LinkedArrayListCursor! v-cursor v))

        (validation/vector-or-chunked? v)
        (let [v-cursor (.appendCursor write-list)]
          (coll->ArrayListCursor! v-cursor v))

        :else
        (.append write-list (primitive-for v))))
    (.-cursor write-list)))

(defn ^WriteCursor map->WriteHashMapCursor!
  "Writes a Clojure map to a XitDB WriteHashMap.
  Returns the cursor of the created WriteHashMap."
  [^WriteCursor cursor m]
  (let [whm (WriteCountedHashMap. cursor)]
    (doseq [[k v] m]
      (let [hash-value (db-key-hash (-> cursor .db) k)
            key-cursor (.putKeyCursor whm hash-value)
            cursor     (.putCursor whm hash-value)]
        (.writeIfEmpty key-cursor (v->slot! key-cursor k))
        (.write cursor (v->slot! cursor v))))
    (.-cursor whm)))

(defn ^WriteCursor set->WriteCursor!
  "Writes a Clojure set `s` to a XitDB WriteHashSet.
  Returns the cursor of the created WriteHashSet."
  [^WriteCursor cursor s]
  (let [whm (WriteCountedHashSet. cursor)
        db  (-> cursor .db)]
    (doseq [v s]
      (let [hash-code (db-key-hash db v)
            cursor    (.putCursor whm hash-code)]
        (.writeIfEmpty cursor (v->slot! cursor v))))
    (.-cursor whm)))