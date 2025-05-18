(ns xitdb.xitdb-util
  (:import
    [io.github.radarroark.xitdb Database$Float Database$Bytes Database$Int Database$Uint ReadArrayList ReadCursor ReadHashMap ReadLinkedArrayList Slot WriteArrayList WriteCursor WriteHashMap Tag WriteLinkedArrayList]))

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
   :nil         "nl"
   :inst        "in"
   :date        "da"})

(def true-str "#t")
(def false-str "#f")

;; map of logical key -> key stored in the HashMap
(def internal-keys
  {:count :%xitdb__count
   :is-set? :%xitdb_set})

;; HashMap keys which are used internally and should be hidden from user
(def hidden-keys (set (vals internal-keys)))

(declare ^WriteCursor map->WriteHashMapCursor!)
(declare ^WriteCursor coll->ArrayListCursor!)
(declare ^WriteCursor list->LinkedArrayListCursor!)
(declare ^WriteCursor set->WriteCursor!)

(def ^:dynamic *debug?* false)

(defn lazy-seq? [v]
  (instance? clojure.lang.LazySeq v))

(defn vector-or-chunked? [v]
  (or (vector? v) (chunked-seq? v)))

(defn list-or-cons? [v]
  (or (list? v) (instance? clojure.lang.Cons v)))

(defn ^String keyname [key]
  (if (keyword? key)
    (if (namespace key)
      (str (namespace key) "/" (name key))
      (name key))
    key))


(defn ^Database$Bytes database-bytes
  ([^String s]
   (Database$Bytes. s))
  ([^String s ^String tag]
   (Database$Bytes. s tag)))


(defn ^Slot primitive-for
  "Converts a Clojure primitive value to its corresponding XitDB representation.
  Handles strings, keywords, integers, booleans, and floats.
  Throws an IllegalArgumentException for unsupported types."
  [v]
  (cond

    (lazy-seq? v)
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

    ;;TODO: Confirm that it is correct to return the Read slots
    (instance? ReadHashMap v)
    (-> ^ReadHashMap v .cursor .slot)

    (instance? ReadArrayList v)
    (-> ^ReadArrayList v .cursor .slot)

    (map? v)
    (do
      (.write cursor nil)
      (.slot (map->WriteHashMapCursor! cursor v)))

    (list-or-cons? v)
    (do
      (.write cursor nil)
      (.slot (list->LinkedArrayListCursor! cursor v)))

    (vector-or-chunked? v)
    (do
      (.write cursor nil)
      (.slot (coll->ArrayListCursor! cursor v)))

    (set? v)
    (do
      (.write cursor nil)
      (.slot (set->WriteCursor! cursor v)))

    :else
    (primitive-for v)))

(defn ^WriteArrayList array-list-append-value!
  "Appends a value to a WriteArrayList.
  Converts the value to an appropriate XitDB representation using v->slot!."
  [^WriteArrayList wal v]
  (let [cursor (.appendCursor wal)]
    (.write cursor (v->slot! cursor v))
    wal))

(defn ^WriteArrayList array-list-assoc-value!
  "Associates a value at index i in a WriteArrayList.
  Appends the value if the index equals the current count.
  Replaces the value at the specified index otherwise.
  Throws an IllegalArgumentException if the index is out of bounds."
  [^WriteArrayList wal i v]

  (assert (= Tag/ARRAY_LIST (-> wal .cursor .slot .tag)))
  (assert (number? i))

  (when (> i (.count wal))
    (throw (IllegalArgumentException. "Index out of bounds. ")))

  (let [cursor (if (= i (.count wal))
                 (.appendCursor wal)
                 (.putCursor wal i))]
    (.write cursor (v->slot! cursor v)))
  wal)

(defn array-list-pop! [^WriteArrayList wal]
  (when (zero? (.count wal))
    (throw (IllegalStateException. "Can't pop empty array")))

  (.slice wal (dec (.count wal))))

(defn array-list-empty! [^WriteArrayList wal]
  (let [^WriteCursor cursor (-> wal .cursor)]
    (.write cursor (v->slot! cursor []))))

(defn linked-array-list-append-value!
  "Appends a value to a WriteLinkedArrayList.
  Converts the value to an appropriate XitDB representation using v->slot!."
  [^WriteLinkedArrayList wlal v]
  (let [cursor (.appendCursor wlal)]
    (.write cursor (v->slot! cursor v))
    nil))

(defn linked-array-list-insert-value!
  "Appends a value to a WriteLinkedArrayList.
  Converts the value to an appropriate XitDB representation using v->slot!."
  [^WriteLinkedArrayList wlal pos v]
  (let [cursor (-> wlal .cursor)]
    (.insert wlal pos (v->slot! cursor v)))
  nil)

(defn linked-array-list-pop!
  [^WriteLinkedArrayList wlal]
  (.remove wlal 0)
  nil)

(defn ^Database$Bytes db-key
  "Converts k from a Clojure type to a Database$Bytes representation to be used in
  cursor functions."
  [k]
  (cond
    (integer? k)
    (database-bytes (str k) "ki") ;integer keys are stored as strings with 'ki' format tag
    :else
    (primitive-for k)))

;; Enable storing the count of items in the hashmap under an internal key :count
(def ^:dynamic *enable-map-fast-count?* false)

(defn- update-map-item-count!
  "Update the internal key `:count` by applying `f` to the current value.
  If the key `:count` does not exist, it is created."
  [^WriteHashMap whm f]
  (when *enable-map-fast-count?*
    (let [count-cursor (.putCursor whm (db-key (internal-keys :count)))
          value (try
                  (.readInt count-cursor)
                  (catch Exception _ 0))
          new-value (primitive-for (f (or value 0)))]
      (.write count-cursor new-value))))

(defn- map-item-count-stored
  "Returns the value of the internal key `:count`."
  [^ReadHashMap rhm]
  (let [count-cursor (.getCursor rhm (db-key (internal-keys :count)))]
    (.readInt count-cursor)))

(defn map-assoc-value!
  "Associates a key-value pair in a WriteHashMap.
  Converts the key to a string and the value to an appropriate XitDB representation.
  throws when trying to associate a internal key."
  [^WriteHashMap whm k v]
  (when (contains? hidden-keys k)
    (throw (IllegalArgumentException. (str "Cannot assoc key. " k ". It is reserved for internal use."))))

  (let [cursor (.putCursor whm (db-key k))
        new? (= (-> cursor .slot .tag) Tag/NONE)]
    (.write cursor (v->slot! cursor v))
    (when new?
      (update-map-item-count! whm inc))
    whm))

(defn map-dissoc-key!
  [^WriteHashMap whm k]
  (when (contains? hidden-keys k)
    (throw (IllegalArgumentException. (str "Cannot dissoc key. " k ". It is reserved for internal use."))))

  (when (.remove whm (db-key k))
    (update-map-item-count! whm dec)))

(defn ^WriteHashMap map-empty! [^WriteHashMap whm]
  (let [^WriteCursor cursor (-> whm .cursor)]
    (.write cursor (v->slot! cursor {}))
    whm))

(defn map-contains-key? [^WriteHashMap whm key]
  (not (nil? (.getCursor whm (db-key key)))))

(defn map-item-count-iterated
  "Returns the number of keys in the map by iterating.
  The count includes internal keys if any."
  [^ReadHashMap rhm]
  (let [it (.iterator rhm)]
    (loop [cnt 0]
      (if (.hasNext it)
        (do
          (.next it)
          (recur (inc cnt)))
        cnt))))

(defn map-item-count
  "Returns the number of key/vals in the map."
  [^ReadHashMap rhm]
  (if *enable-map-fast-count?*
    (map-item-count-stored rhm)
    (map-item-count-iterated rhm)))

(defn map-read-cursor [^ReadHashMap rhm key]
  (.getCursor rhm (db-key key)))

(defn map-write-cursor [^WriteHashMap whm key]
  (.putCursor whm (db-key key)))

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

        (list-or-cons? v)
        (let [v-cursor (.appendCursor write-array)]
          (list->LinkedArrayListCursor! v-cursor v))

        (vector-or-chunked? v)
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

        (lazy-seq? v)
        (throw (IllegalArgumentException. "Lazy sequences can be infinite and not allowed !"))

        (list-or-cons? v)
        (let [v-cursor (.appendCursor write-list)]
          (list->LinkedArrayListCursor! v-cursor v))

        (vector-or-chunked? v)
        (let [v-cursor (.appendCursor write-list)]
          (coll->ArrayListCursor! v-cursor v))

        :else
        (.append write-list (primitive-for v))))
    (.-cursor write-list)))

;; ----------

(defn set-assoc-value!
  [^WriteHashMap whm v]
  (let [hash-code (if v (.hashCode v) 0)]
    (let [cursor (.putCursor whm (db-key hash-code))
          new? (= (-> cursor .slot .tag) Tag/NONE)]
      (when new?
        ;; Only write value when the hashCode key doesn't exist
        (.write cursor (v->slot! cursor v))
        (update-map-item-count! whm inc))
      whm)))



(defn ^WriteHashMap mark-as-set! [^WriteHashMap whm]
  (let [is-set-key (db-key (internal-keys :is-set?))]
    (-> whm
        (.putCursor is-set-key)
        (.write (primitive-for 1)))
    whm))

(defn ^WriteHashMap init-hash-set! [^WriteCursor cursor]
  (let [whm (WriteHashMap. cursor)]
    (mark-as-set! whm)
    whm))

(defn ^WriteHashMap set-empty! [^WriteHashMap whm]
  (map-empty! whm)
  (init-hash-set! (.cursor whm))
  whm)

(defn ^WriteCursor set->WriteCursor!
  "Creates a hash-map and associates the internal key :is-set? to 1.
  Map is keyed by the .hashCode of the value, valued by the value :)"
  [^WriteCursor cursor s]
  (let [whm (init-hash-set! cursor)]
    (doseq [v s]
      (set-assoc-value! whm  v))
    (.-cursor whm)))

(defn ^WriteCursor map->WriteHashMapCursor!
  "Writes a Clojure map to a XitDB WriteHashMap.
  Returns the cursor of the created WriteHashMap."
  [^WriteCursor cursor m]
  (let [whm (WriteHashMap. cursor)]
    (doseq [[k v] m]
      (map-assoc-value! whm k v))
    (.-cursor whm)))

(defn read-bytes-with-format-tag [^ReadCursor cursor]
  (let [bytes-obj (.readBytesObject cursor nil)
        str (String. (.value bytes-obj))
        fmt-tag (some-> bytes-obj .formatTag String.)]
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

(defn map-seq
  "Return a lazy seq of key-value MapEntry pairs, skipping hidden keys."
  [^ReadHashMap rhm read-from-cursor]
  (let [it (.iterator rhm)]
    (letfn [(step []
              (lazy-seq
                (when (.hasNext it)
                  (let [cursor (.next it)
                        kv     (.readKeyValuePair cursor)
                        k      (read-bytes-with-format-tag (.-keyCursor kv))]
                    (if (contains? hidden-keys k)
                      (step)
                      (let [v (read-from-cursor (.-valueCursor kv))]
                        (cons (clojure.lang.MapEntry. k v) (step))))))))]
      (step))))

(defn array-seq [^ReadArrayList ral read-from-cursor]
  (let [iter (.iterator ral)
        lazy-iter (fn lazy-iter []
                    (when (.hasNext iter)
                      (let [cursor (.next iter)
                            value (read-from-cursor cursor)]
                        (lazy-seq (cons value (lazy-iter))))))]
    (lazy-iter)))

;;Same as above, but different type hints
(defn linked-array-seq [^ReadLinkedArrayList rlal read-from-cursor]
  (let [iter (.iterator rlal)
        lazy-iter (fn lazy-iter []
                    (when (.hasNext iter)
                      (let [cursor (.next iter)
                            value (read-from-cursor cursor)]
                        (lazy-seq (cons value (lazy-iter))))))]
    (lazy-iter)))


