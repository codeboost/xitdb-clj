(ns xitdb.util.operations
  (:require
    [xitdb.util.conversion :as conversion]
    [xitdb.util.validation :as validation]
    [xitdb.util.schema :as sch])
  (:import
    [io.github.radarroark.xitdb ReadArrayList ReadCountedHashMap ReadCountedHashSet ReadHashMap ReadHashSet ReadLinkedArrayList Tag WriteArrayList WriteCountedHashMap WriteCountedHashSet WriteCursor WriteHashMap WriteHashSet WriteLinkedArrayList]))

;; ============================================================================
;; Array List Operations
;; ============================================================================

(defn ^WriteArrayList array-list-append-value!
  "Appends a value to a WriteArrayList.
  Converts the value to an appropriate XitDB representation using v->slot!."
  [^WriteArrayList wal v]
  (let [cursor (.appendCursor wal)]
    (.write cursor (conversion/v->slot! cursor v))
    wal))

(defn ^WriteArrayList array-list-assoc-value!
  "Associates a value at index i in a WriteArrayList.
  Appends the value if the index equals the current count.
  Replaces the value at the specified index otherwise.
  Throws an IllegalArgumentException if the index is out of bounds."
  [^WriteArrayList wal i v]

  (assert (= Tag/ARRAY_LIST (-> wal .cursor .slot .tag)))
  (assert (number? i))

  (validation/validate-index-bounds i (.count wal) "Array list assoc")

  (let [cursor (if (= i (.count wal))
                 (.appendCursor wal)
                 (.putCursor wal i))]
    (.write cursor (conversion/v->slot! cursor v)))
  wal)

(defn array-list-pop!
  "Removes the last element from a WriteArrayList.
  Throws IllegalStateException if the array is empty.
  Returns the array list with the last element removed."
  [^WriteArrayList wal]
  (validation/validate-non-empty (.count wal) "pop")
  (.slice wal (dec (.count wal))))

(defn array-list-empty!
  "Empties a WriteArrayList by replacing its contents with an empty array.
  Returns the modified WriteArrayList."
  [^WriteArrayList wal]
  (let [^WriteCursor cursor (-> wal .cursor)]
    (.write cursor (conversion/v->slot! cursor []))))

;; ============================================================================
;; Linked Array List Operations
;; ============================================================================

(defn linked-array-list-append-value!
  "Appends a value to a WriteLinkedArrayList.
  Converts the value to an appropriate XitDB representation using v->slot!."
  [^WriteLinkedArrayList wlal v]
  (let [cursor (.appendCursor wlal)]
    (.write cursor (conversion/v->slot! cursor v))
    wlal))

(defn linked-array-list-insert-value!
  "Inserts a value at position pos in a WriteLinkedArrayList.
  Converts the value to an appropriate XitDB representation using v->slot!."
  [^WriteLinkedArrayList wlal pos v]
  (let [cursor (-> wlal .cursor)]
    (.insert wlal pos (conversion/v->slot! cursor v)))
  wlal)

(defn linked-array-list-pop!
  "Removes the first element from a WriteLinkedArrayList.
  This is a stack-like operation (LIFO) for linked lists.
  Returns the modified WriteLinkedArrayList."
  [^WriteLinkedArrayList wlal]
  (.remove wlal 0)
  wlal)

;; ============================================================================
;; Map Operations
;; ============================================================================

;; Dynamic variables for keypath context and display options
(def ^:dynamic *read-keypath* [])
(def ^:dynamic *show-hidden-keys?* false)

(defn- ->vals-array!
  "Associates a key-value pair to a schema-optimized :xdb/values array.
  Returns true if the value was written to the schema array, false otherwise."
  [schema whm k v]
  (when-let [idx (sch/index-of-key-in-schema schema k)]
    (let [keyhash (conversion/db-key-hash (-> whm .cursor .db) :xdb/values)
          acursor (.putCursor whm keyhash)
          avals   (WriteArrayList. acursor)]

      (when (zero? (.count avals))
        (let [key-cursor (.putKeyCursor whm keyhash)
              sch-keys   (sch/schema-keys schema)]
          (doseq [_ sch-keys]
            (.append avals (conversion/primitive-for nil)))
          (.writeIfEmpty key-cursor (conversion/v->slot! key-cursor :xdb/values))
          (.write acursor (conversion/v->slot! acursor avals))))

      (let [value-cur (.putCursor avals idx)]
        (binding [conversion/*current-write-keypath* (conj conversion/*current-write-keypath* k)]
          (.write value-cur (conversion/v->slot! value-cur v))))
      true)))

(defn map-assoc-value!
  "Associates a key-value pair in a WriteHashMap.
  If a schema exists for the current write keypath and the key is in the schema,
  the value will be stored in the :xdb/values array instead of as a regular map entry.
  
  Args:
    whm - The WriteHashMap to modify
    k   - The key to associate (converted to XitDB representation)
    v   - The value to associate (converted to XitDB representation)
  
  Returns the modified WriteHashMap.
  
  Throws IllegalArgumentException if attempting to associate an internal key.
  Updates the internal count if fast counting is enabled."
  [^WriteHashMap whm k v]
  ;; Check if we should use schema-optimized storage
  (let [schema (conversion/schema-for-keypath conversion/*current-write-keypath*)]
    (if (and schema (->vals-array! schema whm k v))
      ;; Value was written to schema array
      whm
      ;; Fall back to standard map association
      (let [key-hash   (conversion/db-key-hash (-> whm .cursor .db) k)
            key-cursor (.putKeyCursor whm key-hash)
            cursor     (.putCursor whm key-hash)]
        (.writeIfEmpty key-cursor (conversion/v->slot! key-cursor k))
        (.write cursor (conversion/v->slot! cursor v))
        whm))))

(defn map-dissoc-key!
  "Removes a key-value pair from a WriteHashMap.
  Throws IllegalArgumentException if attempting to remove an internal key.
  Updates the internal count if fast counting is enabled."
  [^WriteHashMap whm k]
  (let [key-hash (conversion/db-key-hash (-> whm .cursor .db) k)]
    (.remove whm key-hash))
  whm)

(defn ^WriteHashMap map-empty!
  "Empties a WriteHashMap by replacing its contents with an empty map.
  Returns the modified WriteHashMap."
  [^WriteHashMap whm]
  (let [^WriteCursor cursor (-> whm .cursor)]
    (.write cursor (conversion/v->slot! cursor {}))
    whm))

(defn map-contains-key?
  "Checks if a WriteHashMap contains the specified key.
  Returns true if the key exists, false otherwise."
  [^ReadHashMap whm key]
  (let [key-hash (conversion/db-key-hash (-> whm .cursor .db) key)]
    (not (nil? (.getKeyCursor whm key-hash)))))

(defn map-item-count-iterated
  "Returns the number of keys in the map by iterating.
  The count includes internal keys if any."
  [^Iterable rhm]
  (let [it (.iterator rhm)]
    (loop [cnt 0]
      (if (.hasNext it)
        (do
          (.next it)
          (recur (inc cnt)))
        cnt))))

(defn map-item-count
  "Returns the number of key/vals in the map.
  For schema-optimized maps, excludes the hidden :xdb/values key from the count."
  [^ReadHashMap rhm]
  (let [base-count (if (instance? ReadCountedHashMap rhm)
                     (.count ^ReadCountedHashMap rhm)
                     (map-item-count-iterated rhm))]
    (if (and (not *show-hidden-keys?*)
             (map-contains-key? rhm :xdb/values))
      (dec base-count)
      base-count)))

(defn map-read-cursor
  "Gets a read cursor for the specified key in a ReadHashMap.
  Returns the cursor if the key exists, nil otherwise."
  [^ReadHashMap rhm key]
  (let [key-hash (conversion/db-key-hash (-> rhm .cursor .db) key)]
    (.getCursor rhm key-hash)))

;; ============================================================================
;; Set Operations  
;; ============================================================================

(defn set-item-count
  "Returns the number of values in the set."
  [^ReadHashSet rhs]
  (if (instance? ReadCountedHashSet rhs)
    (.count ^ReadCountedHashSet rhs)
    (map-item-count-iterated rhs)))

(defn set-assoc-value!
  "Adds a value to a set."
  [^WriteHashSet whs v]
  (let [hash-code (conversion/db-key-hash (-> whs .cursor .db) v)
        cursor    (.putCursor whs hash-code)]
    (.writeIfEmpty cursor (conversion/v->slot! cursor v))
    whs))

(defn set-disj-value!
  "Removes a value from a set"
  [^WriteHashSet whs v]
  (let [hash-code (conversion/db-key-hash (-> whs .cursor .db) v)]
    (.remove whs hash-code)
    whs))

(defn set-contains?
  "Returns true if `v` is in the set."
  [rhs v]
  (let [hash-code (conversion/db-key-hash (-> rhs .-cursor .-db) v)
        cursor    (.getCursor rhs hash-code)]
    (some? cursor)))

(defn ^WriteHashSet set-empty!
  "Replaces the whs value with an empty set."
  [^WriteHashSet whs]
  (let [empty-set (conversion/v->slot! (.cursor whs) #{})]
    (.write ^WriteCursor (.cursor whs) empty-set))
  whs)

;; ============================================================================
;; Sequence Operations
;; ============================================================================

(defn- schema-vals-array
  "Determines the schema for current keypath and if one exists,
  looks up the `:xdb/value` key in the map.
  If the `:xdb/values` value exists, returns a vector of [schema-for-keypath vals-array-cursor].
  `vals-array-cursor` is a cursor on the `:xdb/vals` array list. "
  [rhm]
  (let [current-path  *read-keypath*
        schema        (conversion/schema-for-keypath current-path)
        has-values?   (and schema
                           (not *show-hidden-keys?*)
                           (map-contains-key? rhm :xdb/values))
        values-cursor (when has-values?
                        (map-read-cursor rhm :xdb/values))]
    (when has-values?
      [schema values-cursor])))

(defn- schema-entries-seq
  "Returns a lazy sequence of map entries reconstructed from schema and :xdb/values cursor.
  Only reads individual values from the array as entries are consumed.
  If there's no schema for *read-keypath* or no `:xdb/values` value, returns nil."
  [rhm read-from-cursor]
  (when-let [[schema values-cursor] (schema-vals-array rhm)]
    (let [path *read-keypath*
          schema-keys (sch/schema-keys schema)
          ^ReadArrayList ral (ReadArrayList. values-cursor)]
      (letfn [(step [keys idx]
                (lazy-seq
                  (when (seq keys)
                    (let [key      (first keys)
                          new-path (conj path key)]
                      (binding [*read-keypath* new-path]
                        (let [value-cursor (.getCursor ral idx)
                              value        (read-from-cursor value-cursor)]
                          (cons (clojure.lang.MapEntry. key value)
                                (step (rest keys) (inc idx)))))))))]
        (step schema-keys 0)))))


(defn- should-hide-key?
  "Returns true if the key should be hidden from map iteration.
  Internal :xdb/ keys are hidden unless *show-hidden-keys?* is true."
  [k]
  (and (keyword? k)
       (.startsWith (str k) ":xdb/")
       (not *show-hidden-keys?*)))

(defn- map-iterator-seq
  "Returns a lazy sequence of map entries from an iterator, 
  filtering out internal :xdb/ keys.
  
  The `path` parameter represents the current keypath context (e.g. [:users 123]).
  It is used for maintaining proper *read-keypath* bindings during traversal."
  [it read-from-cursor]
  (letfn [(step [current-path]
            (lazy-seq
              (when (.hasNext it)
                (let [cursor (.next it)
                      kv     (.readKeyValuePair cursor)
                      k      (read-from-cursor (.-keyCursor kv))]
                  (if (should-hide-key? k)
                    (step current-path)
                    (let [new-path (conj current-path k)]
                      (binding [*read-keypath* new-path]
                        (let [v (read-from-cursor (.-valueCursor kv))]
                          (cons (clojure.lang.MapEntry. k v) (step new-path))))))))))]
    (step *read-keypath*)))

(defn- map-read-value
  "Reads the value of `key` from the map."
  [rhm key not-found read-from-cursor]
  (let [cursor (map-read-cursor rhm key)]
    (if (nil? cursor)
      not-found
      (binding [*read-keypath* (conj *read-keypath* key)]
        (read-from-cursor cursor)))))

(defn- map-read-schema-value
  "If schema exists for current keypath, reads the key value
  from the `:xdb/value` array at the index corresponding to the key index in schema.
  Returns a vector of [true value] if the value was read from the `:xdb/values` array.
  Returns nil if there's no matching schema, key not found in schema, or no `:xdb/values` array
  exists in the map."
  [rhm key read-from-cursor]
  (let [schema      (conversion/schema-for-keypath *read-keypath*)
        idx         (when schema (sch/index-of-key-in-schema schema key))
        vals-cursor (when idx (map-read-cursor rhm :xdb/values))]
    (when vals-cursor
      (let [^ReadArrayList ral (ReadArrayList. vals-cursor)]
        (binding [*read-keypath* (conj *read-keypath* key)]
          [true (read-from-cursor (.getCursor ral idx))])))))

(defn map-val-at
  "Looks up a key in a ReadHashMap, handling schema-optimized maps.
  Returns the value for the key, or not-found if the key doesn't exist."
  [^ReadHashMap rhm key not-found read-from-cursor]
  (let [[read? val] (map-read-schema-value rhm key read-from-cursor)]
    (if read?
      val
      (map-read-value rhm key not-found read-from-cursor))))

(defn map-contains-key-schema-aware?
  "Checks if a ReadHashMap contains a key, handling schema-optimized maps.
  Returns true if the key exists either in the schema or as a regular entry."
  [^ReadHashMap rhm key]
  ;; Check if this is a schema-optimized map and the key is in the schema
  (let [current-path *read-keypath*
        schema       (conversion/schema-for-keypath current-path)]
    (if (and schema 
             (not *show-hidden-keys?*)
             (map-contains-key? rhm :xdb/values))
      ;; Schema-optimized: check if key is in schema or exists as regular entry
      (or (some? (sch/index-of-key-in-schema schema key))
          (map-contains-key? rhm key))
      ;; Standard containsKey
      (map-contains-key? rhm key))))

(defn map-seq
  "Return a lazy seq of key-value MapEntry pairs.
  If the map contains :xdb/values and there's a schema for the current keypath,
  first iterates through `:xdb/values`, reconstructing the MapEntry's from schema,
  then iterates through the rest of the keys in the map.
  If `*show-hidden-keys?*` is true, will iterate through the map returning all keys, including
  hidden keys."
  [^ReadHashMap rhm read-from-cursor]
  (concat
    (when-not *show-hidden-keys?*
      (schema-entries-seq rhm read-from-cursor))
    (map-iterator-seq (.iterator rhm) read-from-cursor)))


(defn set-seq
  "Return a lazy seq values from the set."
  [rhm read-from-cursor]
  (let [it (.iterator rhm)]
    (letfn [(step []
              (lazy-seq
                (when (.hasNext it)
                  (let [cursor (.next it)
                        kv     (.readKeyValuePair cursor)
                        v      (read-from-cursor (.-keyCursor kv))]
                    (cons v (step))))))]
      (step))))

(defn array-seq
  "Creates a lazy sequence from a ReadArrayList.
  Uses the provided read-from-cursor function to convert cursors to values.
  Returns a lazy sequence of the array elements."
  [^ReadArrayList ral read-from-cursor]
  (let [iter      (.iterator ral)
        lazy-iter (fn lazy-iter []
                    (when (.hasNext iter)
                      (let [cursor (.next iter)
                            value  (read-from-cursor cursor)]
                        (lazy-seq (cons value (lazy-iter))))))]
    (lazy-iter)))

(defn linked-array-seq
  "Creates a lazy sequence from a ReadLinkedArrayList.
  Uses the provided read-from-cursor function to convert cursors to values.
  Returns a lazy sequence of the linked array elements."
  [^ReadLinkedArrayList rlal read-from-cursor]
  (let [iter      (.iterator rlal)
        lazy-iter (fn lazy-iter []
                    (when (.hasNext iter)
                      (let [cursor (.next iter)
                            value  (read-from-cursor cursor)]
                        (lazy-seq (cons value (lazy-iter))))))]
    (lazy-iter)))

(defn map-kv-reduce
  "Efficiently reduces over key-value pairs in a ReadHashMap, skipping hidden keys."
  [^ReadHashMap rhm read-from-cursor f init]
  (let [it (.iterator rhm)]
    (loop [result init]
      (if (.hasNext it)
        (let [cursor     (.next it)
              kv         (.readKeyValuePair cursor)
              k          (read-from-cursor (.-keyCursor kv))
              v          (read-from-cursor (.-valueCursor kv))
              new-result (f result k v)]
          (if (reduced? new-result)
            @new-result
            (recur new-result)))
        result))))

(defn array-kv-reduce
  "Efficiently reduces over index-value pairs in a ReadArrayList."
  [^ReadArrayList ral read-from-cursor f init]
  (let [count (.count ral)]
    (loop [i      0
           result init]
      (if (< i count)
        (let [cursor     (.getCursor ral i)
              v          (read-from-cursor cursor)
              new-result (f result i v)]
          (if (reduced? new-result)
            @new-result
            (recur (inc i) new-result)))
        result))))
