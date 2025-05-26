(ns xitdb.util.operations
  (:require
    [xitdb.util.conversion :as conversion]
    [xitdb.util.validation :as validation])
  (:import
    [io.github.radarroark.xitdb ReadArrayList ReadCountedHashMap ReadHashMap ReadLinkedArrayList Tag WriteArrayList WriteCursor WriteHashMap WriteLinkedArrayList]))

(def internal-keys
  "Map of logical internal key names to their actual storage keys in XitDB.
  These keys are used internally by the system and should not be exposed to users."
  {:count :%xitdb__count
   :is-set? :%xitdb_set})

(def hidden-keys
  "Set of keys that are used internally and should be hidden from user operations.
  Operations like seq, reduce, and count will skip these keys."
  (set (vals internal-keys)))

(def ^:dynamic *enable-map-fast-count?*
  "When true, maps store their item count in an internal key for O(1) count operations.
  When false, count operations require iteration over all entries (O(n)).
  Default is false to minimize storage overhead."
  false)

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
    nil))

(defn linked-array-list-insert-value!
  "Inserts a value at position pos in a WriteLinkedArrayList.
  Converts the value to an appropriate XitDB representation using v->slot!."
  [^WriteLinkedArrayList wlal pos v]
  (let [cursor (-> wlal .cursor)]
    (.insert wlal pos (conversion/v->slot! cursor v)))
  nil)

(defn linked-array-list-pop!
  "Removes the first element from a WriteLinkedArrayList.
  This is a stack-like operation (LIFO) for linked lists.
  Returns nil."
  [^WriteLinkedArrayList wlal]
  (.remove wlal 0)
  nil)

;; ============================================================================
;; Map Operations
;; ============================================================================

(defn map-assoc-value!
  "Associates a key-value pair in a WriteHashMap.
  
  Args:
    whm - The WriteHashMap to modify
    k   - The key to associate (converted to XitDB representation)
    v   - The value to associate (converted to XitDB representation)
  
  Returns the modified WriteHashMap.
  
  Throws IllegalArgumentException if attempting to associate an internal key.
  Updates the internal count if fast counting is enabled."
  [^WriteHashMap whm k v]
  (when (contains? hidden-keys k)
    (throw (IllegalArgumentException. (str "Cannot assoc key. " k ". It is reserved for internal use."))))

  (let [cursor (.putCursor whm (conversion/db-key k))]
    (.write cursor (conversion/v->slot! cursor v))
    whm))

(defn map-dissoc-key!
  "Removes a key-value pair from a WriteHashMap.
  Throws IllegalArgumentException if attempting to remove an internal key.
  Updates the internal count if fast counting is enabled."
  [^WriteHashMap whm k]
  (when (contains? hidden-keys k)
    (throw (IllegalArgumentException. (str "Cannot dissoc key. " k ". It is reserved for internal use."))))

  (.remove whm (conversion/db-key k))
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
  [^WriteHashMap whm key]
  (not (nil? (.getCursor whm (conversion/db-key key)))))

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
  (if (instance? ReadCountedHashMap rhm)
    (.count ^ReadCountedHashMap rhm)
    (map-item-count-iterated rhm)))

(defn map-read-cursor
  "Gets a read cursor for the specified key in a ReadHashMap.
  Returns the cursor if the key exists, nil otherwise."
  [^ReadHashMap rhm key]
  (.getCursor rhm (conversion/db-key key)))

(defn map-write-cursor
  "Gets a write cursor for the specified key in a WriteHashMap.
  Creates the key if it doesn't exist."
  [^WriteHashMap whm key]
  (.putCursor whm (conversion/db-key key)))

;; ============================================================================
;; Set Operations  
;; ============================================================================

(defn set-assoc-value!
  "Adds a value to a set (implemented as a WriteHashMap).
  Uses the value's hashCode as the key and the value itself as the value.
  Only adds the value if it doesn't already exist (based on hashCode).
  Returns the modified WriteHashMap."
  [^WriteHashMap whm v]
  (let [hash-code (if v (.hashCode v) 0)]
    (let [cursor (.putCursor whm (conversion/db-key hash-code))
          new? (= (-> cursor .slot .tag) Tag/NONE)]
      (when new?
        ;; Only write value when the hashCode key doesn't exist
        (.write cursor (conversion/v->slot! cursor v)))
      whm)))

(defn ^WriteHashMap mark-as-set!
  "Marks a WriteHashMap as being a set by adding an internal marker.
  This allows the system to distinguish between maps and sets.
  Returns the modified WriteHashMap."
  [^WriteHashMap whm]
  (let [is-set-key (conversion/db-key (internal-keys :is-set?))]
    (-> whm
        (.putCursor is-set-key)
        (.write (conversion/primitive-for 1)))
    whm))

(defn ^WriteHashMap init-hash-set!
  "Initializes a new WriteHashMap as a set.
  Creates a WriteHashMap and marks it as a set using the internal marker.
  Returns the newly created WriteHashMap configured as a set."
  [^WriteCursor cursor]
  (let [whm (WriteHashMap. cursor)]
    (mark-as-set! whm)
    whm))

(defn ^WriteHashMap set-empty!
  "Empties a set (WriteHashMap) and re-initializes it as an empty set.
  Clears all values and re-adds the internal set marker.
  Returns the emptied and re-initialized WriteHashMap."
  [^WriteHashMap whm]
  (map-empty! whm)
  (init-hash-set! (.cursor whm))
  whm)

;; ============================================================================
;; Sequence Operations
;; ============================================================================

(defn map-seq
  "Return a lazy seq of key-value MapEntry pairs, skipping hidden keys."
  [^ReadCountedHashMap rhm read-from-cursor]
  (let [it (.iterator rhm)]
    (letfn [(step []
              (lazy-seq
                (when (.hasNext it)
                  (let [cursor (.next it)
                        kv     (.readKeyValuePair cursor)
                        k      (conversion/read-bytes-with-format-tag (.-keyCursor kv))]
                    (if (contains? hidden-keys k)
                      (step)
                      (let [v (read-from-cursor (.-valueCursor kv))]
                        (cons (clojure.lang.MapEntry. k v) (step))))))))]
      (step))))

(defn array-seq
  "Creates a lazy sequence from a ReadArrayList.
  Uses the provided read-from-cursor function to convert cursors to values.
  Returns a lazy sequence of the array elements."
  [^ReadArrayList ral read-from-cursor]
  (let [iter (.iterator ral)
        lazy-iter (fn lazy-iter []
                    (when (.hasNext iter)
                      (let [cursor (.next iter)
                            value (read-from-cursor cursor)]
                        (lazy-seq (cons value (lazy-iter))))))]
    (lazy-iter)))

(defn linked-array-seq
  "Creates a lazy sequence from a ReadLinkedArrayList.
  Uses the provided read-from-cursor function to convert cursors to values.
  Returns a lazy sequence of the linked array elements."
  [^ReadLinkedArrayList rlal read-from-cursor]
  (let [iter (.iterator rlal)
        lazy-iter (fn lazy-iter []
                    (when (.hasNext iter)
                      (let [cursor (.next iter)
                            value (read-from-cursor cursor)]
                        (lazy-seq (cons value (lazy-iter))))))]
    (lazy-iter)))

(defn map-kv-reduce
  "Efficiently reduces over key-value pairs in a ReadHashMap, skipping hidden keys."
  [^ReadHashMap rhm read-from-cursor f init]
  (let [it (.iterator rhm)]
    (loop [result init]
      (if (.hasNext it)
        (let [cursor (.next it)
              kv     (.readKeyValuePair cursor)
              k      (conversion/read-bytes-with-format-tag (.-keyCursor kv))]
          (if (contains? hidden-keys k)
            (recur result)
            (let [v (read-from-cursor (.-valueCursor kv))
                  new-result (f result k v)]
              (if (reduced? new-result)
                @new-result
                (recur new-result)))))
        result))))

(defn array-kv-reduce
  "Efficiently reduces over index-value pairs in a ReadArrayList."
  [^ReadArrayList ral read-from-cursor f init]
  (let [count (.count ral)]
    (loop [i 0
           result init]
      (if (< i count)
        (let [cursor (.getCursor ral i)
              v (read-from-cursor cursor)
              new-result (f result i v)]
          (if (reduced? new-result)
            @new-result
            (recur (inc i) new-result)))
        result))))