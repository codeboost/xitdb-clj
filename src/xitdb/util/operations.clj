(ns xitdb.util.operations
  (:require
    [xitdb.util.conversion :as conversion]
    [xitdb.util.validation :as validation])
  (:import
    [io.github.radarroark.xitdb Database ReadArrayList ReadCountedHashMap ReadCountedHashSet ReadHashMap ReadHashSet ReadLinkedArrayList Tag WriteArrayList WriteCursor WriteHashMap WriteHashSet WriteLinkedArrayList]))

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
  (let [hash-value (conversion/hash-value (-> whm .cursor .db) k)
        key-cursor (.putKeyCursor whm hash-value)
        cursor (.putCursor whm hash-value)]
    (.writeIfEmpty key-cursor (conversion/v->slot! key-cursor k))
    (.write cursor (conversion/v->slot! cursor v))))

(defn map-dissoc-key!
  "Removes a key-value pair from a WriteHashMap.
  Throws IllegalArgumentException if attempting to remove an internal key.
  Updates the internal count if fast counting is enabled."
  [^WriteHashMap whm k]
  (let [hash-value (conversion/hash-value (-> whm .cursor .db) k)]
    (.remove whm hash-value))
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
  (let [hash-value (conversion/hash-value (-> whm .cursor .db) key)]
    (not (nil? (.getKeyCursor whm hash-value)))))

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
  "Returns the number of key/vals in the map."
  [^ReadHashMap rhm]
  (if (instance? ReadCountedHashMap rhm)
    (.count ^ReadCountedHashMap rhm)
    (map-item-count-iterated rhm)))

(defn map-read-cursor
  "Gets a read cursor for the specified key in a ReadHashMap.
  Returns the cursor if the key exists, nil otherwise."
  [^ReadHashMap rhm key]
  (let [hash-value (conversion/hash-value (-> rhm .cursor .db) key)]
    (.getCursor rhm hash-value)))


(defn map-write-cursor
  "Gets a write cursor for the specified key in a WriteHashMap.
  Creates the key if it doesn't exist."
  [^WriteHashMap whm key]
  (let [hash-value (conversion/hash-value (-> whm .cursor .db) key)]
    (.putCursor whm hash-value)))

;; ============================================================================
;; Set Operations  
;; ============================================================================

(defn set-item-count
  "Returns the number of key/vals in the map."
  [^ReadHashSet rhs]
  (if (instance? ReadCountedHashSet rhs)
    (.count ^ReadCountedHashSet rhs)
    (map-item-count-iterated rhs)))

(defn set-assoc-value!
  "Adds a value to a set (implemented as a WriteHashMap).
  Uses the value's hashCode as the key and the value itself as the value.
  Only adds the value if it doesn't already exist (based on hashCode).
  Returns the modified WriteHashMap."
  [^WriteHashSet whs v]
  (let [hash-code (conversion/hash-value (-> whs .cursor .db) v)
        cursor (.putCursor whs hash-code)]
    (.writeIfEmpty cursor (conversion/v->slot! cursor v))
    whs))

(defn set-disj-value!
  [^WriteHashSet whs v]
  (let [hash-code (conversion/hash-value (-> whs .cursor .db) v)]
    (.remove whs hash-code)
    whs))

(defn set-contains?
  [rhs v]
  (let [hash-code (conversion/hash-value (-> rhs .-cursor .-db) v)
        cursor (.getCursor rhs hash-code)]
    (some? cursor)))

(defn ^WriteHashMap set-empty!
  ""
  [^WriteHashSet whs]
  (let [empty-set (conversion/v->slot! (.cursor whs) #{})]
    (.write ^WriteCursor (.cursor whs) empty-set))
  whs)

;; ============================================================================
;; Sequence Operations
;; ============================================================================

(defn map-seq
  "Return a lazy seq of key-value MapEntry pairs."
  [^ReadHashMap rhm read-from-cursor]
  (let [it (.iterator rhm)]
    (letfn [(step []
              (lazy-seq
                (when (.hasNext it)
                  (let [cursor (.next it)
                        kv     (.readKeyValuePair cursor)
                        k      (read-from-cursor (.-keyCursor kv))]
                    (let [v (read-from-cursor (.-valueCursor kv))]
                      (cons (clojure.lang.MapEntry. k v) (step)))))))]
      (step))))

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
              k      (read-from-cursor (.-keyCursor kv))]
          (let [v (read-from-cursor (.-valueCursor kv))
                new-result (f result k v)]
            (if (reduced? new-result)
              @new-result
              (recur new-result))))

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