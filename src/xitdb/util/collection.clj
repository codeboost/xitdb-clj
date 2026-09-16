(ns xitdb.util.collection
  "Collection equality and hashing shared by database views. Work streams over
  the view; no materialization or mutable-view hash caching is needed."
  (:import [clojure.lang Associative Indexed Murmur3 SeqIterator Util]
           [java.util AbstractCollection AbstractList AbstractSet Collections Map$Entry]))

(defn index-in-bounds?
  "True when `i` is an integer index inside a collection of `size` elements.
  Anything else, including floats, nil and keywords, is out of bounds, so a
  caller may coerce `i` to int once this returns true."
  [i size]
  (and (integer? i) (>= i 0) (< i size)))

(defn index-out-of-bounds!
  "Throws the IndexOutOfBoundsException shared by the Clojure nth/invoke and
  Java List.get methods of the sequence views."
  [i size]
  (throw (IndexOutOfBoundsException. (str "Index: " i ", Size: " size))))

(defn indexed-nth
  "nth without a default for sequence views: the element at an in-bounds index,
  otherwise IndexOutOfBoundsException. Also serves Java List.get, which needs
  the exception for negative indices too. Reads with a nil default because an
  in-range NONE slot represents a stored nil and may have no read cursor."
  [^Indexed coll i]
  (if (index-in-bounds? i (count coll))
    (.nth coll (int i) nil)
    (index-out-of-bounds! i (count coll))))

(defn indexed-lookup
  "ILookup valAt for index-keyed views: the element for an in-bounds integer
  key, which may be a stored nil, and `not-found` for every other key."
  [^Indexed coll k not-found]
  (if (index-in-bounds? k (count coll))
    (.nth coll (int k) not-found)
    not-found))

(defn indexed-invoke
  "Single-argument invocation of a sequence view, matching Clojure vectors:
  a non-integer key throws IllegalArgumentException and an out-of-bounds
  integer throws IndexOutOfBoundsException."
  [coll k]
  (when-not (integer? k)
    (throw (IllegalArgumentException. "Key must be integer")))
  (indexed-nth coll k))

(defn list-view
  "Live, read-only List adapter for shared Java methods on sequence wrappers.
  AbstractList supplies bounds-checked sublists, bidirectional iterators and
  typed arrays. Forward iteration streams the wrapper's seq without copying."
  ^java.util.List [coll]
  (Collections/unmodifiableList
    (proxy [AbstractList] []
      (size [] (count coll))
      (get [i] (indexed-nth coll i))
      (iterator [] (SeqIterator. (seq coll))))))

(defn sequence-equal?
  "Compares `a` element by element with sequential or java.util.List `b`.
  Uses Clojure value equivalence when `equiv?` is true, Java equality otherwise.
  Stops at the first mismatch or unequal length without materializing either sequence."
  [a b equiv?]
  (and (or (sequential? b) (instance? java.util.List b))
       (loop [xs (seq a), ys (seq b)]
         (cond
           (nil? xs) (nil? ys)
           (nil? ys) false
           (if equiv?
             (Util/equiv (first xs) (first ys))
             (Util/equals (first xs) (first ys))) (recur (next xs) (next ys))
           :else false))))

(defn ordered-hasheq
  "Returns the order-sensitive Clojure hash of `coll`, compatible with native
  lists and vectors. Computes from current contents without caching."
  [coll]
  (Murmur3/hashOrdered (or (seq coll) [])))

(defn ordered-hash
  "Returns the Java List-style hashCode of `coll`, combining element hashCodes
  in sequence order with 32-bit overflow. Computes from current contents."
  [coll]
  (reduce (fn [h v] (unchecked-add-int (unchecked-multiply-int 31 h) (Util/hash v)))
          1 (seq coll)))

(defn unordered-hasheq
  "Returns the order-independent Clojure hash of `coll`, compatible with native
  sets and maps. Maps contribute key-value entries; sets contribute members.
  Computes from current contents without caching."
  [coll]
  (Murmur3/hashUnordered (or (seq coll) [])))

(defn map-equiv?
  "Compares map `a` with java.util.Map `b` using Clojure value equivalence.
  Persistent maps must also implement MapEquivalence. Requires matching counts
  and keys, distinguishing absent keys from keys whose values are nil."
  [a b]
  (and (instance? java.util.Map b)
       (or (not (map? b)) (instance? clojure.lang.MapEquivalence b))
       (= (count a) (.size ^java.util.Map b))
       (every? (fn [[k v]]
                 (and (.containsKey ^java.util.Map b k)
                      (Util/equiv v (.get ^java.util.Map b k))))
               (seq a))))

(defn map-hash
  "Returns the Java Map-style hashCode of `coll`: the sum of each entry's key
  hashCode XOR value hashCode, with 32-bit overflow and no ordering dependence."
  [coll]
  (reduce (fn [h [k v]] (unchecked-add-int h (bit-xor (Util/hash k) (Util/hash v))))
          0 (seq coll)))

(defn set-hash
  "Returns the Java Set-style hashCode of `coll`: the sum of member hashCodes,
  with 32-bit overflow and no ordering dependence. Computes from current contents."
  [coll]
  (reduce (fn [h v] (unchecked-add-int h (Util/hash v))) 0 (seq coll)))

(defn map-key-set
  "Returns a live, read-only java.util.Set view of map `m`'s keys, backed by
  the map's own seq, count, and containsKey. Iteration follows the map's seq
  order, so sorted maps yield ascending keys. Nothing is copied."
  ^java.util.Set [^Associative m]
  (proxy [AbstractSet] []
    (iterator [] (SeqIterator. (seq (map key (seq m)))))
    (size [] (count m))
    (contains [k] (.containsKey m k))))

(defn map-values
  "Returns a live, read-only java.util.Collection view of map `m`'s values in
  the map's seq order. Nothing is copied."
  ^java.util.Collection [m]
  (proxy [AbstractCollection] []
    (iterator [] (SeqIterator. (seq (map val (seq m)))))
    (size [] (count m))))

(defn map-entry-set
  "Returns a live, read-only java.util.Set view of map `m`'s entries in the
  map's seq order. Membership checks look the key up with entryAt and compare
  the value with Java equality, as clojure.lang.APersistentMap does."
  ^java.util.Set [^Associative m]
  (proxy [AbstractSet] []
    (iterator [] (SeqIterator. (seq m)))
    (size [] (count m))
    (contains [o]
      (boolean
        (when (instance? Map$Entry o)
          (when-let [found (.entryAt m (key o))]
            (Util/equals (val found) (val o))))))))

(defn unsupported-mutation!
  "Throws UnsupportedOperationException for Java collection mutation methods.
  Database views accept updates through their Clojure collection operations."
  []
  (throw (UnsupportedOperationException. "Use Clojure collection operations to update database values")))
