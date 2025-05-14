(ns xitdb.gen-map
  (:require
    [clojure.string :as str]))

;;Code below Generated From Prompt by Claude 3.7
;;I need a Clojure function which generates a deeply nested map with random keys and random values.
;;I will use this function to generate maps to test a database implementation.
;;Each nested map has at least one key valued by a map and one key valued by a vector.
;;Keys can be keywords, integers, strings, boolean or nil. Values can also be maps or arrays.
;;I should be able to configure: num-keys-per-map and max-array-length and max-nesting-level

(defn gen-random-map
  "Generate a deeply nested map with random keys and values.
   Each map has at least one key with a map value and one key with a vector value.

   Parameters:
   - num-keys-per-map: number of keys in each map
   - max-array-length: maximum length of generated arrays
   - max-nesting-level: maximum depth of nesting"
  [num-keys-per-map max-array-length max-nesting-level]

  (letfn [(random-key []
            (let [key-type (rand-int 5)]
              (case key-type
                0 (keyword (str "key-" (rand-int 1000)))  ; keyword
                1 (rand-int 1000)                         ; integer
                2 (str "string-" (rand-int 1000))         ; string
                3 (zero? (rand-int 2))                    ; boolean
                4 nil)))                                  ; nil

          (random-primitive []
            (let [value-type (rand-int 5)]
              (case value-type
                0 (rand-int 1000)                         ; integer
                1 (str "value-" (rand-int 1000))          ; string
                2 (zero? (rand-int 2))                    ; boolean
                3 nil                                     ; nil
                4 (rand))))                               ; float

          (generate-array [level]
            (let [length (inc (rand-int max-array-length))]
              (vec (repeatedly length #(generate-value level)))))

          (generate-value [level]
            (if (>= level max-nesting-level)
              (random-primitive)
              (let [value-type (rand-int 3)]
                (case value-type
                  0 (random-primitive)
                  1 (generate-nested-map (inc level))
                  2 (generate-array (inc level))))))

          (generate-nested-map [level]
            (if (>= level max-nesting-level)
              ;; At max level, just return primitives
              (into {} (for [_ (range (max 1 num-keys-per-map))]
                         [(random-key) (random-primitive)]))
              ;; Otherwise ensure we have at least one map and one vector
              (let [keys-to-generate (max 2 num-keys-per-map)
                    map-key (random-key)
                    vector-key (random-key)

                    ;; The rest of the entries
                    remaining-keys (- keys-to-generate 2)
                    regular-entries (for [_ (range remaining-keys)]
                                      [(random-key) (generate-value level)])]

                ;; Combine ensuring we have the required structure
                (into {} (concat
                           [[map-key (generate-nested-map (inc level))]]
                           [[vector-key (generate-array (inc level))]]
                           regular-entries)))))]

    (generate-nested-map 0)))

(defn random-map-paths
  "Given a deeply nested map, returns a collection of n [path value] tuples.
   Each tuple contains a path vector (for use with get-in) and its corresponding value.
   Paths are selected randomly from all possible paths in the map."
  [m n]
  (letfn [(collect-paths [data path]
            (cond
              (map? data)
              (mapcat (fn [[k v]]
                        (collect-paths v (conj path k)))
                      data)

              (and (coll? data) (not (string? data)))
              (mapcat (fn [i v]
                        (collect-paths v (conj path i)))
                      (range) data)

              :else [[path data]]))]

    (if (pos? n)
      (let [all-paths (collect-paths m [])
            shuffled-paths (shuffle all-paths)]
        (take n shuffled-paths))
      [])))

(comment

  (def rmap (gen-random-map 5 5 3))
  (def paths (random-map-paths rmap 3))

  (doseq [path paths]
    (let [keypath (first path)
          value (second path)]
      (= value (get-in rmap keypath))))

  (spit "test-resources/map-2.edn" (gen-random-map 5 5 6)))