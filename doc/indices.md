# Using indices with xitdb

There's no built-in support for indices, because... 
Indices are yet another data structure which you can store in the database.

```clojure 

(defn records->index-set
  "Returns a map keyed by `k` and valued by a `set` of ids."
  [records k & {:keys [id] :or {id :id}}]
  (->> records
       (group-by k)
       (map (fn [[k songs]]
              [k (->> songs (map id) set)]))
       (into {})))

(defn update-index
  "Updates an index in `m`.
  All indices are stored in the 'index' map (eg. songs-index).
  Eg:
  `{:songs-index {:artist {'foo' #{8 9}}}`
  The songs-index contains an index which groups the songs by artist name -> set of song ids.
  k represents the `key` on which the `records` are going to be grouped and
  then merged into the `k` index."
  [m index k records]
  (let [k->song-id-set (records->index-set records k)]
    (update-in m [index k] #(merge-with into % k->song-id-set))))

(defn insert-songs! [db songs]
  (let [id->song (into {} (map (juxt :id identity)) songs)]
    (swap! db (fn [m]
                (-> m
                    (update :songs merge id->song)
                    (update-index :songs-indices :artist songs)
                    (update-index :songs-indices :tag songs)
                    (update-index :songs-indices :year songs))))))
```


# External indices
Indices can be stored in a separate database file. 
In fact, you might not need all indices in your 'live' database, but you might 
need some indices for analytics or reporting. 