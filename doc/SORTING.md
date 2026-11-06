# Sorting, paginating and indexes

The `Hash`-based structures are great for looking data up by key, but they store their contents in hash order, which is meaningless to a human. Real apps need to show data in a sensible order (such as users listed alphabetically) one page at a time. Relational databases like SQLite have this built-in: you declare a `CREATE INDEX`, write `ORDER BY username LIMIT 20 OFFSET 40`, and the query planner maintains the index for you.

In xitdb there are no built-in indexes, so you build and maintain them yourself. That's a little more code, but the index is just another data structure: a `sorted-map` whose keys sort the way you want. You keep it in sync by writing to it in the same transaction that writes the primary data.

Why a `sorted-map` and not a vector? A vector keeps things in insertion order, which is only useful when the order you want *is* the order you wrote them in. The moment you want a different order (alphabetical, by score, by anything that isn't "when it arrived") you need a structure that stays sorted by a key. A sorted map does, and it can seek straight to the first key greater than or equal to a given value, which is what makes type-ahead search possible.

* [Sorted collections](#sorted-collections)
* [Ranking](#ranking)
* [A user directory with a secondary index](#a-user-directory-with-a-secondary-index)
* [Paginating](#paginating)
* [Type-ahead search](#type-ahead-search)
* [Non-unique sort keys](#non-unique-sort-keys)
* [Grouping indexes](#grouping-indexes)
* [External indexes](#external-indexes)

## Sorted collections

Store a Clojure `sorted-map` or `sorted-set` and it is persisted as an on-disk sorted collection, backed by the engine's rank-augmented B-tree. Reading it back yields an `XITDBSortedMap` / `XITDBSortedSet` that implements Clojure's ordered interfaces, so `seq`, `rseq`, `nth`, `subseq` and `rsubseq` all work and read only what they touch:

```clojure
(require '[xitdb.db :as xdb])
(def db (xdb/xit-db :memory))

(reset! db (sorted-map "banana" 2 "apple" 1 "cherry" 3))

@db
;; => #XITDBSortedMap{"apple" 1, "banana" 2, "cherry" 3}

(swap! db assoc "date" 4)  ;; inserted in order, not appended
(subseq @db >= "c")        ;; => (["cherry" 3] ["date" 4])
(nth @db 1)                ;; => ["banana" 2]   (O(log n), no scan)
```

Supported key and member types are strings, keywords, booleans, chars, longs, doubles, `UUID`, `Instant` and `Date`. They are stored with an order-preserving codec, so they iterate in natural order: numeric for numbers, chronological for temporals, lexicographic (by code point) for strings, `UUID.compareTo` order for UUIDs. Longs and doubles share a single numeric ordering, so they interleave by value (e.g. `1 < 1.5 < 2`). Only the default ordering is supported: `sorted-map-by` / `sorted-set-by` with a custom comparator is rejected, as are `java.util.Date` subclasses like `java.sql.Timestamp` (use `Instant` instead).

In [compat mode](INTEROP.md), sorted keys and members must be strings.

## Ranking

The `xitdb.sorted` namespace exposes the B-tree's O(log n) rank operations:

- `(rank coll k)`: number of entries strictly less than `k` (i.e. the index of `k`, or its would-be insertion index if absent).
- `(from-index coll n)`: lazy ordered seq starting at rank `n`.
- `(page coll offset limit)`: lazy ordered page `[offset, offset+limit)`.

```clojure
(require '[xitdb.sorted :as xsorted])

(xsorted/rank @db "cherry")   ;; => 2
(xsorted/rank @db "coconut")  ;; => 3, where it would be inserted
(xsorted/page @db 1 2)        ;; => (["banana" 2] ["cherry" 3])
```

They work on both `XITDBSortedMap` (yielding map entries) and `XITDBSortedSet` (yielding members).

## A user directory with a secondary index

Let's model a user directory: a collection of users we look up by id, plus a secondary index that lists them alphabetically by username. The primary store is a hash map from user id to the user's fields (like a row keyed by its primary key). The secondary index is a `sorted-map` keyed by username, whose value is the user id to look up.

Now we write some users. Note they're inserted in arbitrary order; the index sorts them, so insertion order doesn't matter. On each insert we write the user into the primary map and add an entry to the secondary index (keeping both in sync is your job, not the database's):

```clojure
;; inserted in arbitrary order; the index will sort them alphabetically
(def new-users
  [{:id "user000000000001" :username "dave"  :name "Dave Smith"}
   {:id "user000000000002" :username "alice" :name "Alice Jones"}
   {:id "user000000000003" :username "carol" :name "Carol White"}
   {:id "user000000000004" :username "dan"   :name "Dan Brown"}
   {:id "user000000000005" :username "bob"   :name "Bob Lee"}
   {:id "user000000000006" :username "eve"   :name "Eve Adams"}])

(defn add-user [moment {:keys [id username name]}]
  (-> moment
      ;; the primary store: a hash map from user id to the user's fields
      (assoc-in [:id->user id] {:username username :name name})
      ;; the secondary index: a sorted map ordered alphabetically by username.
      ;; there's no CREATE INDEX here, so we maintain it ourselves on every write.
      (assoc-in [:username->id username] id)))

(reset! db {})
(swap! db (fn [moment]
            (reduce add-user
                    (assoc moment :id->user {} :username->id (sorted-map))
                    new-users)))
```

## Paginating

To display a page, we walk the sorted map instead of the hash map. A web app would take a `page-size` and an `after` offset from the request (something like `/users?after=20`), so this is the xitdb equivalent of `ORDER BY username LIMIT page-size OFFSET after`:

```clojure
;; a web request would supply these; here we just grab the first page
(def page-size 2)
(def after 0)

(let [{:keys [id->user username->id]} @db]
  ;; seek straight to the start of the page, then walk forward one entry at a
  ;; time. because the sorted map is a count-augmented B-tree, `page` finds
  ;; rank `after` in O(log n) without scanning the entries it skips, so
  ;; jumping to page 500 is just as cheap as page 1.
  (->> (xsorted/page username->id after page-size)
       ;; the index entry's value is the user id; use it to read the
       ;; full user out of the primary map
       (map (fn [[_username id]] (get-in id->user [id :name])))))
;; => ("Alice Jones" "Bob Lee")
```

To page from a known key instead of a rank, such as a timestamp boundary, use `subseq` and `take`:

```clojure
(->> (subseq (:username->id @db) >= "c")
     (take page-size))
;; => (["carol" "user000000000003"] ["dan" "user000000000004"])
```

## Type-ahead search

Pagination by index is only half of what the ordering buys us. Because the index is sorted by username, we can also seek straight to a *key* (the first username greater than or equal to a prefix) and walk forward only as far as the prefix matches. That's a type-ahead search (think @-mention autocomplete), and it's the thing a vector can't do: with no sorted index, there's nothing to seek into. `subseq` seeks to the first key `>=` the prefix in O(log n), and `take-while` stops the lazy walk once a username no longer starts with it:

```clojure
;; the user typed "da" into an @-mention box; find everyone whose username
;; starts with it
(let [prefix "da"]
  (->> (subseq (:username->id @db) >= prefix)
       (map key)
       (take-while #(clojure.string/starts-with? % prefix))))
;; => ("dan" "dave")
```

## Non-unique sort keys

This works for any ordering you need: sort by a username with a string key like we did here, by score with a numeric key (numbers sort numerically, no byte encoding needed), or build several sorted maps over the same primary map to offer the data in different orders. When a sort key isn't unique (many users could share a score), map it to a set of ids so two users with the same score don't overwrite each other:

```clojure
(defn index-score [moment user-id score]
  (update-in moment [:score->ids score] (fnil conj #{}) user-id))

(swap! db #(-> % (assoc :score->ids (sorted-map))  ;; update-in would create a hash map
                 (index-score "user000000000001" 10)
                 (index-score "user000000000002" 10)
                 (index-score "user000000000003" 7)))

(xdb/materialize (rseq (:score->ids @db)))
;; => ([10 #{"user000000000001" "user000000000002"}] [7 #{"user000000000003"}])
```

With xitdb you "bring your own index". It takes a bit more effort than the declarative convenience of SQL databases, but it gives you more explicit control, and avoids the common problem in SQL where queries silently become inefficient due to not using indexes. In xitdb, inefficiency is hard to miss because you are always writing your queries as plain Clojure code and the indexes are always explicit.

## Grouping indexes

Not every index needs an order. A plain hash map from a field value to the set of ids that have it answers "all songs by this artist" in one lookup, and it is cheap to maintain for several fields at once. Here all the indexes of the songs live under one `:songs-indices` key, one map per field:

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
  "Merges `records`, grouped by `k`, into the index `k` under `index`.
  For example `{:songs-indices {:artist {\"foo\" #{8 9}}}}` groups
  the songs by artist name -> set of song ids."
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

(defn titles-of-songs-for-artist [moment artist]
  (->> (get-in moment [:songs-indices :artist artist])
       (map #(get-in moment [:songs % :title]))))
```

Make the index a `sorted-map` instead of a hash map when you also want to walk it in order, such as songs by year.

## External indexes

Indexes can be stored in a separate database file. You might not need every index in your "live" database, but you might need some for analytics or reporting, and a separate file keeps their writes and their history out of the main one. Values read from one database can't be written into another directly, so `materialize` what you copy between them (see [Reading Lazily](../README.md#reading-lazily)).
