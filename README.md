<p align="center">
  xitdb-clj is an immutable database for Clojure
  <br/>
  <br/>
  <b>Choose your flavor:</b>
  <a href="https://github.com/xit-vcs/xitdb">Zig</a> |
  <a href="https://github.com/xit-vcs/xitdb-java">Java</a> |
  <a href="https://github.com/codeboost/xitdb-clj">Clojure</a> |
  <a href="https://github.com/xit-vcs/xitdb-ts">TypeScript</a> |
  <a href="https://github.com/xit-vcs/xitdb-go">Go</a>
</p>

[![Clojars Project](https://img.shields.io/clojars/v/io.github.codeboost/xitdb-clj.svg)](https://clojars.org/io.github.codeboost/xitdb-clj)

* A database that works like a Clojure atom: `swap!`, `reset!` and `deref`, with Clojure data structures.
* Each transaction efficiently creates a new "copy" of the database, and past copies can still be read from and reverted to.
* Supports storing in a single file as well as purely in-memory use.
* Runs as a library (embedded in process).
* Incrementally reads and writes, so file-based databases can contain larger-than-memory datasets.
* Reads never block writes, and a database can be read from multiple threads without locks.
* No query engine of any kind. You just write Clojure data structures (maps, vectors, sets, lists, sorted maps and sorted sets) that can be nested arbitrarily, and query them with `filter`, `group-by`, `reduce`, etc.
* Only one dependency: [xitdb-java](https://github.com/xit-vcs/xitdb-java), which has none besides the Java standard library (currently requires Java 17).
* Database files can be shared with the other flavors of xitdb (see [Hashing and sharing files with other languages](#hashing-and-sharing-files-with-other-languages)).

`xitdb-clj` is a Clojure interface for [xitdb-java](https://github.com/xit-vcs/xitdb-java), itself a port of [xitdb](https://github.com/xit-vcs/xitdb), written in Zig. The database was originally made for the [xit version control system](https://github.com/xit-vcs/xit). The combination of being immutable and having an API similar to in-memory data structures is pretty powerful, and in Clojure it is a natural fit: a xitdb value is read with the same functions as a Clojure map or vector, and a transaction is a function from the old value to the new one. Check out [xitdb.net](https://xitdb.net) and the video [My SQLite killer was a happy accident](https://www.youtube.com/watch?v=YRUF-EaTFFM).

> **⚠️ Alpha Software - Work in Progress**
>
> This project is in early development and rapidly evolving. Expect breaking
> changes, rough edges, and incomplete documentation. Bug reports, suggestions
> and pull requests are very welcome.

* [Example](#example)
* [Initializing a Database](#initializing-a-database)
* [Types](#types)
* [Reading Lazily](#reading-lazily)
* [Cloning and Undoing](#cloning-and-undoing)
* [Cursors](#cursors)
* [Sorting and Paginating](#sorting-and-paginating)
* [Iteration](#iteration)
* [Hashing and sharing files with other languages](#hashing-and-sharing-files-with-other-languages)
* [Compaction](#compaction)
* [Replication](#replication)
* [Thread Safety](#thread-safety)
* [Upgrading from older versions](#upgrading-from-older-versions)
* [Architecture](#architecture)

## Example

Add the dependency to your project:

```clojure
io.github.codeboost/xitdb-clj {:mvn/version "0.7.1"}
```

In this example, we create a new database, write some data in a transaction, and read the data afterwards.

```clojure
(require '[xitdb.db :as xdb])

;; open (or create) a file-based database
(def db (xdb/xit-db "main.db"))

;; this is how a transaction is executed: swap! calls the function with the
;; most recent copy of the database and writes the value it returns. if any
;; error happens in it, the transaction will not complete and the db will be
;; unaffected.
;;
;; after this transaction, the db will look like this if represented as EDN
;; (in reality the format is binary):
;;
;; {:foo "foo"
;;  :bar "bar"
;;  :fruits ["apple" "pear" "grape"]
;;  :people [{:name "Alice" :age 25}
;;           {:name "Bob" :age 42}]}
(swap! db assoc
       :foo "foo"
       :bar "bar"
       :fruits ["apple" "pear" "grape"]
       :people [{:name "Alice" :age 25}
                {:name "Bob" :age 42}])

;; get the most recent copy of the database, like a moment in time
(def moment @db)

;; we can read the value of :foo from the map like from any Clojure map
(:foo moment)
;; => "foo"

;; the :fruits vector is read the same way
(count (:fruits moment))
;; => 3

;; now we can get the first item from the fruits vector
(first (:fruits moment))
;; => "apple"

;; nested paths work too
(get-in moment [:people 1 :name])
;; => "Bob"

;; convert the whole thing to native Clojure data
(xdb/materialize moment)
;; => {:foo "foo", :bar "bar", :fruits ["apple" "pear" "grape"],
;;     :people [{:name "Alice", :age 25} {:name "Bob", :age 42}]}
```

Since the database implements `IAtom`, everything you know about atoms applies: `swap!` with extra arguments, `reset!` to replace the whole value, `@` or `deref` to read it. The top-level value is usually a map, but it can be any supported collection.

```clojure
(swap! db update-in [:people 0 :age] inc)
(swap! db update :fruits conj "banana")
(reset! db {:fresh "start"})
```

## Initializing a Database

`xit-db` takes either a file name or `:memory`:

* A file-based database writes to the file through an in-memory buffer, so it performs well. If the file does not exist, it is created.
* A `:memory` database works completely in memory. It is handy for tests and REPL exploration, and `xitdb.snapshot/snapshot-memory-db` copies part of a file into one.

```clojure
(def file-db (xdb/xit-db "main.db"))
(def mem-db  (xdb/xit-db :memory))
```

A database is `java.io.Closeable`, so use `with-open` when its lifetime is scoped, or call `.close` when you are done with it:

```clojure
(with-open [db (xdb/xit-db :memory)]
  (reset! db {:answer 42})
  (:answer @db))
;; => 42
```

Since xitdb is an append-only database, the data you are writing is invisible to any reader until the very last step, when a small header at the start of the file is updated to point at it. This is how it supports transactions, despite not having any rollback journal or write-ahead log: if a transaction fails, the data it appended is simply discarded. A `swap!` whose function throws leaves the database unchanged.

The top-level data structure of the file is an array list called the *history*. Each transaction appends the new copy of the database (which I call a "moment") to it, so past copies stay readable and can be reverted to. `@db` reads the last entry, `count` tells you how many there are, and `deref-at` reads any of them:

```clojure
(reset! db {:version 1})
(swap! db assoc :version 2)
(swap! db assoc :version 3)

(count db)              ;; => 3
(xdb/deref-at db -1)    ;; the most recent moment, same as @db
(xdb/deref-at db -2)    ;; the one before it: {:version 2}
(xdb/deref-at db 0)     ;; the earliest moment: {:version 1}
```

A `swap!` can also return the history count together with the previous and new values, by binding `*return-history?*` to `true`. The history count is the `count` of the database after the transaction, so the moment it wrote is at `(dec history-count)`:

```clojure
(binding [xdb/*return-history?* true]
  (let [[history-count old-value new-value] (swap! db assoc :version 4)]
    (println "old:" (xdb/materialize old-value))
    (println "new:" (xdb/materialize new-value))
    (= new-value (xdb/deref-at db (dec history-count)))))
;; old: {:version 3}
;; new: {:version 4}
;; => true
```

## Types

In xitdb there are a variety of immutable data structures that you can nest arbitrarily. xitdb-clj maps Clojure collections onto them, and reads them back as Clojure collections:

| Clojure value | xitdb structure | Read back as |
|---|---|---|
| map `{}` | `HashMap` (counted in Clojure mode) | `XITDBHashMap` |
| set `#{}` | `HashSet` (counted in Clojure mode) | `XITDBHashSet` |
| vector `[]` | `ArrayList` | `XITDBArrayList` |
| list `'()` / seq | `LinkedArrayList` | `XITDBLinkedList` |
| `sorted-map` | `SortedMap` | `XITDBSortedMap` |
| `sorted-set` | `SortedSet` | `XITDBSortedSet` |

The `Hash`-based data structures and the `ArrayList` use the hash array mapped trie, invented by Phil Bagwell (originally made immutable and widely available by Rich Hickey in Clojure). The `LinkedArrayList`, `SortedMap`, and `SortedSet` are based on a B-tree. The wrappers implement the Clojure collection interfaces, so `get`, `assoc`, `conj`, `seq`, `count`, `contains?`, `subseq` and friends all work on them.

Scalar values are stored as the engine's byte arrays, integers and floats. The Clojure types that xitdb has no native tag for (keywords, booleans, chars, `UUID`, `Instant`, `Date`) are stored as byte arrays with a two-byte *format tag* (for example `kw` for a keyword), which is how they come back as the right type when read:

| Clojure value | Stored as |
|---|---|
| string | `Bytes` |
| integer (`long`) | `Int` |
| `double` | `Float` |
| keyword | `Bytes` with format tag `kw` |
| boolean | `Bytes` with format tag `bl` |
| char | `Bytes` with format tag `ch` |
| `java.util.UUID` | `Bytes` with format tag `uu` |
| `java.time.Instant` | `Bytes` with format tag `in` |
| `java.util.Date` | `Bytes` with format tag `da` |
| `nil` | an empty slot |

```clojure
(reset! db {:when  (java.time.Instant/parse "2026-01-01T18:55:48Z")
            :id    (random-uuid)
            :ratio 0.75
            :tags  #{:a :b}
            :ok?   true})

(type (:when @db)) ;; => java.time.Instant
(:ok? @db)         ;; => true
```

Any other value type throws `IllegalArgumentException` when written, including byte arrays, big integers, and records. To store such a value, serialize it yourself into a supported type, such as a string. Records and `NaN` are also rejected as map keys and set members, including anywhere inside collection-valued keys or members, so writes throw instead of silently merging unequal keys. Convert record keys to plain maps explicitly if map identity is what you need.

Map keys can be any supported scalar or collection in Clojure mode, including `nil`; in compat mode they must be strings (see [Hashing](#hashing-and-sharing-files-with-other-languages)).

## Reading Lazily

Reading from the database does not load the whole value into memory. It returns wrappers around cursors into the database file:

```clojure
(type @db)                         ;; => xitdb.hash_map.XITDBHashMap
(type (get-in @db [:people 0]))    ;; => xitdb.hash_map.XITDBHashMap
```

A `XITDBHashMap` wraps xitdb-java's `ReadHashMap`, which holds a cursor to the tree node in the file. Any read operation on these types returns new `XITDB` types, reading only the nodes it touches, so you can navigate a database larger than memory with `get-in`, `filter`, `reduce` and friends.

Use `materialize` to convert a nested `XITDB` value into native Clojure data:

```clojure
(xdb/materialize (get-in @db [:people 0]))
;; => {:name "Alice", :age 25}
```

A value read from a database is a pointer into that database's storage. It can be written back into the *same* database without copying (this is what makes reverting to an earlier moment cheap, see below), but it cannot be written into a *different* database. Doing so throws `IllegalArgumentException`; `materialize` the value first to copy it:

```clojure
(reset! other-db (xdb/materialize (:people @db)))
```

## Cloning and Undoing

A powerful feature of immutable data is fast cloning. Any data structure can be instantly cloned and changed without affecting the original. Starting with the example above, we can make a new transaction that creates a `:food` vector based on the existing `:fruits` vector. Before we do, let's save the latest history index, so we can revert back to this moment of the database later:

```clojure
(def history-index (dec (count db)))
```

Now the clone. A first attempt looks like plain Clojure:

```clojure
(swap! db (fn [moment]
            (-> moment
                ;; create a new key called :food whose initial value is
                ;; based on the :fruits vector
                (assoc :food (:fruits moment))
                (update :food conj "eggs" "rice" "fish"))))

;; the food vector includes the fruits
(count (:food @db))   ;; => 6

;; ..but so does fruits! we did not intend to mutate this
(count (:fruits @db)) ;; => 6
```

The reason that `:fruits` was mutated is because all data in a given transaction is temporarily **mutable**, like a Clojure transient. This is a very important optimization: every `assoc` and `update` inside `swap!` writes straight through to the file instead of building a new value. Reading `(:fruits moment)` through the transaction's moment makes that vector part of the transaction too, so `:food` and `:fruits` point at the same mutable vector, and appending to one appends to both. (Data from previous transactions is immutable in the file, but the moment you reach it through the moment being written, it becomes writable in that transaction. This is different from the Java library, where you choose a read or a write cursor explicitly.)

In this case, it's not what we want. To show how to fix this, let's first undo the transaction we just made. Here we use the `history-index` we saved before to revert back to the older database moment. Since the old moment is already in the file, this writes just a pointer, not a copy:

```clojure
(reset! db (xdb/deref-at db history-index))

(xdb/materialize @db)
;; => {:fruits ["apple" "pear" "grape"], ...}
```

This time, after reading `:fruits`, we call `freeze!` on it. This tells xitdb that all data written so far in the transaction must be treated as immutable, so later writes copy it instead of changing it. Now it works the way we wanted:

```clojure
(swap! db (fn [moment]
            (-> moment
                ;; freeze here, so writes to :food won't mutate :fruits
                (assoc :food (xdb/freeze! (:fruits moment)))
                (update :food conj "eggs" "rice" "fish"))))

;; the food vector contains all six
(count (:food @db))   ;; => 6

;; and fruits only contains the original three
(count (:fruits @db)) ;; => 3
```

`freeze!` is not an expensive copy; the frozen vector still shares its first three elements with `:food` on disk, just like structural sharing between in-memory Clojure vectors. It only exists because the default is the opposite of Clojure's: in Clojure you opt *in* to temporary mutability with transients, whereas inside a xitdb transaction you opt *out* of it. `freeze!` throws if called outside a transaction.

The same applies to data created earlier in the same transaction, which is the case the Java README shows with its "big cities" example: `(assoc moment :big-cities [...])` followed by `(assoc moment :cities (:big-cities moment))` shares one mutable vector until you `freeze!` it.

## Cursors

A cursor is an atom-like view onto a keypath inside the database. `deref`, `swap!` and `reset!` on it read and write the value at that path, and a `swap!` on the cursor is still one transaction on the whole database:

```clojure
(reset! db {:counters {:visits 10} :untouched 99})

(def visits (xdb/xdb-cursor db [:counters :visits]))

@visits               ;; => 10
(swap! visits inc)    ;; => 11
(swap! visits + 1 2)  ;; => 14
(reset! visits 25)    ;; => 25

(xdb/materialize @db) ;; => {:counters {:visits 25}, :untouched 99}
```

Cursors nest: `(xdb-cursor visits [...])` appends to the keypath. With `*return-history?*` bound, the previous and new values returned by `swap!` are the values at the cursor's keypath. A cursor whose path does not exist derefs to `nil`, like `get-in`.

## Sorting and Paginating

The `Hash`-based structures are great for looking data up by key, but they store their contents in hash order, which is meaningless to a human. Real apps need to show data in a sensible order (such as users listed alphabetically) one page at a time. Relational databases like SQLite have this built-in: you declare a `CREATE INDEX`, write `ORDER BY username LIMIT 20 OFFSET 40`, and the query planner maintains the index for you.

In xitdb there are no built-in indexes, so you build and maintain them yourself. That's a little more code, but the index is just another data structure: a `sorted-map` whose keys sort the way you want. You keep it in sync by writing to it in the same transaction that writes the primary data.

Why a `sorted-map` and not a vector? A vector keeps things in insertion order, which is only useful when the order you want *is* the order you wrote them in. The moment you want a different order (alphabetical, by score, by anything that isn't "when it arrived") you need a structure that stays sorted by a key. A sorted map does, and it can seek straight to the first key greater than or equal to a given value, which is what makes type-ahead search possible.

Store a Clojure `sorted-map` or `sorted-set` and it is persisted as an on-disk sorted collection, backed by the engine's rank-augmented B-tree. Reading it back yields an `XITDBSortedMap` / `XITDBSortedSet` that implements Clojure's ordered interfaces, so `seq`, `rseq`, `nth`, `subseq` and `rsubseq` all work and read only what they touch:

```clojure
(reset! db (sorted-map "banana" 2 "apple" 1 "cherry" 3))

@db
;; => #XITDBSortedMap{"apple" 1, "banana" 2, "cherry" 3}

(swap! db assoc "date" 4)  ;; inserted in order, not appended
(subseq @db >= "c")        ;; => (["cherry" 3] ["date" 4])
(nth @db 1)                ;; => ["banana" 2]   (O(log n), no scan)
```

Supported key and member types are strings, keywords, booleans, chars, longs, doubles, `UUID`, `Instant` and `Date`. They are stored with an order-preserving codec, so they iterate in natural order: numeric for numbers, chronological for temporals, lexicographic (by code point) for strings, `UUID.compareTo` order for UUIDs. Longs and doubles share a single numeric ordering, so they interleave by value (e.g. `1 < 1.5 < 2`). Only the default ordering is supported: `sorted-map-by` / `sorted-set-by` with a custom comparator is rejected, as are `java.util.Date` subclasses like `java.sql.Timestamp` (use `Instant` instead).

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

(swap! db (fn [moment]
            (reduce add-user
                    (assoc moment :id->user {} :username->id (sorted-map))
                    new-users)))
```

To display a page, we walk the sorted map instead of the hash map. The `xitdb.sorted` namespace exposes the B-tree's O(log n) rank operations:

- `(rank coll k)`: number of entries strictly less than `k` (i.e. the index of `k`, or its would-be insertion index if absent).
- `(from-index coll n)`: lazy ordered seq starting at rank `n`.
- `(page coll offset limit)`: lazy ordered page `[offset, offset+limit)`.

A web app would take a `page-size` and an `after` offset from the request (something like `/users?after=20`), so this is the xitdb equivalent of `ORDER BY username LIMIT page-size OFFSET after`:

```clojure
(require '[xitdb.sorted :as xsorted])

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

With xitdb you "bring your own index". It takes a bit more effort than the declarative convenience of SQL databases, but it gives you more explicit control, and avoids the common problem in SQL where queries silently become inefficient due to not using indexes. In xitdb, inefficiency is hard to miss because you are always writing your queries as plain Clojure code and the indexes are always explicit. See [doc/indices.md](doc/indices.md) for a pattern that groups records by several fields at once.

Since the values are Clojure collections, a query engine is a library away: [datascript works out of the box](https://gist.github.com/xeubie/663116fcd204f3f89a7e43f52fa676ef) if you store the datoms as a vector in the db.

## Iteration

All data structures support Clojure's sequence functions. Here's an example of iterating over the `:people` vector and printing all of the keys and values of each map contained in it:

```clojure
(doseq [person (:people @db)
        [k v]  person]
  (println k v))
;; :name Alice
;; :age 25
;; :name Bob
;; :age 42
```

Values come back as their Clojure types, so there is no tag to switch on. `reduce`, `into`, `map` and `filter` all work, and they read entries on demand, so a `(take 10 ...)` over a huge vector reads ten elements. `reduce-kv` on a map and `nth` on a vector are direct lookups rather than scans.

For the sorted collections, `subseq`, `rsubseq` and `xitdb.sorted/from-index` start the walk from a key or a rank respectively, without visiting what they skip. See the [Sorting and Paginating](#sorting-and-paginating) section for an example.

## Hashing and sharing files with other languages

Hash maps and sets look keys up by their SHA-1 hash. Files record their key hashing in a four-byte header ID, and xitdb-clj has two modes:

* **Clojure mode** (header `clj1`) hashes keys with a canonical, type-aware encoding, so any supported value can be a key: strings, keywords, numbers, `nil`, even vectors and maps. This is the default for new files. Other languages can read the values in such a file, but can't look its keys up.
* **Compat mode** (any other header, such as the `0` that xitdb-java writes by default) hashes string keys as their raw UTF-8 bytes, the way the Java and TypeScript libraries do, so both sides can read and write the same file.

An existing file opens in the mode its header records, so files from other languages open in compat mode with no option:

```clojure
(def db (xdb/xit-db "from-java.db"))
(xdb/db-mode db) ;; => :compat
(get-in @db ["profile" "city"])
(swap! db assoc "name" "Alice")
```

Create a new database in compat mode when another language will read it:

```clojure
(def db (xdb/xit-db "shared.db" {:mode :compat})) ;; also works with :memory
```

Passing `:mode` for an existing file also checks it against the header, for code that relies on one mode. A mismatch throws `IllegalArgumentException` and leaves the file unmodified:

| File                            | no `:mode`                 | `{:mode :clojure}`         | `{:mode :compat}` |
|---------------------------------|----------------------------|----------------------------|-------------------|
| New or empty                    | created with header `clj1` | created with header `clj1` | created with header `sha1` |
| Header `clj1`                   | opens in Clojure mode      | opens                      | rejected |
| Any other header (`0`, `sha1`…) | opens in compat mode       | rejected                   | opens |

`compact` keeps the header, so a compacted database stays in its mode. A full copy during [replication](doc/REPLICATION.md) brings the source's header, so the replica switches to the source's mode, even if it was opened with `:mode`.

In compat mode, hash map keys and hash set members must be strings, at every depth. Writing anything else throws `IllegalArgumentException` and the transaction leaves the database unchanged; values can still be any supported type. Reading with a non-string key finds nothing (`(:foo m)` is `nil`), and `dissoc` or `disj` of one does nothing. So if a lookup on a file from another language comes back `nil`, check that the key is a string, not a keyword.

The same rule holds for sorted map keys and sorted set members: they must be strings and are stored as their raw UTF-8 bytes, as Java's `WriteSortedMap.put(String, …)` and `WriteSortedSet.put(String)` store them. Lookups and `contains?` with a non-string key find nothing, and `dissoc` or `disj` of one does nothing. A non-string key is never present and sorts before every string, so `(subseq m > :k)` is the whole map, `(rsubseq m < :k)` is empty, and `rank` is 0. A sorted map shared with Java:

```java
// Java
var scores = new WriteSortedMap(moment.putCursor("scores"));
scores.put("alice", new Database.Uint(1));
scores.put("bob", new Database.Uint(2));
```

```clojure
;; Clojure
(get-in @db ["scores" "bob"])            ;; => 2
(subseq (get @db "scores") >= "b")       ;; => (["bob" 2])
(swap! db update "scores" assoc "carol" 3)
```

Maps and sets written from native Clojure values are plain, not counted, so Java reads them with `ReadHashMap` and `ReadHashSet`, and `count` on them walks the whole collection. Editing an existing collection in place keeps its kind, but replacing a Java-written counted one with a native value makes it plain. Maps and sets in Clojure mode are still counted.

Files from other languages must use the same file format as xitdb-clj, the one xitdb-java has written since 0.46.0; files in an older format are rejected.

Not supported: foreign format tags on values, binary values, hashes other than SHA-1, a top-level hash map with no history, and read-only opening.

## Compaction

Normally, an immutable database grows forever, because old data is never deleted. To reclaim disk space and clear the history, xitdb supports compaction. This involves completely rebuilding the database file to only contain the data accessible from the latest copy (i.e., "moment") of the database.

```clojure
(with-open [compacted (xdb/compact db "compact.db")]
  (count compacted)                      ;; => 1
  (println (xdb/materialize @compacted)))
```

`compact` takes either a file name or `:memory` and returns a new, open database rather than modifying the existing one. A file target must not already exist, because in-place compaction is not supported. If you want to delete the original database and replace it with the compacted one, you'll need to do that yourself.

Compaction tracks the offsets of copied objects in a temporary on-disk xitdb file, so it does not run out of memory on large databases. It holds the source database's write lock for the duration of the copy: reads continue, but any `swap!` or `reset!` on the source will block until compaction finishes. For the same reason, `compact` must not be called from inside a `swap!` or `reset!` on the database being compacted; it throws `IllegalStateException` if it is.

You may be wondering if there is a way to evict certain pieces of data without losing all of your database's history. For example, imagine your moment contains a vector of people:

```clojure
{:people [{:name "Alice" :age 25}
          {:name "Bob" :age 42}]}
```

If Alice asks you to delete her data, you can remove her from the latest moment and compact the database. Her data will be gone, but so will all of your past moments, including every past version of Bob.

It's important to realize that the top-level history isn't the only place you can keep history. It's the only one compaction discards, so you are free to track history of specific things within your moment. The `:people` vector can contain vectors tracking the history of changes for each person individually.

Additionally, you can have a "global" internal history vector (`:versions` below), where each item is a vector of integers that are indexes into each person's history. Here is what the moment looks like after Bob turns 43:

```clojure
{:people   [[{:name "Alice" :age 25}]
            [{:name "Bob" :age 42} {:name "Bob" :age 43}]]
 :versions [[0 0]
            [0 1]]}
```

Each item in `:versions` is a snapshot of the database, with one integer per person that is an index into their list. For example, the last version says that Alice is `(get-in moment [:people 0 0])` and Bob is `(get-in moment [:people 1 1])`.

Compaction can't follow an integer, so `:people` is the only way to reach Alice's data. To evict her, you just clear her slot with `(swap! db assoc-in [:people 0] nil)`, compact the database, then delete the original. Every past version of Bob will still be there, but you'll need to handle the integers that no longer lead anywhere when reading a version.

## Replication

Since xitdb files are append-only, replicating them is just copying the bytes that the destination doesn't have yet. With `xitdb.replication` loaded, databases work with `clojure.java.io`, so a sync is an `io/copy`:

```clojure
(require '[clojure.java.io :as io]
         '[xitdb.replication :as replication])

(with-open [in  (io/input-stream source :since (replication/replica-state replica))
            out (io/output-stream replica)]
  (io/copy in out))
```

See [doc/REPLICATION.md](doc/REPLICATION.md) for replicating over a network, full copies, interrupted syncs, and the rules for replicas.

## Thread Safety

A database handle can be used from multiple threads, and so can the values read from it. Reads never take a lock and run in parallel with each other and with a writer: a reader sees the moment that was current when it dereferenced, and a transaction committed afterwards does not change it. This is a big benefit of immutable databases.

Writes are serialized: `swap!` and `reset!` take the database's lock, so only one transaction runs at a time and `swap!` is never retried, unlike on an atom. Calling `swap!` or `reset!` from inside another transaction on the same database throws `IllegalStateException`.

A file-based database also takes an exclusive file lock for the duration of each transaction, so another process that opens the same file can read it, but a write from a second process while a transaction runs throws `IllegalStateException` ("Database is locked for writing").

## Upgrading from older versions

Version 0.6.0 uses a new file format (from xitdb-java 0.46.0, which adds replication), so it can't open files written by 0.5.0 and earlier. Those old files have header `0`, the same as Java files, but are rejected in every mode because of the format, so they never open in compat mode, where lookups would miss and writes would create duplicates. Opening an incompatible file throws `IllegalArgumentException` and does not modify it.

To upgrade, export the data using the version that created the file (`materialize` reads entries without looking them up by hash), then import it into a **new** database with the current version. Export each `deref-at` value in order if history must be preserved. Use a serializer that preserves your value types and sorted collections; plain EDN does not round-trip every supported type. Compaction does not migrate files.

Compat files written by xitdb-clj 0.7.0 and 0.7.1 store sorted map keys and sorted set members with a leading type-tag byte (a space for strings), which this version reads as part of the key. Those sorted collections must be rewritten.

## Architecture

`xitdb-clj` builds on [xitdb-java](https://github.com/xit-vcs/xitdb-java), which implements:

- **Hash Array Mapped Trie (HAMT)** for hash maps, hash sets and vectors
- **B-trees** for sorted maps, sorted sets and linked lists
- **Structural sharing** between moments, so a transaction writes only what changed
- **Append-only, copy-on-write storage**, which gives transactions and cheap history

The Clojure wrapper adds:

- The atom interface (`IAtom`, `IDeref`) with `swap!`, `reset!` and `deref`
- Lazy `XITDB` collection types that implement the Clojure collection interfaces
- Automatic conversion between Clojure values and the engine's types, with format tags for keywords, booleans, UUIDs and temporals
- A canonical key hashing (`clj1`) that supports every Clojure key type, and a compat mode for sharing files with the other xitdb libraries
- Cursors, history access, freezing, compaction and replication built on the engine's primitives

Read operations on maps and vectors are O(log16 n) thanks to the trie structure; writes are O(log16 n) with structural sharing. Sorted collections are O(log n) for lookup, rank and seek.

## License

MIT
