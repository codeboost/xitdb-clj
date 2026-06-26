> **⚠️ Alpha Software - Work in Progress**
>
> This project is in early development and rapidly evolving. 
> Expect breaking changes, rough edges, and incomplete documentation.
>
> **Help Wanted!** If you find this useful, please consider contributing:
> - Report bugs and issues you encounter
> - Suggest improvements or new features
> - Submit pull requests for fixes or enhancements
> - Share your configuration patterns and workflows
> - Help improve documentation and examples
>
> Your feedback and contributions will help make this tool better for the entire Clojure community!

## Overview

`xitdb-clj` is a embedded database for efficiently storing and retrieving immutable, persistent data structures.
The library provides atom-like semantics for working with the database from Clojure.

It is a Clojure interface for [xitdb-java](https://github.com/xit-vcs/xitdb-java), itself a port of [xitdb](https://github.com/xit-vcs/xitdb), written in Zig.


[![Clojars Project](https://img.shields.io/clojars/v/io.github.codeboost/xitdb-clj.svg)](https://clojars.org/io.github.codeboost/xitdb-clj)

## Main characteristics
 
- Embeddable, tiny library.
- Supports writing to a file as well as purely in-memory use.
- Each transaction (done via `swap!`) efficiently creates a new "copy" of the database, and past copies can still be read from.
- Reading/Writing to the database is efficient, only the necessary nodes are read or written.
- Thread safe. Multiple readers, one writer.
- Append-only. The data you are writing is invisible to any reader until the very last step, when the top-level history header is updated.
- All heavy lifting done by the bare-to-the-jvm java library.
- Database files can be used from other languages, via [xitdb Java library](https://github.com/xit-vcs/xitdb-java) or the [xitdb Zig library](https://github.com/xit-vcs/xitdb)

## Quick Start

Add the dependency to your project, start a REPL.

### You already know how to use it! 

For the programmer, a `xitdb` database is like a Clojure atom.
`reset!` or `swap!` to reset or update, `deref` or `@` to read.

```clojure
(require '[xitdb.db :as xdb])
(def db (xdb/xit-db "my-app.db"))
;; Use it like an atom
(reset! db {:users {"alice" {:name "Alice" :age 30}
                    "bob"   {:name "Bob" :age 25}}})
;; Read the entire database
(xdb/materialize @db)
;; => {:users {"alice" {:name "Alice", :age 30}, "bob" {:name "Bob", :age 25}}}

(get-in @db [:users "alice" :age])
;; => 30
(swap! db assoc-in [:users "alice" :age] 31)

(get-in @db [:users "alice" :age])
;; => 31
```

## Data structures are read lazily from the database

Reading from the database returns wrappers around cursors in the database file:

```clojure 
(type @db) ;; => xitdb.hash_map.XITDBHashMap
```

The returned value is a `XITDBHashMap` which is a wrapper around the xitdb-java's `ReadHashMap`, 
which itself has a cursor to the tree node in the database file. 
These wrappers implement the protocols for Clojure collections - vectors, lists, maps and sets, 
so they behave exactly like the Clojure native data structures.
Any read operation on these types is going to return new `XITDB` types:

```clojure
(type (get-in @db [:users "alice"])) ;; => xitdb.hash_map.XITDBHashMap
```

So it will not read the entire nested structure into memory, but return a 'cursor' type, which you can operate upon
using Clojure functions.

Use `materialize` to convert a nested `XITDB` data structure to a native Clojure data structure:

```clojure
(xdb/materialize (get-in @db [:users "alice"])) ;; => {:name "Alice" :age 31}
```

## No query language

Use `filter`, `group-by`, `reduce`, etc.
If you want a query engine, [datascript works out of the box](https://gist.github.com/xeubie/663116fcd204f3f89a7e43f52fa676ef), you can store the datoms as a vector in the db.

Here's a taste of how your queries could look like:
```clojure 
(defn titles-of-songs-for-artist
  [db artist]
  (->> (get-in db [:songs-indices :artist artist])
       (map #(get-in db [:songs % :title]))))

(defn what-is-the-most-viewed-song? [db tag]
  (let [views (->> (get-in db [:songs-indices :tag tag])
                   (map (:songs db))
                   (map (juxt :id :views))
                   (sort-by #(parse-long (second %))))]
    (get-in db [:songs (first (last views))])))

```

## Sorted collections

In addition to (unordered) hash maps and sets, xitdb supports **on-disk sorted
maps and sets**, backed by the engine's rank-augmented B-tree. Store a Clojure
`sorted-map` / `sorted-set` and it is persisted as a sorted collection that keeps
its keys/members ordered on disk:

```clojure
(reset! db (sorted-map "banana" 2 "apple" 1 "cherry" 3))

@db
;; => #XITDBSortedMap{"apple" 1, "banana" 2, "cherry" 3}

(swap! db assoc "date" 4) ;; inserted in order, not appended
```

Reading back yields an `XITDBSortedMap` / `XITDBSortedSet` that implements
Clojure's ordered interfaces, so `seq`, `rseq`, `nth`, `subseq` and `rsubseq`
all work and read only what they touch from disk:

```clojure
(reset! db (into (sorted-map) (map vector (range 0 100 2) (range))))

(nth @db 10)          ;; => [20 10]   ;; O(log n), no full scan
(subseq @db >= 90)    ;; => ([90 45] [92 46] [94 47] [96 48] [98 49])
(rseq @db)            ;; => lazy descending seq of entries
```

Supported key/member types are strings, keywords, longs, doubles, `Instant`
and `Date`. They are stored with an order-preserving codec, so they iterate in
natural order — numeric for numbers, chronological for temporals, lexicographic
(by code point) for strings. Longs and doubles share a single numeric ordering,
so they interleave by value (e.g. `1 < 1.5 < 2`). Only the default ordering is
supported: `sorted-map-by` / `sorted-set-by` with a custom comparator is
rejected.

### Ranking & pagination

The `xitdb.sorted` namespace exposes the B-tree's O(log n) superpowers, which
are handy for building and paging on-disk secondary indexes:

- `(rank coll k)` — number of entries strictly less than `k` (i.e. the index of
  `k`, or its would-be insertion index if absent).
- `(from-index coll n)` — lazy ordered seq starting at rank `n`.
- `(page coll offset limit)` — lazy ordered page `[offset, offset+limit)`.

```clojure
(require '[xitdb.sorted :as xsorted])

;; build a timestamp -> id index; events can arrive out of order
(reset! db (sorted-map))
(doseq [e events]
  (swap! db assoc (:ts e) (:id e)))

(xsorted/rank @db some-ts)     ;; chronological position of some-ts
(xsorted/page @db 100 20)      ;; the 20 entries at ranks [100, 120)
```

## History

Since the database is immutable, all previous values are accessed by reading
from the respective `history index`.
The root data structure of a xitdb database is a ArrayList, called 'history'.
Each transaction adds a new entry into this array, which points to the latest value 
of the database (usually a map).

```clojure
(xdb/deref-at db -1) ;; the most recent value, same as @db
(xdb/deref-at db -2) ;; the second most recent value
(xdb/deref-at db 0)  ;; the earliest value
(xdb/deref-at db 1)  ;; the second value
```

You can get the latest history index from the `count` of the database:

```clojure
(def history-index (dec (count db)))
```

After making further transactions, you can revert back to it simply like this:

```clojure
(reset! db (xdb/deref-at db history-index))
```

It is also possible to create a transaction which returns the previous and current 
values of the database, by setting the `*return-history?*` binding to `true`.

```clojure
;; Work with history tracking
(binding [xdb/*return-history?* true]
  (let [[history-index old-value new-value] (swap! db assoc :new-key "value")]
    (println "old value:" old-value)
    (println "new value:" new-value)))
```

## Freezing

One important distinction from the Clojure atom is that inside a transaction (eg. a `swap!`), the data is temporarily mutable. This is exactly like Clojure's transients, and it is a very important optimization. However, this can lead to a surprising behavior:

```clojure
(swap! db (fn [moment]
            (let [moment (assoc moment :fruits ["apple" "pear" "grape"])
                  moment (assoc moment :food (:fruits moment))
                  moment (update moment :food conj "eggs" "rice" "fish")]
              moment)))

;; =>

{:fruits ["apple" "pear" "grape" "eggs" "rice" "fish"]
 :food ["apple" "pear" "grape" "eggs" "rice" "fish"]}

;; the fruits vector was mutated!
```

If you want to prevent data from being mutated within a transaction, you must `freeze!` it:

```clojure
(swap! db (fn [moment]
            (let [moment (assoc moment :fruits ["apple" "pear" "grape"])
                  moment (assoc moment :food (xdb/freeze! (:fruits moment)))
                  moment (update moment :food conj "eggs" "rice" "fish")]
              moment)))

;; =>

{:fruits ["apple" "pear" "grape"]
 :food ["apple" "pear" "grape" "eggs" "rice" "fish"]}
```

Note that this is not doing an expensive copy of the fruits vector. We are benefitting from structural sharing, just like in-memory Clojure data. The reason we have to `freeze!` is because the default is different than Clojure; in Clojure, you must opt-in to temporary mutability by using transients, whereas in xitdb you must opt-out of it.

### Architecture
`xitdb-clj` builds on [xitdb-java](https://github.com/xit-vcs/xitdb-java) which implements:

- **Hash Array Mapped Trie (HAMT)** - For HashMap/Set and ArrayList
- **B-trees** - For SortedMap/Set and LinkedArrayList (ArrayList with efficient slice and concat)
- **Structural Sharing** - Minimizes memory usage across versions
- **Copy-on-Write** - Ensures immutability while maintaining performance

The Clojure wrapper adds:
- Idiomatic Clojure interfaces (`IAtom`, `IDeref`)
- Automatic type conversion between Clojure and Java types
- Thread-local read connections for scalability
- Integration with Clojure's sequence abstractions

### Supported Data Types

- **Maps** - Hash maps with efficient key-value access
- **Sorted maps** - On-disk B-tree maps with ordered iteration, `subseq`/`nth`/`rank`
- **Vectors** - Array lists with indexed access
- **Sets** - Hash sets with unique element storage
- **Sorted sets** - On-disk B-tree sets with ordered iteration and ranking
- **Lists** - Linked lists and RRB tree-based linked array lists
- **Primitives** - Numbers, strings, keywords, booleans, dates.

## Performance Characteristics

- **Read Operations**: O(log16 n) for maps and vectors due to trie structure
- **Write Operations**: O(log16 n) with structural sharing for efficiency
- **Memory Usage**: Minimal overhead with automatic deduplication of identical subtrees
- **Concurrency**: Thread-safe with optimized read-write locks


## License

MIT
