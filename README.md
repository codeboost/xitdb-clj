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

Version 0.6.0 uses a new file format (from xitdb-java 0.46.0, which adds
[replication](#replication)), so it can't open files written by 0.5.0 and
earlier. Database files also record their key-hash format in their header:
`clj1` for the Clojure key hashing, while any other header opens in
[compat mode](#compatibility-mode-sharing-files-with-other-languages). Files
whose keys aren't hashed with SHA-1 are rejected. Opening an incompatible file
throws `IllegalArgumentException` and does not modify it.

To upgrade, export the data using the version that created the file
(`materialize` reads entries without looking them up by hash), then import it
into a **new** database with the current version. Export each `deref-at` value
in order if history must be preserved. Use a serializer that preserves your
value types and sorted collections; plain EDN does not round-trip every
supported type. Compaction does not migrate files.

Records and `NaN` are rejected as map keys and set members, including anywhere
inside collection-valued keys or members. Writes throw `IllegalArgumentException`
instead of silently merging unequal keys. Convert record keys to plain maps
explicitly if map identity is what you need.

### Compatibility mode: sharing files with other languages

The default Clojure key hashing supports every key type, but the Java and
TypeScript libraries can't look those keys up. Compat mode hashes string keys the
way those libraries do, so both sides can read and write the same file.

A file's header records its mode, and an existing file opens in that mode. Files
from other languages open in compat mode with no option:

```clojure
(def db (xdb/xit-db "from-java.db"))
(xdb/db-mode db) ;; => :compat
(get-in @db ["profile" "city"])
(swap! db assoc "name" "Alice")
```

New databases are in Clojure mode. Create one in compat mode when another
language will read it:

```clojure
(def db (xdb/xit-db "shared.db" {:mode :compat})) ;; also works with :memory
```

Passing `:mode` for an existing file also checks it against the header, for code
that relies on one mode. A mismatch throws `IllegalArgumentException` and leaves
the file unmodified:

| File                            | no `:mode`                 | `{:mode :clojure}`         | `{:mode :compat}` |
|---------------------------------|----------------------------|----------------------------|-------------------|
| New or empty                    | created with header `clj1` | created with header `clj1` | created with header `sha1` |
| Header `clj1`                   | opens in Clojure mode      | opens                      | rejected |
| Any other header (`0`, `sha1`…) | opens in compat mode       | rejected                   | opens |

`compact` keeps the header, so a compacted database stays in its mode. A full
copy during [replication](#replication) brings the source's header, so the replica
switches to the source's mode, even if it was opened with `:mode`.

In compat mode, hash map keys and hash set members must be strings, at every
depth. Writing anything else throws `IllegalArgumentException` and the transaction
leaves the database unchanged; values can still be any supported type. Reading
with a non-string key finds nothing (`(:foo m)` is `nil`), and `dissoc` or `disj`
of one does nothing. So if a lookup on a file from another language comes back
`nil`, check that the key is a string, not a keyword.

Files from other languages must use the same file format as xitdb-clj, the one
xitdb-java has written since 0.46.0; files in an older format are rejected, like
those written by xitdb-clj 0.5.0 and earlier. Those old xitdb-clj files have
header `0`, the same as Java files, so without that check they would open in
compat mode, where lookups would miss and writes would create duplicates.
Migrate them as described above.

Not supported: Java `SortedMap`s, foreign format tags on values, binary values,
hashes other than SHA-1, a top-level hash map with no history, and read-only
opening.

Maps and sets written from native Clojure values are plain, not counted, so Java
reads them with `ReadHashMap` and `ReadHashSet`, and `count` on them walks the
whole collection. Editing an existing collection in place keeps its kind, but
replacing a Java-written counted one with a native value makes it plain. Maps and
sets in Clojure mode are still counted.

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

A value read from a database is a pointer into that database's storage, so it can
be written back into the *same* database without copying (this is what makes
reverting to an earlier version cheap, see History below), but it cannot be
written into a *different* database. Doing so throws an `IllegalArgumentException`;
`materialize` the value first to copy it:

```clojure
(reset! other-db (xdb/materialize (get @db :users)))
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

Supported key/member types are strings, keywords, booleans, chars, longs,
doubles, `UUID`, `Instant` and `Date`. They are stored with an order-preserving
codec, so they iterate in natural order — numeric for numbers, chronological
for temporals, lexicographic (by code point) for strings, `UUID.compareTo`
order for UUIDs. Longs and doubles share a single numeric ordering, so they
interleave by value (e.g. `1 < 1.5 < 2`). Only the default ordering is
supported: `sorted-map-by` / `sorted-set-by` with a custom comparator is
rejected, as are `java.util.Date` subclasses like `java.sql.Timestamp` (use
`Instant` instead).

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

It is also possible to create a transaction which returns the history count and the
previous and current values of the database, by setting the `*return-history?*` binding to `true`.
The history count is the `count` of the database after the transaction, so the version
it wrote is at `(dec history-count)`.

```clojure
;; Work with history tracking
(binding [xdb/*return-history?* true]
  (let [[history-count old-value new-value] (swap! db assoc :new-key "value")]
    (println "old value:" old-value)
    (println "new value:" new-value)
    (println "same as new value:" (xdb/deref-at db (dec history-count)))))
```

On a cursor, `swap!` and `reset!` return the value at the cursor's keypath, and the
previous and current values above are also the values at that keypath.

## Compaction

Normally, an immutable database grows forever, because old data is never deleted. To reclaim disk space and clear the history, xitdb supports compaction. This involves completely rebuilding the database file to only contain the data accessible from the latest copy (i.e., "moment") of the database.

```clojure
(with-open [compacted (xdb/compact db "compacted.xdb")]
  (println (xdb/materialize @compacted)))
```

The `compact` function takes either a file name or `:memory` and returns a new database rather than modifying the existing one. A file target must not already exist, because in-place compaction is not supported. If you want to delete the original database and replace it with the compacted one, you'll need to do that yourself.

Compaction holds the source database's write lock for the duration of the copy: reads continue, but any `swap!` or `reset!` on the source will block until compaction finishes. For the same reason, `compact` must not be called from inside a `swap!` or `reset!` on the database being compacted; it throws an `IllegalStateException` if it is.

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

## Replication

Since xitdb files are append-only, it is absurdly easy to replicate them: just copy the bytes that the destination doesn't have yet! Other databases like SQLite require complex external tools to replicate. In xitdb, once a transaction is committed, its bytes never change, except for a small header that points at the latest commit. That makes it easy to keep a copy of a database (a "replica") up to date, whether it's another file on the same machine or on another machine across a network.

With `xitdb.replication` loaded, databases work with `clojure.java.io`, so a sync is just an `io/copy`:

```clojure
(require '[clojure.java.io :as io]
         '[xitdb.replication :as replication])

(with-open [in  (io/input-stream source :since (replication/replica-state replica))
            out (io/output-stream replica)]
  (io/copy in out))
```

`replica-state` describes what the replica already has, as a map of two numbers. `io/input-stream` opens a stream of the bytes that the replica is missing, and `io/output-stream` writes them to the replica, committing them when it is closed. The output stream holds the replica's write lock until then, so close it on the thread that opened it (`with-open` does this).

Since the state is plain data and the bytes are an ordinary `InputStream`, replicating across a network works the same way. For example, the source can serve updates from a Ring handler, and the replica can apply them straight from the response:

```clojure
;; on the source (with ring.middleware.params/wrap-params)
(defn updates-handler [{:keys [query-params]}]
  {:status 200
   :body   (io/input-stream db :since {:committed-size (parse-long (query-params "size"))
                                       :checksum       (parse-long (query-params "checksum"))})})

;; on the replica
(let [{:keys [committed-size checksum]} (replication/replica-state replica)]
  (with-open [in (io/input-stream (str "http://example.com/updates?size=" committed-size "&checksum=" checksum))]
    (replication/replicate! replica in)))
```

`replicate!` does the same `io/copy`, but if the connection drops, it throws that error. With `with-open` around both streams, you would get the exception from closing the output stream on an incomplete update instead, which hides the cause. It returns `{:full-copy? b :replica-state s}`: whether the update replaced the replica with a full copy (see below), and the replica's new state.

Without `:since`, the stream contains the whole database, which is handy for copying it into a new database as a backup.

A replica can start out as any new database. Each database has a random file ID, so the first sync sees that the replica isn't a copy of the source and sends the entire source file, replacing the replica. The same happens if the replica can no longer be extended, such as when the source is replaced by a compacted copy (which is a new file) or restored from an older backup.

If an incremental sync is interrupted, such as by a dropped connection, closing the output stream throws and the replica stays at its last commit.

A full copy replaces the replica in place: the replica is truncated as the copy begins and the new bytes are written over it. While it runs, reads from the replica throw. Values read from the replica before it (lazy maps, vectors, cursors) hold offsets into the old file, so afterwards they read whatever the copy holds there: another value, garbage, or an error, with no way to tell. Don't use them after a full copy: `materialize` what you need to keep, or read it again after the sync. `replicate!` returns whether an update was a full copy.

If a full copy is interrupted, the replica can't be read until a sync succeeds. `replica-state` describes it as empty, so the next sync sends a full copy again. If the process restarts first, the file won't open: delete it and sync again, since a replica is only a copy. Don't replicate a database into itself or into another handle on the same file, and only replicate from databases that xitdb-clj can open (the key hash format is not checked). A full copy gives the replica the source's [mode](#compatibility-mode-sharing-files-with-other-languages).

If a replica must stay readable during a full copy, apply the copy to a new database and switch to it: ask the source whether it can extend the replica with `can-extend?` (over a network, the source's handler can return the answer), and if not, copy the source without `:since` into a new file, then swap files or handles in your app.

Treat a replica as read-only: don't `swap!` or `reset!` it. Once a replica has commits of its own, it is no longer a copy of the source, so the next sync replaces it with a full copy and those commits are lost.

### Architecture
`xitdb-clj` builds on [xitdb-java](https://github.com/xit-vcs/xitdb-java) which implements:

- **Hash Array Mapped Trie (HAMT)** - For hash map/set and vector
- **B-trees** - For sorted map/set and linked list
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
- **Lists** - B-tree-based linked array lists
- **Primitives** - Numbers, strings, keywords, booleans, dates.

## Performance Characteristics

- **Read Operations**: O(log16 n) for maps and vectors due to trie structure
- **Write Operations**: O(log16 n) with structural sharing for efficiency
- **Memory Usage**: Minimal overhead with automatic deduplication of identical subtrees
- **Concurrency**: Thread-safe with optimized read-write locks


## License

MIT
