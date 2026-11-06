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

Hash maps store their contents in hash order, which is meaningless to a human. Real apps need to show data in a sensible order (such as users listed alphabetically) one page at a time. In xitdb there are no built-in indexes, so you build and maintain them yourself. That's a little more code, but the index is just another data structure: a `sorted-map` whose keys sort the way you want, kept in sync by writing to it in the same transaction that writes the primary data.

Store a Clojure `sorted-map` or `sorted-set` and it is persisted as an on-disk sorted collection, backed by the engine's rank-augmented B-tree. Reading it back yields an `XITDBSortedMap` / `XITDBSortedSet` that implements Clojure's ordered interfaces, so `seq`, `rseq`, `nth`, `subseq` and `rsubseq` all work and read only what they touch:

```clojure
(reset! db (sorted-map "banana" 2 "apple" 1 "cherry" 3))

@db
;; => #XITDBSortedMap{"apple" 1, "banana" 2, "cherry" 3}

(swap! db assoc "date" 4)  ;; inserted in order, not appended
(subseq @db >= "c")        ;; => (["cherry" 3] ["date" 4])
(nth @db 1)                ;; => ["banana" 2]   (O(log n), no scan)
```

Supported key and member types are strings, keywords, booleans, chars, longs, doubles, `UUID`, `Instant` and `Date`, stored with an order-preserving codec so they iterate in natural order. Only the default ordering is supported: `sorted-map-by` / `sorted-set-by` with a custom comparator is rejected.

The `xitdb.sorted` namespace exposes the B-tree's O(log n) rank operations, which are what make paging an index cheap: `page` finds its offset without scanning the entries it skips, so jumping to page 500 is just as cheap as page 1.

```clojure
(require '[xitdb.sorted :as xsorted])

(xsorted/rank @db "cherry")   ;; => 2, the index of the key
(xsorted/page @db 1 2)        ;; => (["banana" 2] ["cherry" 3]), ranks [1, 3)
(xsorted/from-index @db 2)    ;; => lazy ordered seq from rank 2
```

See [doc/SORTING.md](doc/SORTING.md) for a worked example: a user directory with a primary hash map and a secondary sorted index, paged with `page` and searched by prefix with `subseq`, plus indexes on non-unique keys and grouping indexes.

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

Hash maps and sets look keys up by their SHA-1 hash, and a file's header records how its keys are hashed. xitdb-clj has two modes:

* **Clojure mode** (header `clj1`) hashes keys with a canonical, type-aware encoding, so any supported value can be a key: strings, keywords, numbers, `nil`, even vectors and maps. This is the default for new files. Other languages can read the values in such a file, but can't look its keys up.
* **Compat mode** (any other header, such as the `0` that xitdb-java writes by default) hashes string keys as their raw UTF-8 bytes, the way the Java and TypeScript libraries do, so both sides can read and write the same file. Map keys, set members and sorted keys must be strings.

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

See [doc/INTEROP.md](doc/INTEROP.md) for the `:mode` option on existing files, the string-key rules for hash and sorted collections, counted versus plain maps, what is not supported, and migrating files written by xitdb-clj 0.5.0 and earlier, which use an older file format.

## Compaction

Normally, an immutable database grows forever, because old data is never deleted. To reclaim disk space and clear the history, xitdb supports compaction. This involves completely rebuilding the database file to only contain the data accessible from the latest copy (i.e., "moment") of the database.

```clojure
(with-open [compacted (xdb/compact db "compact.db")]
  (count compacted)                      ;; => 1
  (println (xdb/materialize @compacted)))
```

`compact` takes either a file name or `:memory` and returns a new, open database rather than modifying the existing one. A file target must not already exist, because in-place compaction is not supported. If you want to delete the original database and replace it with the compacted one, you'll need to do that yourself.

Compaction holds the source database's write lock for the duration of the copy: reads continue, but any `swap!` or `reset!` on the source will block until compaction finishes. For the same reason, `compact` must not be called from inside a `swap!` or `reset!` on the database being compacted; it throws `IllegalStateException` if it is.

Compaction discards the whole top-level history. See [doc/COMPACTION.md](doc/COMPACTION.md) for keeping history of individual things inside the moment, so you can evict one person's data without losing every past version of everyone else.

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
