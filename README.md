> **⚠️ Alpha Software - Work in Progress**
>
> This project is in early development and rapidly evolving. 
> Expect breaking changes, rough edges, and incomplete documentation.
>
> **🤝 Help Wanted!** If you find this useful, please consider contributing:
> - Report bugs and issues you encounter
> - Suggest improvements or new features
> - Submit pull requests for fixes or enhancements
> - Share your configuration patterns and workflows
> - Help improve documentation and examples
>
> Your feedback and contributions will help make this tool better for the entire Clojure community!

## Overview

`xitdb-clj` is a embedded database for efficiently storing and retrieving immutable, persistent data structures. 

It is a Clojure interface for [xitdb-java](https://github.com/radarroark/xitdb-java), 
itself a port of [xitdb](https://github.com/radarroark/xitdb), written in Zig.

`xitdb-clj` provides atom-like semantics when working with the database from Clojure.

[![Clojars Project](https://img.shields.io/clojars/v/io.github.codeboost/xitdb-clj.svg)](https://clojars.org/io.github.codeboost/xitdb-clj)

## Main characteristics
 
- Embeddable, tiny library.
- Supports writing to a file as well as purely in-memory use.
- Each transaction (done via `swap!`) efficiently creates a new "copy" of the database, and past copies can still be read from.
- Reading/Writing to the database is efficient, only the necessary nodes are read or written.
- Thread safe. Multiple readers, one writer.
- Append-only. The data you are writing is invisible to any reader until the very last step, when the top-level history header is updated.
- All heavy lifting done by the bare-to-the-jvm java library.
- Database files can be used from other languages, via [xitdb Java library](https://github.com/radarroark/xitdb-java) or the [xitdb Zig library](https://github.com/radarroark/xitdb)

## Architecture

`xitdb-clj` builds on [xitdb-java](https://github.com/radarroark/xitdb-java) which implements:

- **Hash Array Mapped Trie (HAMT)** - For efficient map and set operations
- **RRB Trees** - For vector operations with good concatenation performance
- **Structural Sharing** - Minimizes memory usage across versions
- **Copy-on-Write** - Ensures immutability while maintaining performance

The Clojure wrapper adds:
- Idiomatic Clojure interfaces (`IAtom`, `IDeref`)
- Automatic type conversion between Clojure and Java types
- Thread-local read connections for scalability
- Integration with Clojure's sequence abstractions

## Quickstart

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
One important distinction to the Clojure atom is that inside a transaction (eg. a `swap!`), 
'change' operations on the received db argument are mutating it.

```clojure
(with-db [db (xdb/xit-db :memory)]
  (reset! db {})
  (swap! db (fn [db]
              (let [db1 (assoc db :foo :bar)]
                (println "db1:" db1)
                (println "db:" db)))))
```
prints 
```
db1: {:foo :bar}
db: {:foo :bar}
```
As you can see, `(assoc db :foo :bar)` changed the value of `db`, in contrast
to how it works with a Clojure persistent map. This is because, inside `swap!`, 
`db` is referencing a WriteCursor, which writes the value to the underlying 
ArrayList or HashMap objects inside `xit-db-java`.
The value will actually be commited to the database when the `swap!` function returns.

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
(materialize (get-in @db [:users "alice"])) ;; => {:name "Alice" :age 31}
```

## No query language

Use `filter`, `group-by`, `reduce`, etc.
If you want a query engine, `datascript` works out of the box, you can store the datoms as a vector in the db.

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

## History
Since the database is immutable, all previous values are accessing by reading
from the respective `history index`.
The root data structure of a xitdb database is a ArrayList, called 'history'.
Each transaction adds a new entry into this array, which points to the latest value 
of the database (usually a map).
It is also possible to create a transaction which returns the previous and current 
values of the database, by setting the `*return-history?*` binding to `true`.

```clojure
;; Work with history tracking
(binding [xdb/*return-history?* true]
  (let [[history-index old-value new-value] (swap! db assoc :new-key "value")]
    (println "old value:" old-value)
    (println "new value:" new-value)))
```

### Supported Data Types
- **Maps** - Hash maps with efficient key-value access
- **Vectors** - Array lists with indexed access
- **Sets** - Hash sets with unique element storage
- **Lists** - Linked lists and RRB tree-based linked array lists
- **Primitives** - Numbers, strings, keywords, booleans, dates.

### Persistence Models
- **File-based** - Data persisted to disk with crash recovery
- **In-memory** - Fast temporary storage for testing or caching

## Examples

### User Management System

```clojure
(def user-db (xdb/xit-db "users.db"))

(reset! user-db {:users {}
                 :sessions {}
                 :settings {:max-sessions 100}})

;; Add a new user
(swap! user-db assoc-in [:users "user123"] 
       {:id "user123"
        :email "alice@example.com"
        :created-at (java.time.Instant/now)
        :preferences {:theme "dark" :notifications true}})

;; Create a session
(swap! user-db assoc-in [:sessions "session456"]
       {:user-id "user123"
        :created-at (java.time.Instant/now)
        :expires-at (java.time.Instant/ofEpochSecond (+ (System/currentTimeMillis) 3600))})

;; Update user preferences
(swap! user-db update-in [:users "user123" :preferences] 
       merge {:language "en" :timezone "UTC"})
```

### Configuration Store

```clojure
(def config-db (xdb/xit-db "app-config.db"))

(reset! config-db 
  {:database {:host "localhost" :port 5432 :name "myapp"}
   :cache {:ttl 3600 :max-size 1000}
   :features #{:user-registration :email-notifications :analytics}
   :rate-limits [{:path "/api/*" :requests-per-minute 100}
                 {:path "/upload" :requests-per-minute 10}]})

;; Enable a new feature
(swap! config-db update :features conj :real-time-updates)

;; Update database configuration
(swap! config-db assoc-in [:database :host] "db.production.com")
```

## Performance Characteristics

- **Read Operations**: O(log16 n) for maps and vectors due to trie structure
- **Write Operations**: O(log16 n) with structural sharing for efficiency
- **Memory Usage**: Minimal overhead with automatic deduplication of identical subtrees
- **Concurrency**: Thread-safe with optimized read-write locks


## License

MIT
