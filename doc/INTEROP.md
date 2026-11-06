# Sharing files with other languages

A xitdb file written by xitdb-clj can be read by the Java, TypeScript, Go and Zig libraries, and vice versa, as long as both sides agree on how keys are hashed. This document covers the two key-hashing modes, the rules for keys in compat mode, and migrating files from older versions of xitdb-clj.

* [Modes](#modes)
* [Opening and creating files](#opening-and-creating-files)
* [Keys in compat mode](#keys-in-compat-mode)
* [Counted and plain collections](#counted-and-plain-collections)
* [Not supported](#not-supported)
* [Upgrading from older versions](#upgrading-from-older-versions)

## Modes

Hash maps and sets look keys up by their SHA-1 hash. Files record their key hashing in a four-byte header ID, and xitdb-clj has two modes:

* **Clojure mode** (header `clj1`) hashes keys with a canonical, type-aware encoding, so any supported value can be a key: strings, keywords, numbers, `nil`, even vectors and maps. This is the default for new files. Other languages can read the values in such a file, but can't look its keys up.
* **Compat mode** (any other header, such as the `0` that xitdb-java writes by default) hashes string keys as their raw UTF-8 bytes, the way the Java and TypeScript libraries do, so both sides can read and write the same file.

## Opening and creating files

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

`compact` keeps the header, so a compacted database stays in its mode. A full copy during [replication](REPLICATION.md) brings the source's header, so the replica switches to the source's mode, even if it was opened with `:mode`.

## Keys in compat mode

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

## Counted and plain collections

Maps and sets written from native Clojure values are plain, not counted, so Java reads them with `ReadHashMap` and `ReadHashSet`, and `count` on them walks the whole collection. Editing an existing collection in place keeps its kind, but replacing a Java-written counted one with a native value makes it plain. Maps and sets in Clojure mode are still counted.

## Not supported

Files from other languages must use the same file format as xitdb-clj, the one xitdb-java has written since 0.46.0; files in an older format are rejected.

Not supported: foreign format tags on values, binary values, hashes other than SHA-1, a top-level hash map with no history, and read-only opening.

## Upgrading from older versions

Version 0.6.0 uses a new file format (from xitdb-java 0.46.0, which adds replication), so it can't open files written by 0.5.0 and earlier. Those old files have header `0`, the same as Java files, but are rejected in every mode because of the format, so they never open in compat mode, where lookups would miss and writes would create duplicates. Opening an incompatible file throws `IllegalArgumentException` and does not modify it.

To upgrade, export the data using the version that created the file (`materialize` reads entries without looking them up by hash), then import it into a **new** database with the current version. Export each `deref-at` value in order if history must be preserved. Use a serializer that preserves your value types and sorted collections; plain EDN does not round-trip every supported type. Compaction does not migrate files.

Compat files written by xitdb-clj 0.7.0 and 0.7.1 store sorted map keys and sorted set members with a leading type-tag byte (a space for strings), which this version reads as part of the key. Those sorted collections must be rewritten.
