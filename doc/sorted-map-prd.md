# PRD: Sorted Map & Sorted Set support for xitdb-clj

Status: ready-for-implementation
Date: 2026-06-23

## Problem Statement

As a user of xitdb-clj, I can persist hash maps, hash sets, array lists and
linked lists, but I have no way to keep keys (or set members) **in order** on
disk. When I need range queries, ordered iteration, pagination, or "the entry
at position N", I have to load the whole collection into memory and sort it in
Clojure on every read. That defeats the point of an embedded, immutable,
on-disk database — and it does not scale to large collections or secondary
indexes (e.g. "all posts created between T1 and T2, page 3").

The upstream Java library (`io.github.radarroark.xitdb`) now ships a
rank-augmented B-tree exposed as `SortedMap` and `SortedSet`. xitdb-clj has no
Clojure types that wrap them, so none of this capability is reachable from
Clojure today.

## Solution

Add two new pairs of wrapper types — `XITDBSortedMap` / `XITDBWriteSortedMap`
and `XITDBSortedSet` / `XITDBWriteSortedSet` — that wrap the Java
`ReadSortedMap`/`WriteSortedMap` and `ReadSortedSet`/`WriteSortedSet`. These
behave like first-class Clojure sorted collections: they implement
`clojure.lang.Sorted`, so `sorted?`, `subseq`, `rsubseq`, `seq`, and `rseq`
work out of the box, and they additionally implement `clojure.lang.Indexed`
(O(log n) `nth` by rank) and `clojure.lang.Reversible`.

Construction is fully idiomatic and requires **no new public API**: when a value
written to the database is a `clojure.lang.PersistentTreeMap` (i.e. a
`sorted-map`) or a `clojure.lang.PersistentTreeSet` (a `sorted-set`), xitdb-clj
persists it as an on-disk `SORTED_MAP` / `SORTED_SET`. Reading it back returns
the corresponding `XITDBSorted*` type. So:

```clojure
(reset! db (sorted-map 3 :c 1 :a 2 :b))
(subseq @db >= 2)        ;; => ([2 :b] [3 :c])
(nth @db 0)              ;; => [1 :a]   ; O(log n), not O(n)
(rseq @db)               ;; => ([3 :c] [2 :b] [1 :a])
(sorted? @db)            ;; => true
```

The single honest limitation: ordering is the engine's fixed natural ordering
(produced by an order-preserving key codec). **Custom comparators
(`sorted-map-by` / `sorted-set-by`) are not supported** — the comparison lives
in the Java B-tree as unsigned byte comparison, not in a pluggable Clojure fn.

## User Stories

1. As a developer, I want to write a `(sorted-map ...)` to the db, so that it is
   persisted as an ordered on-disk structure without me learning a new API.
2. As a developer, I want to write a `(sorted-set ...)` to the db, so that set
   members are kept in sorted order on disk.
3. As a developer, I want `(sorted? db-value)` to return `true` for a persisted
   sorted map/set, so that generic code can detect orderedness.
4. As a developer, I want to call `(subseq m >= k)`, `(subseq m > k)`,
   `(subseq m <= k)`, `(subseq m < k)` and the two-bound form, so that I can run
   ascending range queries directly against disk.
5. As a developer, I want `(rsubseq m ...)` with the same test/bound forms, so
   that I can run descending range queries.
6. As a developer, I want `(seq m)` to iterate entries in ascending key order, so
   that ordered traversal is the default.
7. As a developer, I want `(rseq m)` to iterate entries in descending key order,
   so that I can walk from the largest key down.
8. As a developer, I want `(nth m i)` to return the entry at rank `i` in
   O(log n), so that positional access and pagination are cheap even for large
   maps.
9. As a developer, I want `(nth m -1)`/negative indexing semantics surfaced via a
   helper, so that I can get the last/last-k entries without counting.
10. As a developer, I want `(get m k)` / `(m k)` / `(:k m)` lookups, so that a
    sorted map is a drop-in associative read.
11. As a developer, I want `(contains? m k)` and `(find m k)`, so that presence
    checks and entry retrieval work like any map.
12. As a developer, I want `(count m)` to be O(1), so that size checks are cheap.
13. As a developer, I want to `(swap! db assoc k v)` a sorted map inside a
    transaction, so that inserts keep the structure ordered and persistent.
14. As a developer, I want to `(swap! db dissoc k)` a sorted map, so that I can
    remove keys while preserving order.
15. As a developer, I want `(swap! db conj v)` / `(swap! db disj v)` on a sorted
    set, so that membership edits preserve order.
16. As a developer, I want re-assoc'ing an existing key to replace the value and
    not change the count, so that updates behave like a normal map.
17. As a developer, I want string and keyword keys to sort in their natural
    (code-point) order, so that text indexes read correctly.
18. As a developer, I want long/integer keys to sort numerically (so `9 < 10`,
    and negatives before positives), so that numeric indexes behave intuitively.
19. As a developer, I want double keys to sort numerically, so that floating
    point indexes are ordered correctly.
20. As a developer, I want `java.time.Instant` and `java.util.Date` keys to sort
    chronologically, so that I can build time-ordered secondary indexes.
21. As a developer, I want keys to round-trip to their exact original Clojure
    type on read, so that `(keys m)` and entry keys are not stringly-typed.
22. As a developer, I want to build a timestamp→id secondary index and paginate
    it (offset/limit) efficiently, so that I can serve ordered, paged queries
    from disk.
23. As a developer, I want a `rank` operation ("how many keys are strictly less
    than k") in O(log n), so that I can compute a key's position / build
    pagination cursors.
24. As a developer, I want sorted maps/sets to nest inside other structures
    (e.g. a hash map whose value is a sorted map), so that I can model rich
    documents.
25. As a developer, I want a sorted map value to nest arbitrary values (vectors,
    maps, sets) as its values, so that the value side is as flexible as a hash
    map's.
26. As a developer, I want `(materialize sorted-map-value)` to return a plain
    Clojure `sorted-map` with the same ordering, so that I can fully realise it
    in memory.
27. As a developer, I want `(empty sorted-map)` semantics to produce an empty
    ordered structure, so that clearing works in a transaction.
28. As a developer, I want `=` / `equiv` to compare a persisted sorted map to a
    plain Clojure map by contents, so that test assertions read naturally.
29. As a developer reading the result of `assoc`/`dissoc` on a **read-only**
    sorted map (outside a transaction), I want a plain Clojure sorted collection
    back, so that the immutable-read contract matches the existing hash
    map/set types.
30. As a developer, I want a clear, early error if I try to persist a
    `sorted-map-by`/`sorted-set-by` with a custom comparator, so that I am not
    silently given a different ordering.
31. As a developer, I want a clear error if I use an unsupported key type, so
    that I fail fast instead of getting corrupt ordering.
32. As a developer, I want the print representation of a persisted sorted map/set
    to be distinguishable (e.g. `#XITDBSortedMap{...}`) and ordered, so that REPL
    output is legible.
33. As a developer using multiple threads, I want sorted-map reads to work from
    reader threads like the other types, so that concurrency behaves consistently.

## Implementation Decisions

### Construction trigger (no new public API)

- `conversion/v->slot!` and the nested writers (`coll->ArrayListCursor!`,
  `map->WriteHashMapCursor!`, etc.) gain branches that detect
  `clojure.lang.PersistentTreeMap` and `clojure.lang.PersistentTreeSet` **before**
  the generic `map?` / `set?` branches (a tree map is also a `map?`, so order of
  checks matters). These write `SORTED_MAP` / `SORTED_SET` respectively.
- If the detected tree map/set carries a **non-default comparator**, throw
  `IllegalArgumentException` ("custom comparators are not supported; sorted
  collections use natural ordering"). Detection: compare `.comparator` against
  `clojure.lang.RT/DEFAULT_COMPARATOR` / `compare`.

### Read dispatch

- `xitdb-types/read-from-cursor` gains `SORTED_MAP` and `SORTED_SET` cases that
  return `XITDBSortedMap`/`XITDBSortedSet` (read) or the `Write` variants when
  `for-writing?` is true — mirroring the existing `HASH_MAP`/`HASH_SET` cases.

### New module: key codec (deep module — the heart of this PRD)

A new namespace (e.g. `xitdb.util.sorted-key`) provides a bijective,
**order-preserving** encoding between supported Clojure key values and `byte[]`,
such that `Arrays.compareUnsigned(encode(a), encode(b))` has the same sign as the
natural ordering of `a` and `b`.

- Interface (small, stable):
  - `encode-key ^bytes [k]` — Clojure key → order-preserving bytes.
  - `decode-key [^bytes b]` — bytes → original Clojure key (exact type).
- Each encoding is prefixed with a 1-byte **type tag** that also defines a stable
  cross-type ordering, so heterogeneous keys never throw (a strict improvement
  over Clojure's `compare`, which throws across classes). Even though v1's
  primary contract is single-type maps, the tag makes the encoding total.
- Supported key types for v1 (per decision):
  - **String** → tag + UTF-8 bytes. UTF-8 byte order equals Unicode code-point
    order, so no transformation needed.
  - **Keyword** → tag + UTF-8 of the (namespace-qualified) name. Reuses
    `conversion/keyname`.
  - **Long / integer** → tag + 8-byte big-endian with the **sign bit flipped**
    (XOR `0x80` on the top byte), which makes signed integers sort correctly as
    unsigned bytes (negatives < positives, ascending within each). This is the
    same big-endian-with-flipped-sign technique used in the Java library's own
    `testSortedMap` example for a creation-time index.
  - **Double** → tag + IEEE-754 8-byte big-endian with the order-preserving bit
    flip (if sign bit set, flip all bits; else flip only the sign bit). Handles
    negative/positive ordering. (NaN handling: documented as undefined / rejected.)
  - **Instant / Date** → tag + big-endian epoch encoding (e.g. epoch-second +
    nanos, or epoch-milli) so chronological order = byte order. `Date` decodes
    back to `Date`, `Instant` back to `Instant` (distinct tags).
- The codec is the single place ordering correctness lives; it is pure
  (no DB handle needed) and unit-testable in isolation.

> Note: this is intentionally **separate** from the existing
> `conversion/db-key-hash`, which SHA-1-hashes keys for hash maps. Hashing
> destroys order and identity; sorted keys must be stored as their
> order-preserving bytes and recovered via the key cursor.

### New module: sorted-map operations

A namespace parallel to `xitdb.util.operations` (e.g. `xitdb.util.sorted-operations`)
holding the imperative bridge between the wrapper types and the Java API:

- `sorted-map-assoc-value!` — `encode-key` then `WriteSortedMap.putCursor(key)`
  + write value slot via `conversion/v->slot!`.
- `sorted-map-dissoc-key!` — `WriteSortedMap.remove(encoded)`.
- `sorted-map-get-cursor` — `ReadSortedMap.getCursor(encoded)` (nil when absent).
- `sorted-map-contains?` — non-nil cursor / `getKeyValuePair`.
- `sorted-map-count` — `ReadSortedMap.count()` (O(1)).
- `sorted-map-rank` — `ReadSortedMap.rank(encoded)`.
- `sorted-map-nth` — `ReadSortedMap.getIndexKeyValuePair(i)` → MapEntry
  (decode key, read value); supports negative indices per Java semantics.
- `sorted-map-seq` — lazy seq of `MapEntry` from `iterator()` (ascending),
  decoding keys.
- `sorted-map-seq-from` — ascending lazy seq from `iteratorFrom(encoded)`.
- `sorted-map-rseq` / descending-from — built on `rank` + descending
  `getIndexKeyValuePair` walk (no native reverse iterator exists; this is the
  agreed implementation strategy). Lazy and low-memory.
- Set variants (`sorted-set-*`) over `ReadSortedSet`/`WriteSortedSet`
  (`put`/`remove`/`contains`/`rank`/`getIndexKeyValuePair`/iterators); members
  are decoded keys, no value side.

### New module: the wrapper types

`xitdb.sorted-map` and `xitdb.sorted-set`, modelled on `xitdb.hash-map` /
`xitdb.hash-set`.

- `XITDBSortedMap` (read) implements:
  - `clojure.lang.ILookup`, `Associative`, `IPersistentMap`,
    `IPersistentCollection`, `Counted`, `Seqable`, `IFn`, `Iterable`,
    `IKVReduce`, plus `common/ISlot` / `IUnwrap` / `IMaterialize` /
    `IMaterializeShallow`.
  - `clojure.lang.Sorted`: `comparator`, `entryKey`, `seq(ascending?)`,
    `seqFrom(k, ascending?)` — this is what powers `subseq`/`rsubseq`/`sorted?`.
  - `clojure.lang.Indexed`: `nth` → rank-based `getIndexKeyValuePair`.
  - `clojure.lang.Reversible`: `rseq`.
  - Read-only `assoc`/`dissoc`/`cons` materialise-shallow then operate (return a
    plain Clojure `sorted-map`), matching `XITDBHashMap` behaviour.
- `XITDBWriteSortedMap` (write) implements the mutating `assoc`/`without`/`cons`/
  `empty` against the live `WriteSortedMap`, plus `IReadOnly`, mirroring
  `XITDBWriteHashMap`.
- `XITDBSortedSet` / `XITDBWriteSortedSet` analogously implement
  `IPersistentSet` + `Sorted` + `Indexed` + `Reversible`.
- `print-method` registered for each, ordered output, distinct tags
  (`#XITDBSortedMap` / `#XITDBSortedSet`).

### `clojure.lang.Sorted` contract mapping (the load-bearing detail)

- `seqFrom(k, true)` → `iteratorFrom(encode k)` (native O(log n) lower bound).
- `seqFrom(k, false)` → `rank(encode k)` then descending index walk.
- `seq(true)` → `iterator()`; `seq(false)` → descending index walk.
- `comparator` → a comparator consistent with the codec's natural ordering
  (so `subseq`'s own bound checks agree with the engine).
- `entryKey` → `key` of the MapEntry (map) / identity (set).

### Public surface for the "superpowers"

Expose, from a stable namespace (e.g. `xitdb.db` or a new `xitdb.sorted`):
- `rank` — key → index, O(log n).
- (Optional) a `nth`/`get-by-index` convenience already covered by `Indexed`.
- (Optional) a paginate helper built on `iteratorFromIndex`.

## Testing Decisions

Good tests here verify **external behavior** — what a user observes through the
Clojure collection API — not the internal byte layout of the B-tree or the
private shape of the operations namespace. Assertions compare against plain
Clojure `sorted-map`/`sorted-set` built with the same data, which is the
ground-truth oracle for ordering.

Prior art to mirror:
- `test/xitdb/set_test.clj` and `test/xitdb/map_test.clj` — `with-db` fixture,
  `reset!`/`swap!`, `(tu/db-equal-to-atom? db)` round-trip checks,
  read-only-return-type assertions.
- `test/xitdb/data_types_test.clj` — per-key-type round-tripping.
- `test/xitdb/generated_data_test.clj` / `gen_map.clj` — generative coverage.

Modules and what to test:

1. **Key codec (`xitdb.util.sorted-key`)** — the priority for unit tests, in
   isolation, no DB:
   - Round-trip: `(= k (decode-key (encode-key k)))` for each supported type.
   - **Order preservation (property-based)**: for random pairs `a`, `b` of the
     same type, `sign(compareUnsigned(encode a, encode b)) = sign(compare a b)`.
     Cover negatives, zero, large longs, `Long/MIN_VALUE`/`MAX_VALUE`, negative
     and positive doubles, sub-second instants.
   - Cross-type total ordering is stable (no exceptions across types).
   - Unsupported type / custom-comparator → throws.
2. **Sorted map/set integration tests** (with `with-db`), mirroring the Java
   `testSortedMap` scenario and the existing set/map tests:
   - Build from `(sorted-map ...)` / `(sorted-set ...)`; `@db` equals the plain
     sorted collection (order-sensitive comparison).
   - `subseq`/`rsubseq` for all six test/bound forms vs. the plain-Clojure oracle.
   - `seq` ascending, `rseq` descending, `nth` (including negative), `count` O(1).
   - `assoc`/`dissoc` in a `swap!` keep order; re-assoc replaces without changing
     count; `disj`/`conj` on the set.
   - Key-type matrix: string, keyword, long, double, inst/date keys each iterate
     in correct natural order.
   - `sorted?` is true; `materialize` returns a plain `sorted-map` with matching
     order; read-only `assoc`/`dissoc` returns a plain Clojure sorted collection.
   - `rank` returns correct positions and is the inverse of `nth`.
   - Empty / none-cursor cases: `subseq` and iteration on an empty sorted map
     yield nothing.
   - Nesting: sorted map as a value inside a hash map, and rich values inside a
     sorted map.
3. **Multi-threaded read** — a light check that reader threads can `subseq`/read a
   sorted map, consistent with `multi_threaded_test.clj`.

Generative tests (`test.check`) are the recommended vehicle for the codec
ordering property and for "insert a random key set, assert iteration order ==
`(sort ...)`".

## Out of Scope

- **Custom comparators** (`sorted-map-by` / `sorted-set-by`). The engine's order
  is fixed; custom comparators are rejected with a clear error, not supported.
- Key types beyond strings, keywords, longs, doubles, `Instant`/`Date` in v1
  (e.g. booleans, `nil`, `BigInteger`/`BigDecimal`, vectors/tuples as keys,
  `ratio`). These can be added later by extending the codec's tag table.
- A native streaming **reverse iterator** in the Java layer — descending is
  implemented via rank + index walk in Clojure; we do not modify the Java lib.
- Changing how hash maps/sets are stored or their key hashing.
- A bespoke public constructor API (decision: reuse `sorted-map`/`sorted-set`).

## Further Notes

- **Why the codec is the risk center**: every ordering guarantee in the feature
  reduces to "does `encode-key` preserve order". It is pure and isolated
  specifically so it can be proven correct independently before the wrapper types
  are trusted. De-risking the codec first (round-trip + property tests) is the
  recommended build order.
- **Headline win over `clojure.core`**: `nth`/positional access and `rank` are
  O(log n) here, whereas Clojure's in-memory `sorted-map` is O(n) for positional
  access. Combined with `iteratorFromIndex`, this makes `XITDBSortedMap` an
  excellent fit for **on-disk secondary indexes** with efficient pagination — the
  motivating use case demonstrated in the Java library's own tests.
- **Check ordering of type checks** in `v->slot!`: `PersistentTreeMap` satisfies
  `map?` and `PersistentTreeSet` satisfies `set?`, so the sorted branches must be
  evaluated first or the generic hash branches will shadow them.
- **UTF-8 ordering caveat**: UTF-8 byte order matches Unicode code-point order,
  which matches Clojure `compare` on strings for the entire BMP and beyond except
  for the surrogate-pair region edge cases; this is acceptable and should be
  noted in docs. ASCII keys (the common case) are exact.
