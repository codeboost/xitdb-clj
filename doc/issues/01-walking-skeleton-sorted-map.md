# Issue 1: Walking skeleton — string/keyword-keyed sorted map (read + write)

Type: AFK
Status: ready-for-agent

## Parent

[Sorted Map & Sorted Set PRD](../sorted-map-prd.md)

## What to build

The end-to-end walking skeleton that makes a persisted `sorted-map` a working,
ordered, on-disk Clojure collection — for **string and keyword keys only**.
This slice threads every integration layer once so the remaining slices can
extend it:

- A small **order-preserving key codec** (`xitdb.util.sorted-key`) with a stable
  1-byte type tag per key type. This slice implements the tag infrastructure plus
  the **string** and **keyword** encodings (UTF-8 bytes, which already sort in
  code-point order). Interface: `encode-key ^bytes [k]` and `decode-key [^bytes]`.
- **Construction detection**: `conversion/v->slot!` (and the nested writers)
  recognise `clojure.lang.PersistentTreeMap` and persist it as a `SORTED_MAP`.
  The tree-map branch must be checked **before** the generic `map?` branch, since
  a tree map is also a `map?`. If the tree map carries a non-default comparator,
  throw `IllegalArgumentException` (custom comparators are not supported).
- **Read dispatch**: `xitdb-types/read-from-cursor` returns `XITDBSortedMap` (read)
  or `XITDBWriteSortedMap` (write) for the `SORTED_MAP` tag, mirroring the
  existing `HASH_MAP` cases.
- **Wrapper types** (`xitdb.sorted-map`), modelled on `xitdb.hash-map`:
  - `XITDBSortedMap` (read): `ILookup`, `Associative`, `IPersistentMap`,
    `Counted`, `Seqable` (ascending ordered `seq`), `IFn`, `Iterable`,
    `IKVReduce`, plus `common/ISlot`/`IUnwrap`/`IMaterialize`/
    `IMaterializeShallow`. Read-only `assoc`/`dissoc`/`cons` materialise-shallow
    and return a plain Clojure `sorted-map`.
  - `XITDBWriteSortedMap` (write): mutating `assoc`/`without`/`cons`/`empty`
    against the live `WriteSortedMap`, plus `IReadOnly`.
- A **sorted-operations** namespace (`xitdb.util.sorted-operations`) bridging the
  types to the Java `Read/WriteSortedMap` (`put`/`remove`/`getCursor`/`count`/
  `iterator`, decoding keys on read).
- `print-method` for both types (ordered output, `#XITDBSortedMap`).

`Sorted`/`Indexed`/`Reversible` (subseq, nth, rseq) are intentionally deferred to
Issue 2. Numeric/temporal keys are deferred to Issue 3.

## Acceptance criteria

- [ ] `(reset! db (sorted-map "b" 2 "a" 1))` then `@db` seqs as `(["a" 1] ["b" 2])` in key order.
- [ ] `(get @db "a")`, `(@db "a")`, `(:k ...)`-style lookup, `(contains? @db "a")`, `(find @db "a")` all work.
- [ ] `(count @db)` is correct and O(1) (delegates to `ReadSortedMap.count()`).
- [ ] `(swap! db assoc "c" 3)` keeps order; `(swap! db dissoc "a")` removes and preserves order; re-assoc of an existing key replaces the value without changing count.
- [ ] Keyword keys round-trip to keywords and sort correctly; string keys round-trip to strings.
- [ ] `(sorted? @db)` is **not** required yet, but `(materialize @db)` returns a plain Clojure `sorted-map` with matching order.
- [ ] Read-only `assoc`/`dissoc` (outside a transaction) returns a plain Clojure sorted collection, not an `XITDB*` type — consistent with `XITDBHashMap`.
- [ ] Persisting a `sorted-map-by` with a custom comparator throws `IllegalArgumentException`.
- [ ] A sorted map nests inside a hash map value and round-trips; values may be vectors/maps/sets.
- [ ] `(tu/db-equal-to-atom? db)` style round-trip holds for a string/keyword-keyed sorted map.
- [ ] `print-method` renders ordered, distinguishable output.

## Blocked by

None - can start immediately.
