# Issue 4: Sorted set end-to-end — `XITDBSortedSet` / `XITDBWriteSortedSet`

Type: AFK
Status: ready-for-agent

## Parent

[Sorted Map & Sorted Set PRD](../sorted-map-prd.md)

## What to build

The set counterpart to the sorted map: persist a `clojure.lang.PersistentTreeSet`
as an on-disk `SORTED_SET` and expose it as a fully ordered Clojure set. Reuses
the key codec (Issues 1 + 3) and the `Sorted`/`Indexed`/`Reversible` machinery
established for the map (Issue 2).

- **Construction detection**: `conversion/v->slot!` recognises
  `clojure.lang.PersistentTreeSet` (checked before the generic `set?` branch) and
  writes a `SORTED_SET`. Reject non-default comparators with `IllegalArgumentException`.
- **Read dispatch**: `read-from-cursor` returns `XITDBSortedSet` /
  `XITDBWriteSortedSet` for the `SORTED_SET` tag.
- **Wrapper types** (`xitdb.sorted-set`), modelled on `xitdb.hash-set`:
  - `XITDBSortedSet` (read): `IPersistentSet` (`contains?`/`get`/`disjoin`),
    `Counted`, `Seqable` (ordered), `IFn`, `Iterable`, plus `ISlot`/`IUnwrap`/
    `IMaterialize`/`IMaterializeShallow`, **and** `Sorted`/`Indexed`/`Reversible`
    so `subseq`/`rsubseq`/`rseq`/`nth`/`sorted?` work over the set. Read-only
    `conj`/`disj` return a plain Clojure `sorted-set`.
  - `XITDBWriteSortedSet` (write): mutating `conj`/`disjoin`/`empty` against the
    live `WriteSortedSet`, plus `IReadOnly`.
- Set operations in `xitdb.util.sorted-operations` over the Java
  `Read/WriteSortedSet` (`put`/`remove`/`contains`/`count`/`iterator`/
  `getIndexKeyValuePair`), decoding members on read.
- `print-method` (`#XITDBSortedSet`, ordered).
- `materialize` returns a plain Clojure `sorted-set` with matching order.

## Acceptance criteria

- [ ] `(reset! db (sorted-set 3 1 2))` then `@db` seqs as `(1 2 3)`.
- [ ] `(contains? @db 2)` works; `(get @db 2)` returns the member.
- [ ] `(swap! db conj 5)` and `(swap! db disj 1)` keep order; adding a duplicate is a no-op and does not change count.
- [ ] `(count @db)` is correct and O(1).
- [ ] `(sorted? @db)` is `true`; `(subseq @db >= 2)`, `(rsubseq ...)`, `(nth @db 0)`, `(rseq @db)` all match the plain-Clojure `sorted-set` oracle.
- [ ] String, keyword, long, double, and inst/date members each iterate in correct natural order.
- [ ] Read-only `conj`/`disj` (outside a transaction) returns a plain Clojure sorted set, not an `XITDB*` type.
- [ ] `(materialize @db)` returns a plain `sorted-set` with matching order.
- [ ] A sorted set nests inside other structures and round-trips.

## Blocked by

- [Issue 2: `clojure.lang.Sorted` for the sorted map](02-sorted-protocol-map.md)
- [Issue 3: Numeric & temporal key codec](03-numeric-temporal-key-codec.md)
