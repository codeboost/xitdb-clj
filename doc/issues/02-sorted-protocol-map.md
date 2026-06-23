# Issue 2: `clojure.lang.Sorted` for the sorted map — subseq / rsubseq / rseq / nth / sorted?

Type: AFK
Status: ready-for-agent

## Parent

[Sorted Map & Sorted Set PRD](../sorted-map-prd.md)

## What to build

Make `XITDBSortedMap` a fully sorted Clojure collection by implementing the
three interfaces that `clojure.core` builds its ordered operations on, so
`sorted?`, `subseq`, `rsubseq`, `rseq`, and indexed `nth` all work against disk.

- `clojure.lang.Sorted`:
  - `comparator` → a comparator consistent with the codec's natural ordering, so
    `subseq`'s own bound checks agree with the engine.
  - `entryKey` → `key` of the MapEntry.
  - `seq(ascending?)` → ascending uses `iterator()`; descending uses a
    rank-based index walk (there is no native reverse iterator).
  - `seqFrom(k, ascending?)` → ascending maps directly to
    `ReadSortedMap.iteratorFrom(encode k)` (native O(log n) lower-bound seek);
    descending uses `rank(encode k)` + a descending `getIndexKeyValuePair` walk.
- `clojure.lang.Indexed`:
  - `nth(i)` / `nth(i, not-found)` → `getIndexKeyValuePair(i)` returning a
    MapEntry (decode key, read value). Support negative indices per Java
    semantics (`-1` = last).
- `clojure.lang.Reversible`:
  - `rseq` → descending lazy seq (index walk from `count-1` down).

Descending seqs must stay lazy and low-memory (step via `getIndexKeyValuePair`,
do not materialise the whole map).

## Acceptance criteria

- [ ] `(sorted? @db)` returns `true` for a persisted sorted map.
- [ ] `(subseq @db >= k)`, `> k`, `<= k`, `< k`, and the two-bound form all return the same entries (in order) as the equivalent plain-Clojure `sorted-map` oracle.
- [ ] `(rsubseq @db ...)` mirrors the plain-Clojure oracle for all test/bound forms.
- [ ] `(seq @db)` is ascending; `(rseq @db)` is descending; both lazy.
- [ ] `(nth @db i)` returns the entry at rank `i` in O(log n); `(nth @db -1)` returns the last entry; out-of-range honours `not-found`/throws like a vector.
- [ ] `subseq`/`rsubseq` on an empty (none-cursor) sorted map yield nothing.
- [ ] `(comparator @db)` is consistent with iteration order (subseq bound filtering agrees with engine order).

## Blocked by

- [Issue 1: Walking skeleton — string/keyword-keyed sorted map](01-walking-skeleton-sorted-map.md)
