# Issue 5: `rank` + pagination public helpers

Type: AFK
Status: ready-for-agent

## Parent

[Sorted Map & Sorted Set PRD](../sorted-map-prd.md)

## What to build

Expose the rank-augmented B-tree "superpowers" that go beyond `clojure.core`'s
in-memory sorted collections, as a small public surface usable on both
`XITDBSortedMap` and `XITDBSortedSet`.

- **`rank`** — given a key/member, return the number of entries strictly less
  than it (its index), in O(log n). Backed by `ReadSortedMap.rank` /
  `ReadSortedSet.rank`. It is the inverse of indexed `nth`.
- **Pagination helper** — an offset/limit (or "from index N, take K") accessor
  backed by `ReadSortedMap.iteratorFromIndex` / `iteratorFromIndex`, returning a
  lazy ordered seq starting at a rank. This makes serving ordered, paged queries
  from disk efficient (the motivating secondary-index use case in the PRD).

Place these in a stable namespace (e.g. `xitdb.sorted` or extend `xitdb.db`)
and document them as the recommended way to build/paginate on-disk secondary
indexes.

## Acceptance criteria

- [ ] `(rank m k)` returns the correct index for present keys/members, and the would-be insertion index for absent ones, in O(log n).
- [ ] `rank` and indexed `nth` are inverses: `(= i (rank m (key (nth m i))))` for all `i`.
- [ ] The pagination helper returns the correct ordered page for a given offset/limit and stops at the end of the collection.
- [ ] Both helpers work on `XITDBSortedMap` and `XITDBSortedSet`.
- [ ] Pagination is lazy and does not materialise the whole collection.
- [ ] A doc example shows building a timestamp→id secondary index and paging through it.

## Blocked by

- [Issue 1: Walking skeleton — string/keyword-keyed sorted map](01-walking-skeleton-sorted-map.md)
- [Issue 4: Sorted set end-to-end](04-sorted-set.md)
