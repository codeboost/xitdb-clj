# Sorted Map & Sorted Set — implementation issues

Tracer-bullet slices for [the PRD](../sorted-map-prd.md). Each is a thin vertical
slice through every layer (codec → construction detection → read dispatch →
wrapper type → tests) and is independently verifiable. All are AFK.

| # | Slice | Blocked by |
|---|-------|-----------|
| [1](01-walking-skeleton-sorted-map.md) | Walking skeleton: string/keyword-keyed sorted map (read + write) | — |
| [2](02-sorted-protocol-map.md) | `clojure.lang.Sorted` for the map — subseq/rsubseq/rseq/nth/sorted? | 1 |
| [3](03-numeric-temporal-key-codec.md) | Numeric & temporal key codec — long, double, inst/date | 1 |
| [4](04-sorted-set.md) | Sorted set end-to-end (`XITDBSortedSet`/`XITDBWriteSortedSet`) | 2, 3 |
| [5](05-rank-and-pagination.md) | `rank` + pagination public helpers | 1, 4 |

## Suggested order

1 → (2 and 3 in parallel) → 4 → 5
