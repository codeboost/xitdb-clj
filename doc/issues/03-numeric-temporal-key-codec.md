# Issue 3: Numeric & temporal key codec — long, double, inst/date

Type: AFK
Status: ready-for-agent

## Parent

[Sorted Map & Sorted Set PRD](../sorted-map-prd.md)

## What to build

Extend the order-preserving key codec (`xitdb.util.sorted-key`) with tagged
encodings for the remaining v1 key types, so that numeric and temporal keys sort
in their natural order on disk. No wrapper-type changes are needed — the sorted
map (and later the sorted set) call `encode-key`/`decode-key`, so they gain these
key types automatically once the codec supports them.

Encodings (each carries its own type tag; the tag also defines a stable
cross-type order so heterogeneous keys never throw):

- **Long / integer** → tag + 8-byte big-endian with the **sign bit flipped**
  (XOR `0x80` on the top byte). Makes signed integers sort correctly as unsigned
  bytes: negatives before positives, ascending within each. (Same technique the
  Java library uses for its creation-time index example.)
- **Double** → tag + IEEE-754 8-byte big-endian with the order-preserving bit
  flip: if the sign bit is set, flip all bits; otherwise flip only the sign bit.
- **Instant** → tag + big-endian epoch encoding (e.g. epoch-second + nanos) so
  chronological order equals byte order; decodes back to `Instant`.
- **Date** → tag + big-endian epoch encoding (distinct tag from `Instant`);
  decodes back to `java.util.Date`.

This slice is the correctness-critical one and must ship with property-based
ordering tests (see Testing Decisions in the PRD).

## Acceptance criteria

- [ ] `(reset! db (sorted-map 9 :a 10 :b 1 :c))` iterates numerically as `1, 9, 10` (not lexically).
- [ ] Negative and positive long keys sort correctly together (e.g. `-5 < 0 < 3`), including `Long/MIN_VALUE` and `Long/MAX_VALUE`.
- [ ] Double keys sort numerically, including negatives, zero, and large magnitudes.
- [ ] `Instant` keys iterate in chronological order and round-trip to `Instant`; `Date` keys likewise round-trip to `Date`.
- [ ] Round-trip property: `(= k (decode-key (encode-key k)))` for every supported type.
- [ ] Order-preservation property (generative): for random same-type pairs `a`,`b`, `sign(compareUnsigned(encode a, encode b)) == sign(compare a b)`.
- [ ] Cross-type ordering is total and never throws.
- [ ] Unsupported key types throw a clear error.

## Blocked by

- [Issue 1: Walking skeleton — string/keyword-keyed sorted map](01-walking-skeleton-sorted-map.md)
