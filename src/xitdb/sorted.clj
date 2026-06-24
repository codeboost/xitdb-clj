(ns xitdb.sorted
  "Public helpers for on-disk sorted collections (`XITDBSortedMap` /
  `XITDBSortedSet`) that go beyond `clojure.core`'s in-memory sorted
  collections, exposing the rank-augmented B-tree's superpowers:

    - `rank`  - O(log n) index of a key/member (inverse of indexed `nth`).
    - `page`  - lazy ordered page starting at a rank (offset/limit).
    - `from-index` - lazy ordered seq starting at a rank.

  These are the recommended way to build and paginate on-disk secondary indexes.
  For example, a timestamp -> id index over events:

      (reset! db (sorted-map))
      (doseq [e events]
        (swap! db assoc (:ts e) (:id e)))

      ;; serve the page [offset, offset+limit) in chronological order,
      ;; reading only that page from disk:
      (xsorted/page @db offset limit)

      ;; or, starting from a known timestamp boundary:
      (->> (subseq @db >= start-ts)
           (take limit))

  Both `rank` and the pagination helpers work on `XITDBSortedMap` (yielding
  `MapEntry` pairs) and `XITDBSortedSet` (yielding members)."
  (:require
    [xitdb.common :as common]
    [xitdb.util.sorted-operations :as sorted-ops])
  (:import
    [io.github.radarroark.xitdb ReadSortedMap ReadSortedSet]))

(defn rank
  "Number of entries in the sorted collection `coll` strictly less than `k`,
  in O(log n). For a present key/member this is its index (the inverse of
  `nth`); for an absent one it is the would-be insertion index. Works on both
  `XITDBSortedMap` and `XITDBSortedSet`."
  [coll k]
  (let [u (common/-unwrap coll)]
    (cond
      (instance? ReadSortedMap u) (sorted-ops/smap-rank u k)
      (instance? ReadSortedSet u) (sorted-ops/sset-rank u k)
      :else (throw (IllegalArgumentException.
                     (str "rank requires an XITDBSortedMap or XITDBSortedSet, got: "
                          (type coll)))))))

(defn from-index
  "Lazy ordered seq of the sorted collection `coll` starting at rank `n`
  (0-based), backed by the engine's `iteratorFromIndex` (O(log n) seek, then a
  streaming walk). Does not materialise the whole collection. For a sorted map
  the elements are `MapEntry` pairs; for a sorted set they are members. Returns
  nil when `n` is at or past the end."
  [coll n]
  (when (neg? n)
    (throw (IllegalArgumentException.
             (str "from-index requires a non-negative rank, got: " n))))
  (let [u (common/-unwrap coll)]
    (cond
      (instance? ReadSortedMap u)
      (sorted-ops/smap-seq-from-index u common/-read-from-cursor n)
      (instance? ReadSortedSet u)
      (sorted-ops/sset-seq-from-index u n)
      :else (throw (IllegalArgumentException.
                     (str "from-index requires an XITDBSortedMap or XITDBSortedSet, got: "
                          (type coll)))))))

(defn page
  "Lazy ordered page of `coll`: at most `limit` elements starting at rank
  `offset`. Equivalent to `(take limit (from-index coll offset))`, and stops
  cleanly at the end of the collection. Lazy and low-memory. For a sorted map
  the elements are `MapEntry` pairs; for a sorted set they are members."
  [coll offset limit]
  (take limit (from-index coll offset)))
