# Compaction

Normally, an immutable database grows forever, because old data is never deleted. To reclaim disk space and clear the history, xitdb supports compaction. This involves completely rebuilding the database file to only contain the data accessible from the latest copy (i.e., "moment") of the database.

```clojure
(with-open [compacted (xdb/compact db "compact.db")]
  (count compacted)                      ;; => 1
  (println (xdb/materialize @compacted)))
```

`compact` takes either a file name or `:memory` and returns a new, open database rather than modifying the existing one. A file target must not already exist, because in-place compaction is not supported. If you want to delete the original database and replace it with the compacted one, you'll need to do that yourself.

Compaction tracks the offsets of copied objects in a temporary on-disk xitdb file, so it does not run out of memory on large databases. It holds the source database's write lock for the duration of the copy: reads continue, but any `swap!` or `reset!` on the source will block until compaction finishes. For the same reason, `compact` must not be called from inside a `swap!` or `reset!` on the database being compacted; it throws `IllegalStateException` if it is.

## Evicting data without losing all history

You may be wondering if there is a way to evict certain pieces of data without losing all of your database's history. For example, imagine your moment contains a vector of people:

```clojure
{:people [{:name "Alice" :age 25}
          {:name "Bob" :age 42}]}
```

If Alice asks you to delete her data, you can remove her from the latest moment and compact the database. Her data will be gone, but so will all of your past moments, including every past version of Bob.

It's important to realize that the top-level history isn't the only place you can keep history. It's the only one compaction discards, so you are free to track history of specific things within your moment. The `:people` vector can contain vectors tracking the history of changes for each person individually.

Additionally, you can have a "global" internal history vector (`:versions` below), where each item is a vector of integers that are indexes into each person's history. Here is what the moment looks like after Bob turns 43:

```clojure
{:people   [[{:name "Alice" :age 25}]
            [{:name "Bob" :age 42} {:name "Bob" :age 43}]]
 :versions [[0 0]
            [0 1]]}
```

Each item in `:versions` is a snapshot of the database, with one integer per person that is an index into their list. For example, the last version says that Alice is `(get-in moment [:people 0 0])` and Bob is `(get-in moment [:people 1 1])`.

Compaction can't follow an integer, so `:people` is the only way to reach Alice's data. To evict her, you just clear her slot with `(swap! db assoc-in [:people 0] nil)`, compact the database, then delete the original. Every past version of Bob will still be there, but you'll need to handle the integers that no longer lead anywhere when reading a version.
