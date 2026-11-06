# Replication

Since xitdb files are append-only, it is absurdly easy to replicate them: just
copy the bytes that the destination doesn't have yet! Other databases like SQLite
require complex external tools to replicate. In xitdb, once a transaction is
committed, its bytes never change, except for a small header that points at the
latest commit. That makes it easy to keep a copy of a database (a "replica") up
to date, whether it's another file on the same machine or on another machine
across a network.

* [A sync is an `io/copy`](#a-sync-is-an-iocopy)
* [Over a network](#over-a-network)
* [Full copies](#full-copies)
* [Interrupted syncs](#interrupted-syncs)
* [Staying readable during a full copy](#staying-readable-during-a-full-copy)
* [Rules for replicas](#rules-for-replicas)

## A sync is an `io/copy`

A sync takes three steps: the replica describes what it already has, the source
streams the bytes the replica is missing, and the replica applies and commits
them. With `xitdb.replication` loaded, databases work with `clojure.java.io`, so
the whole thing is an `io/copy`:

```clojure
(require '[clojure.java.io :as io]
         '[xitdb.db :as xdb]
         '[xitdb.replication :as replication])

(def source  (xdb/xit-db "source.db"))
(def replica (xdb/xit-db "replica.db"))

(with-open [in  (io/input-stream source :since (replication/replica-state replica))
            out (io/output-stream replica)]
  (io/copy in out))

(xdb/materialize @replica) ;; => the latest value of source
```

* `replica-state` describes what the replica already has, as a map of two plain
  numbers: `{:committed-size n :checksum c}`.
* `io/input-stream` on the source, given that state as `:since`, opens an
  `InputStream` of the bytes the replica is missing. Without `:since`, the
  stream contains the whole database, which is handy for copying it into a new
  database as a backup.
* `io/output-stream` on the replica opens an `OutputStream` that applies the
  bytes and commits them when it is closed. The output stream holds the
  replica's write lock until then, so close it on the thread that opened it
  (`with-open` does this).

Run the same `io/copy` again after the source has new transactions and only the
new bytes are sent.

## Over a network

Since the state is plain data and the bytes are an ordinary `InputStream`,
replicating across a network works the same way. The replica sends its state to
the source any way you like, and the source replies with the stream. For
example, the source can serve updates from a Ring handler, and the replica can
apply them straight from the response body:

```clojure
;; on the source (with ring.middleware.params/wrap-params)
(defn updates-handler [{:keys [query-params]}]
  {:status 200
   :body   (io/input-stream db :since {:committed-size (parse-long (query-params "size"))
                                       :checksum       (parse-long (query-params "checksum"))})})

;; on the replica
(let [{:keys [committed-size checksum]} (replication/replica-state replica)]
  (with-open [in (io/input-stream (str "http://example.com/updates?size=" committed-size
                                       "&checksum=" checksum))]
    (replication/replicate! replica in)))
```

`replicate!` does the same `io/copy` as above, but if reading the input fails,
such as when the connection drops, it throws *that* error. With `with-open`
around both streams, you would instead get the exception from closing the output
stream on an incomplete update, which hides the cause.

`replicate!` returns `{:full-copy? b :replica-state s}`: whether the update
replaced the replica with a full copy (see below), and the replica's new state.

A malformed `:since`, such as one built from missing query parameters, throws
`IllegalArgumentException` with a clear message rather than failing deep inside
the engine.

## Full copies

A replica can start out as any new database. Each database has a random file ID,
so the first sync sees that the replica isn't a copy of the source and sends the
entire source file, replacing the replica. The same happens if the replica can
no longer be extended, such as when the source is replaced by a compacted copy
(which is a new file) or restored from an older backup.

A full copy replaces the replica in place: the replica is truncated as the copy
begins and the new bytes are written over it. While it runs, reads from the
replica throw. Values read from the replica before it (lazy maps, vectors,
cursors) hold offsets into the old file, so afterwards they read whatever the
copy holds there: another value, garbage, or an error, with no way to tell.
Don't use them after a full copy: `materialize` what you need to keep, or read
it again after the sync. `replicate!` returns whether an update was a full copy.

A full copy brings the source's header along, so the replica switches to the
source's [mode](../README.md#hashing-and-sharing-files-with-other-languages), even
if it was opened with `:mode`.

## Interrupted syncs

If an incremental sync is interrupted, such as by a dropped connection, closing
the output stream throws and the replica stays at its last commit. The next
sync discards whatever was partially written.

If a full copy is interrupted, the replica can't be read until a sync succeeds.
`replica-state` describes it as empty (`{:committed-size 0 :checksum 0}`), so
the next sync sends a full copy again. If the process restarts first, the file
won't open: delete it and sync again, since a replica is only a copy.

## Staying readable during a full copy

If a replica must stay readable during a full copy, apply the copy to a new
database and switch to it. Ask the source whether it can extend the replica with
`can-extend?` (over a network, the source's handler can return the answer), and
if not, copy the source without `:since` into a new file, then swap files or
handles in your app:

```clojure
(let [state (replication/replica-state replica)]
  (if (replication/can-extend? source state)
    (with-open [in  (io/input-stream source :since state)
                out (io/output-stream replica)]
      (io/copy in out))
    (let [staged (xdb/xit-db "replica-staged.db")]
      (with-open [in  (io/input-stream source)
                  out (io/output-stream staged)]
        (io/copy in out))
      ;; now switch your app from `replica` to `staged`
      staged)))
```

## Rules for replicas

* Treat a replica as read-only: don't `swap!` or `reset!` it. Once a replica
  has commits of its own, it is no longer a copy of the source, so the next
  sync replaces it with a full copy and those commits are lost.
* Don't replicate a database into itself or into another handle on the same
  file.
* Only replicate from databases that xitdb-clj can open; the key hash format is
  not checked.
* Opening a database as a `Reader` or `Writer` throws, since the streams are
  binary. Use `io/input-stream` and `io/output-stream`.
