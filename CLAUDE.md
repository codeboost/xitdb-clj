# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Running Tests
```bash
clj -M:test
```

### Running a Single Test File
```bash
clj -M:test -n xitdb.database-test
```

### REPL Development
```bash
clj
```

## Architecture Overview

This is a Clojure wrapper around the xitdb-java immutable database, providing atom-like semantics for persistent data structures.

### Core Components

- **xitdb.db**: Main database interface implementing IAtom protocol. The `XITDBDatabase` type wraps Java database instances and provides thread-safe operations via ReentrantLock.

- **xitdb.xitdb-types**: Type dispatch system that handles reading/writing different data structures (HashMap, ArrayList, LinkedList, HashSet) to/from database cursors.

- **Data Structure Implementations**: Each data type (hash-map, array-list, linked-list, hash-set) has its own namespace implementing Clojure collection protocols over persistent storage.

- **Conversion Layer**: `util/conversion.clj` handles serialization between Clojure values and database slots.

### Key Protocols

- `ISlot`: For types that can provide their own database slot representation
- `IReadFromCursor`: For reading values from database cursors  
- `IMaterialize`: For converting lazy database structures to in-memory Clojure data
- `IUnwrap`: For extracting wrapped values

### Database Operations

The database acts like a Clojure atom with `reset!` and `swap!` operations, but persists all changes to disk. History is maintained as an append-only log accessible via `db-history`.