# Todo with History Example

A minimal todo app demonstrating xitdb-clj's key features.

## Features Demonstrated

- **Atom-like API** - Familiar `swap!`, `reset!`, `@deref` operations
- **Version history** - Access any previous database state with `deref-at`
- **Clojure-native queries** - No query language, just `filter`, `map`, `reduce`
- **Persistence** - Data survives restarts with file-based storage

## Running the Demo

```bash
cd examples/todo-with-history
clj -M -e "(require 'todo.core) (todo.core/demo)"
```

## REPL Usage

Start a REPL and try:

```clojure
(require '[todo.core :as todo])

;; Create a database
(def db (todo/create-db))
(reset! db {:todos []})

;; Add todos
(todo/add-todo db "Learn xitdb-clj")
(todo/add-todo db "Build something cool")

;; List todos
(todo/list-todos db)

;; Complete a todo
(todo/complete-todo db (-> @db :todos first :id))

;; Query with plain Clojure
(todo/pending-todos db)
(todo/completed-todos db)
(todo/count-by-status db)

;; TIME TRAVEL - The killer feature!
(todo/history-count db)              ;; How many versions?
(todo/todos-at-version db 0)         ;; Todos at first version
(todo/show-all-history db)           ;; All versions
(todo/diff-versions db 0 2)          ;; What changed?
```

## Persistence Demo

```clojure
;; Data survives restarts
(todo/persistence-demo "/tmp/my-todos.db")
```

## Key Takeaways

1. **No query language** - Use standard Clojure functions
2. **Every change is versioned** - Built-in undo/audit-trail
3. **Lazy loading** - Only reads what you access
4. **Embedded** - No external database server needed
