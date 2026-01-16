(ns todo.core
  "A minimal todo app demonstrating xitdb-clj's key features:
   - Atom-like API (swap!, reset!, @deref)
   - Version history / time-travel
   - Clojure-native queries (no query language)
   - Persistence to file"
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [xitdb.db :as xdb]))

;; ============================================================================
;; Database Setup
;; ============================================================================

(defn create-db
  "Create an in-memory database (great for testing/demos)"
  []
  (xdb/xit-db :memory))

(defn create-persistent-db
  "Create a file-based database (data survives restarts)"
  [path]
  (xdb/xit-db path))

;; ============================================================================
;; CRUD Operations - Just like working with a Clojure atom!
;; ============================================================================

(defn add-todo
  "Add a new todo item"
  [db text]
  (swap! db update :todos conj
         {:id   (str (random-uuid))
          :text text
          :done false}))

(defn complete-todo
  "Mark a todo as complete by id"
  [db id]
  (swap! db update :todos
         (fn [todos]
           (mapv #(if (= (:id %) id)
                    (assoc % :done true)
                    %)
                 todos))))

(defn uncomplete-todo
  "Mark a todo as not complete by id"
  [db id]
  (swap! db update :todos
         (fn [todos]
           (mapv #(if (= (:id %) id)
                    (assoc % :done false)
                    %)
                 todos))))

(defn delete-todo
  "Remove a todo by id"
  [db id]
  (swap! db update :todos
         (fn [todos]
           (vec (remove #(= (:id %) id) todos)))))

(defn list-todos
  "Get all todos as native Clojure data"
  [db]
  (:todos @db))

;; ============================================================================
;; Querying - No query language, just Clojure!
;; ============================================================================

(defn pending-todos
  "Filter to only pending (not done) todos"
  [db]
  (->> (:todos @db)
       (filter (complement :done))))

(defn completed-todos
  "Filter to only completed todos"
  [db]
  (->> (:todos @db)
       (filter :done)))

(defn find-todo
  "Find a todo by text (partial match)"
  [db search-text]
  (->> (:todos @db)
       (filter #(str/includes? (:text %) search-text))))

(defn count-by-status
  "Count todos grouped by done/pending"
  [db]
  (let [todos (list-todos db)]
    {:total     (count todos)
     :completed (count (filter :done todos))
     :pending   (count (filter (complement :done) todos))}))

;; ============================================================================
;; History / Time-Travel - The killer feature!
;; ============================================================================

(defn history-count
  "How many versions exist in the database?"
  [db]
  (xdb/history-index db))

(defn todos-at-version
  "Get todos as they were at a specific version (0 = first version)"
  [db version]
  (when-let [snapshot (xdb/deref-at db version)]
    (xdb/materialize (:todos snapshot))))

(defn show-all-history
  "Show all historical versions of the todos"
  [db]
  (let [count (history-count db)]
    (for [i (range count)]
      {:version i
       :todos   (todos-at-version db i)})))

(defn diff-versions
  "Compare todos between two versions"
  [db v1 v2]
  (let [todos1 (set (todos-at-version db v1))
        todos2 (set (todos-at-version db v2))]
    {:added   (set/difference todos2 todos1)
     :removed (set/difference todos1 todos2)}))

;; ============================================================================
;; Demo - Run this to see xitdb-clj in action!
;; ============================================================================

(defn demo
  "Interactive demonstration of xitdb-clj features"
  []
  (println "")
  (println "========================================")
  (println "  xitdb-clj Todo Example")
  (println "========================================")
  (println "")

  ;; Create database
  (let [db (create-db)]

    ;; Initialize with empty todos
    (reset! db {:todos []})
    (println "1. Created database with empty todos")
    (println "   Todos:" (list-todos db))
    (println "")

    ;; Add some todos
    (add-todo db "Learn xitdb-clj basics")
    (add-todo db "Build something with version history")
    (add-todo db "Share project with community")
    (println "2. Added 3 todos:")
    (doseq [todo (list-todos db)]
      (println "   -" (:text todo)))
    (println "")

    ;; Complete one
    (let [first-id (-> @db :todos first :id)]
      (complete-todo db first-id))
    (println "3. Completed first todo:")
    (doseq [todo (list-todos db)]
      (println (str "   [" (if (:done todo) "x" " ") "] " (:text todo))))
    (println "")

    ;; Query examples
    (println "4. Clojure-native queries (no query language!):")
    (println "   Pending:" (count (pending-todos db)))
    (println "   Completed:" (count (completed-todos db)))
    (println "   Stats:" (count-by-status db))
    (println "")

    ;; THE MAGIC: History / Time-Travel
    (println "========================================")
    (println "  History / Time-Travel")
    (println "========================================")
    (println "")
    (println "5. Total versions in database:" (history-count db))
    (println "")

    (println "6. Walk through history:")
    (doseq [{:keys [version todos]} (show-all-history db)]
      (println (str "   Version " version ": " (count todos) " todos"
                    (when (seq todos)
                      (str " - " (mapv :text todos))))))
    (println "")

    ;; Access specific past version
    (println "7. Todos at version 1 (after first add):")
    (doseq [todo (todos-at-version db 1)]
      (println "   -" (:text todo)))
    (println "")

    ;; Show diff between versions
    (println "8. Diff between version 1 and current:")
    (let [{:keys [added removed]} (diff-versions db 1 (dec (history-count db)))]
      (println "   Added:" (count added) "todos")
      (println "   Changed:" (count removed) "todos (marked complete)"))
    (println "")

    (println "========================================")
    (println "  Done! Try it yourself in the REPL")
    (println "========================================")
    (println "")
    (println "  (def db (todo.core/create-db))")
    (println "  (reset! db {:todos []})")
    (println "  (todo.core/add-todo db \"My task\")")
    (println "  (todo.core/list-todos db)")
    (println "  (todo.core/history-count db)")
    (println "")))

;; ============================================================================
;; Persistence Demo
;; ============================================================================

(defn persistence-demo
  "Demonstrate file-based persistence"
  [db-path]
  (println "")
  (println "=== Persistence Demo ===")
  (println "")

  ;; Create and populate
  (println "1. Creating database at:" db-path)
  (with-open [db (create-persistent-db db-path)]
    (reset! db {:todos []})
    (add-todo db "This todo survives restarts")
    (add-todo db "So does this one")
    (println "   Added 2 todos"))

  ;; Reopen and verify
  (println "")
  (println "2. Reopening database...")
  (with-open [db (create-persistent-db db-path)]
    (println "   Found" (count (list-todos db)) "todos:")
    (doseq [todo (list-todos db)]
      (println "   -" (:text todo))))

  (println "")
  (println "Data persisted successfully!"))

;; Run demo when loaded
(comment
  ;; Run the main demo
  (demo)

  ;; Run persistence demo (creates a file)
  (persistence-demo "/tmp/todo-demo.db")

  ;; Interactive REPL usage
  (def db (create-db))
  (reset! db {:todos []})
  (add-todo db "First task")
  (add-todo db "Second task")
  (list-todos db)
  (pending-todos db)
  (history-count db)
  (todos-at-version db 0)
  (show-all-history db))
