(ns xitdb.hashing-test
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb])
  (:import [java.util Date TimeZone]))

(deftest millisecond-date-keys-survive-reopen
  (let [file (java.io.File/createTempFile "xitdb-date-keys-" ".db")
        path (.getPath file)
        a (Date. 1000)
        b (Date. 1001)]
    (try
      (with-open [db (xdb/xit-db path)]
        (reset! db {a :first b :second}))
      (with-open [db (xdb/xit-db path)]
        (is (= 2 (count @db)))
        (is (= :first (get @db a)))
        (is (= :second (get @db b))))
      (finally (.delete file)))))

(deftest collection-keys-ignore-printer-settings
  (with-open [db (xdb/xit-db :memory)]
    (binding [*print-length* 1 *print-level* 1]
      (reset! db {[1 [2]] :first [1 [3]] :second}))
    (is (= 2 (count @db)))
    (is (= :first (get @db [1 [2]])))
    (is (= :second (get @db [1 [3]])))))

(deftest equal-collection-keys-share-one-entry
  (with-open [db (xdb/xit-db :memory)]
    (let [a (array-map :a 1 :b [2 3])
          b (array-map :b '(2 3) :a 1)]
      (reset! db {a :original})
      (is (= :original (get @db b)))
      (swap! db assoc b :updated)
      (is (= 1 (count @db)))
      (is (= :updated (get @db a))))))

(deftest stored-collection-keys-can-be-reused
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {[1 2] :original})
    (let [k (ffirst @db)]
      (is (= :original (get @db k)))
      (swap! db assoc k :updated)
      (is (= 1 (count @db)))
      (is (= :updated (get @db [1 2])))))
  (with-open [db (xdb/xit-db :memory)]
    (reset! db #{[1 2]})
    (let [member (first @db)]
      (swap! db conj member)
      (is (= 1 (count @db)))
      (swap! db disj member)
      (is (empty? @db)))))

(deftest date-keys-and-members-do-not-depend-on-timezone
  (let [original (TimeZone/getDefault)
        a (Date. 1000)
        b (Date. 1001)]
    (try
      (TimeZone/setDefault (TimeZone/getTimeZone "UTC"))
      (with-open [db (xdb/xit-db :memory)]
        (reset! db {:by-date {a :first b :second} :dates #{a b}})
        (TimeZone/setDefault (TimeZone/getTimeZone "America/Los_Angeles"))
        (is (= :first (get-in @db [:by-date a])))
        (is (= :second (get-in @db [:by-date b])))
        (is (= 2 (count (:dates @db))))
        (swap! db update :by-date assoc a :updated)
        (swap! db update :dates disj a)
        (is (= :updated (get-in @db [:by-date a])))
        (is (= #{b} (xdb/materialize (:dates @db)))))
      (finally (TimeZone/setDefault original)))))

(deftest nested-collection-keys-survive-reopen-and-printer-changes
  (let [file (java.io.File/createTempFile "xitdb-collection-keys-" ".db")
        path (.getPath file)
        a (with-meta [{:a 1 :b #{[2 3] '(4 5)}}] {:source :original})
        b (list (array-map :b #{'(2 3) [4 5]} :a 1N))]
    (try
      (with-open [db (xdb/xit-db path)]
        (binding [*print-length* 0 *print-level* 0 *print-meta* true *print-dup* true]
          (reset! db {a :original})))
      (with-open [db (xdb/xit-db path)]
        (is (= :original (get @db b)))
        (swap! db assoc (ffirst @db) :updated)
        (is (= 1 (count @db)))
        (is (= :updated (get @db a)))
        (swap! db dissoc b)
        (is (empty? @db)))
      (finally (.delete file)))))

(deftest date-subclass-keys-are-rejected-before-precision-is-lost
  (let [timestamp (java.sql.Timestamp/from (java.time.Instant/parse "2026-01-01T00:00:00.000000001Z"))]
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:safe true})
      (doseq [value [{timestamp :value} {[timestamp] :value} #{timestamp} #{[timestamp]}]]
        (is (thrown-with-msg? IllegalArgumentException #"Instant" (reset! db value)))
        (is (= {:safe true} (xdb/materialize @db)))))))

(deftest rejected-lookup-does-not-poison-subsequent-writes
  (with-open [source (xdb/xit-db :memory)
              other (xdb/xit-db :memory)]
    (reset! source {})
    (reset! other {})
    (doseq [target [source other]]
      (testing (if (identical? source target) "same database" "another database")
        (is (thrown? IllegalArgumentException
                     (get @source (map identity [1]))))
        (swap! target assoc :saved 42)
        (is (= 42 (get @target :saved)))
        (swap! target assoc :saved 43)
        (is (= 43 (get @target :saved)))
        (is (= 1 (count @target)) "updating the key must not create a duplicate entry")
        (is (= {:saved 43} (xdb/materialize @target)))))))
