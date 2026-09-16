(ns xitdb.indexed-lookup-test
  (:require [clojure.test :refer :all]
            [xitdb.db :as xdb]))

(defn- check-lookups
  "Checks `v`, a database view of a sequence, against the vector `expected`.
  `contains?` and `find` are only checked on associative views: read views of
  lists are not associative, like Clojure lists."
  [v expected]
  (doseq [k [-1 -100 (count expected) 1.9 1.0 nil :x "1"
            Long/MAX_VALUE 999999999999999999999999N]]
    (is (= (get expected k) (get v k)) (str "get at " (pr-str k)))
    (is (= (get expected k ::missing) (get v k ::missing))
        (str "get with default at " (pr-str k)))
    (when (associative? v)
      (is (false? (contains? v k)))
      (is (nil? (find v k)))))
  (doseq [i [-1 (count expected)]]
    (is (= ::missing (nth v i ::missing)))
    (is (thrown? IndexOutOfBoundsException (nth v i))))
  (doseq [i (range (count expected))]
    (is (= (nth expected i) (nth v i) (nth v i ::missing)
           (get v i) (get v i ::missing)))
    (when (associative? v)
      (is (= (find expected i) (find v i)))
      (is (contains? v i))))
  (when (seq expected)
    (is (= (get expected 0N) (get v 0N ::missing)))))

(deftest indexed-lookup-bounds-in-all-views
  (doseq [[stored expected] [[[nil 20 30] [nil 20 30]]
                             [[] []]
                             ['(nil 20 30) [nil 20 30]]
                             ['() []]]]
    (let [file (java.io.File/createTempFile "xitdb-indexed-lookup-" ".db")]
      (try
        (doseq [location [:memory (.getPath file)]]
          (with-open [db (xdb/xit-db location)]
            (reset! db stored)
            (check-lookups @db expected)
            (swap! db (fn [v] (check-lookups v expected) v))
            (swap! db (fn [v] (check-lookups (xdb/freeze! v) expected) v))))
        (with-open [db (xdb/xit-db (.getPath file))]
          (check-lookups @db expected))
        (finally (.delete file))))))

(defn- check-invocation [v]
  (is (nil? (v 0)))
  (is (= 20 (v 1)))
  (is (= 20 (apply v [1])))
  (is (= 20 (v 1 ::missing)))
  (is (= ::missing (v 2 ::missing)))
  (is (= ::missing (v :x ::missing)))
  (doseq [k [-1 2 Long/MAX_VALUE 999999999999999999999999N]]
    (is (thrown? IndexOutOfBoundsException (v k)) (str "invoke with " (pr-str k))))
  (doseq [k [1.0 1.9 nil :x]]
    (is (thrown? IllegalArgumentException (v k)) (str "invoke with " (pr-str k)))))

(deftest invocation-keeps-its-throwing-contract-in-all-views
  (doseq [stored [[nil 20] '(nil 20)]]
    (with-open [db (xdb/xit-db :memory)]
      (reset! db stored)
      (check-invocation @db)
      (swap! db (fn [v] (check-invocation v) v))
      (swap! db (fn [v] (check-invocation (xdb/freeze! v)) v)))))
