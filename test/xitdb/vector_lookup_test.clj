(ns xitdb.vector-lookup-test
  (:require [clojure.test :refer :all]
            [xitdb.db :as xdb]))

(defn- check-lookups [v expected]
  (doseq [k [-1 -100 (count expected) 1.9 1.0 nil :x "1"
            Long/MAX_VALUE 999999999999999999999999N]]
    (is (= (get expected k) (get v k)) (str "get at " (pr-str k)))
    (is (= (get expected k ::missing) (get v k ::missing))
        (str "get with default at " (pr-str k)))
    (is (false? (contains? v k)))
    (is (nil? (find v k))))
  (doseq [i [-1 (count expected)]]
    (is (= ::missing (nth v i ::missing)))
    (is (thrown? IndexOutOfBoundsException (nth v i))))
  (doseq [i (range (count expected))]
    (is (= (nth expected i) (nth v i) (nth v i ::missing)
           (get v i) (get v i ::missing)))
    (is (= (find expected i) (find v i)))
    (is (contains? v i)))
  (when (seq expected)
    (is (= (get expected 0N) (get v 0N ::missing)))))

(deftest vector-lookup-bounds-in-all-views
  (doseq [expected [[nil 20 30] []]]
    (let [file (java.io.File/createTempFile "xitdb-vector-lookup-" ".db")]
      (try
        (doseq [location [:memory (.getPath file)]]
          (with-open [db (xdb/xit-db location)]
            (reset! db expected)
            (check-lookups @db expected)
            (swap! db (fn [v] (check-lookups v expected) v))
            (swap! db (fn [v] (check-lookups (xdb/freeze! v) expected) v))))
        (with-open [db (xdb/xit-db (.getPath file))]
          (check-lookups @db expected))
        (finally (.delete file))))))

(deftest vector-invocation-keeps-its-throwing-contract
  (with-open [db (xdb/xit-db :memory)]
    (reset! db [nil 20])
    (let [v @db]
      (is (nil? (v 0)))
      (is (= 20 (v 1)))
      (doseq [k [-1 2 Long/MAX_VALUE 999999999999999999999999N]]
        (is (thrown? IndexOutOfBoundsException (v k))))
      (doseq [k [1.0 1.9 nil :x]]
        (is (thrown? IllegalArgumentException (v k)))))))
