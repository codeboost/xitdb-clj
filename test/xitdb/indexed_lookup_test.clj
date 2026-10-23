(ns xitdb.indexed-lookup-test
  "Index lookups on stored vectors and lists behave like a Clojure vector."
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]))

(def ^:private expected [nil 20 30])

(def ^:private lookups
  {'get     (fn [v k] (get v k))
   'get-or  (fn [v k] (get v k ::missing))
   'nth     (fn [v k] (nth v k))
   'nth-or  (fn [v k] (nth v k ::missing))})

(defn- outcome
  "The result of calling `f`, or the class of what it threw."
  [f]
  (try (f) (catch Throwable t (class t))))

(defn- check-lookups [view ks]
  (doseq [[op lookup] lookups
          k ks]
    (is (= (outcome #(lookup expected k)) (outcome #(lookup view k)))
        (str op " " (pr-str k)))))

(defn- check-all-views
  "Checks read views (`@db`) and write views (inside `swap!`) of `expected`
  stored as a vector and as a list."
  [ks]
  (with-open [db (xdb/xit-db :memory)]
    (doseq [stored [expected (apply list expected)]]
      (reset! db stored)
      (testing (str "read view of " (type stored))
        (check-lookups @db ks))
      (testing (str "write view of " (type stored))
        (swap! db (fn [v] (check-lookups v ks) v))))))

(deftest in-range-indices
  (check-all-views [0 1 2]))

(deftest negative-and-out-of-range-indices
  (check-all-views [-1 -3 -4 3 Long/MAX_VALUE]))

(deftest keys-that-are-not-indices
  (check-all-views [1.0 1.9 -0.5 nil :x "1" 1N 99999999999999999999N]))

(deftest invoking-a-read-view-throws-like-a-vector
  (with-open [db (xdb/xit-db :memory)]
    (doseq [stored [expected (apply list expected)]]
      (reset! db stored)
      (let [view @db]
        (doseq [k [0 1 2 -1 3 1.0 1.9 nil :x 1N 99999999999999999999N]]
          (is (= (outcome #(expected k)) (outcome #(view k)))
              (str (type stored) " invoked with " (pr-str k))))))))
