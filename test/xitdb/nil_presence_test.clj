(ns xitdb.nil-presence-test
  "A stored nil must stay distinguishable from an absent key or index.
  xitdb-java 0.39 returned no cursor for an explicitly stored NONE slot, so
  lookups with a not-found default reported stored nils as missing."
  (:require
    [clojure.java.io :as io]
    [clojure.test :refer :all]
    [xitdb.db :as xdb]))

(defn- check-views
  "Runs `check` on `value` in a memory and a file database: as a fresh read,
  inside a transaction, on a frozen value, and after reopening the file."
  [value check]
  (let [file (java.io.File/createTempFile "xitdb-nil-presence-" ".db")]
    (try
      (doseq [location [:memory (.getPath file)]]
        (with-open [d (xdb/xit-db location)]
          (reset! d value)
          (check @d)
          (swap! d (fn [v] (check v) v))
          (swap! d (fn [v] (check (xdb/freeze! v)) v))))
      (with-open [d (xdb/xit-db (.getPath file))]
        (check @d))
      (finally
        (io/delete-file file true)))))

(deftest maps-distinguish-stored-nil-from-absence
  (doseq [value [{nil :nil-key :nil-value nil}
                 (sorted-map :nil-value nil)]]
    (check-views value
      (fn [m]
        (is (= [true nil [:nil-value nil] false ::missing]
               [(contains? m :nil-value) (get m :nil-value ::missing)
                (find m :nil-value) (contains? m :absent)
                (get m :absent ::missing)]))
        (when (contains? value nil)
          (is (= [true :nil-key [nil :nil-key]]
                 [(contains? m nil) (get m nil ::missing) (find m nil)])))))))

(deftest sets-contain-their-stored-nil-member
  (check-views #{nil :member}
    (fn [s]
      (is (= [true nil false ::missing]
             [(contains? s nil) (get s nil ::missing)
              (contains? s :absent) (get s :absent ::missing)])))))

(deftest vectors-distinguish-stored-nil-from-out-of-range
  (check-views [nil :member]
    (fn [v]
      (is (= [true nil nil nil false ::missing ::missing]
             [(contains? v 0) (nth v 0 ::missing) (get v 0 ::missing) (first v)
              (contains? v 2) (nth v 2 ::missing) (get v 2 ::missing)])))))

(deftest lists-distinguish-stored-nil-from-out-of-range
  (check-views '(nil :member)
    (fn [l]
      (is (= [nil nil ::missing]
             [(nth l 0 ::missing) (first l) (nth l 2 ::missing)])))))
