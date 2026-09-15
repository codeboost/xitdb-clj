(ns xitdb.nil-presence-test
  (:require [clojure.test :refer :all]
            [xitdb.db :as db]))

(defn- check-views [value check]
  (let [file (java.io.File/createTempFile "xitdb-reads-" ".db")]
    (try
      (doseq [location [:memory (.getPath file)]]
        (with-open [d (db/xit-db location)]
          (reset! d value)
          (check @d)
          (swap! d (fn [v] (check v) v))
          (swap! d (fn [v] (check (db/freeze! v)) v))))
      (with-open [d (db/xit-db (.getPath file))] (check @d))
      (finally (.delete file)))))

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
