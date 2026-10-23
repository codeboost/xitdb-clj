(ns xitdb.lazy-value-test
  "Lazy sequences are rejected wherever they appear in a written value, before
  any of them is realized, and a rejected write leaves the database unchanged."
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]))

(def ^:private placements
  {'top-level identity
   'in-vector vector
   'in-nested-vector #(vector [%])
   'in-java-list #(java.util.ArrayList. ^java.util.Collection [%])
   'in-list list
   'map-value #(hash-map :items %)
   'map-key #(hash-map % 1)
   'in-set hash-set})

(defn- write-outcome
  "Writes `value` with `write` on another thread and returns :rejected,
  :accepted or :timed-out. An accepted infinite sequence would write forever,
  so the attempt is abandoned after a timeout instead of hanging the suite."
  [write value]
  (let [attempt (future
                  (try
                    (write value)
                    :accepted
                    (catch IllegalArgumentException e
                      (if (re-find #"Lazy sequences" (.getMessage e)) :rejected e))))
        outcome (deref attempt 2000 :timed-out)]
    (future-cancel attempt)
    outcome))

(defn- check-rejected
  "Checks that the value built by `make-lazy` is rejected by `reset!` and by
  `swap!` in every placement, and that the database is unchanged afterwards.
  `make-lazy` receives a no-arg function to call if the sequence is realized.
  Building a set or map key hashes, and so realizes, the sequence; only
  realization during the write is checked."
  [make-lazy placements]
  (doseq [[placement place] placements
          [op write] {'reset! (fn [db v] (reset! db v))
                      'swap! (fn [db v] (swap! db assoc :bad v))}]
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {:safe true})
      (let [realized? (atom false)
            value     (place (make-lazy #(reset! realized? true)))]
        (reset! realized? false)
        (testing (str op " " placement)
          (is (= :rejected (write-outcome #(write db %) value)))
          (is (false? @realized?))
          (is (= {:safe true} (xdb/materialize @db)))
          (is (= 1 (count db))))))))

(deftest lazy-seqs-are-rejected-at-every-depth
  (check-rejected (fn [on-realize] (lazy-seq (on-realize) [1 2])) placements))

(deftest infinite-seqs-are-rejected-at-every-depth
  ;; Hashing an infinite seq never returns, so it cannot be put in a set or used
  ;; as a map key in the first place.
  (let [placements (dissoc placements 'map-key 'in-set)]
    (testing "iterate"
      (check-rejected (fn [on-realize] (iterate #(do (on-realize) (inc %)) 0)) placements))
    (testing "range without bounds"
      (check-rejected (fn [_] (range)) placements))
    (testing "cycle"
      (check-rejected (fn [_] (cycle [1 2])) placements))))
