(ns xitdb.sorted-pagination-test
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]
    [xitdb.sorted :as xsorted]
    [xitdb.test-utils :as tu :refer [with-db]])
  (:import
    [java.time Instant]))

(deftest rank-on-sorted-map
  (with-open [db (xdb/xit-db :memory)]
    (let [oracle (into (sorted-map) (map vector (range 0 40 2) (range)))]
      (reset! db oracle)
      (let [m @db]
        (testing "rank of a present key is its index"
          (doseq [[i k] (map-indexed vector (keys oracle))]
            (is (= i (xsorted/rank m k)) (str "rank of present " k))))
        (testing "rank of an absent key is its would-be insertion index"
          (is (= 0 (xsorted/rank m -1)))
          (is (= 1 (xsorted/rank m 1)))
          (is (= 20 (xsorted/rank m 100))))))))

(deftest rank-on-sorted-set
  (with-open [db (xdb/xit-db :memory)]
    (let [oracle (into (sorted-set) (range 0 40 2))]
      (reset! db oracle)
      (let [s @db]
        (testing "rank of a present member is its index"
          (doseq [[i k] (map-indexed vector (seq oracle))]
            (is (= i (xsorted/rank s k)) (str "rank of present " k))))
        (testing "rank of an absent member is its would-be insertion index"
          (is (= 0 (xsorted/rank s -1)))
          (is (= 1 (xsorted/rank s 1)))
          (is (= 20 (xsorted/rank s 100))))))))

(deftest rank-and-nth-are-inverses
  (testing "on a sorted map: (= i (rank m (key (nth m i))))"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (into (sorted-map) (map vector (shuffle (range 30)) (range))))
      (let [m @db]
        (doseq [i (range (count m))]
          (is (= i (xsorted/rank m (key (nth m i)))) (str "i=" i))))))
  (testing "on a sorted set: (= i (rank s (nth s i)))"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (into (sorted-set) (shuffle (range 30))))
      (let [s @db]
        (doseq [i (range (count s))]
          (is (= i (xsorted/rank s (nth s i))) (str "i=" i)))))))

(deftest pagination-on-map
  (with-open [db (xdb/xit-db :memory)]
    (let [oracle (into (sorted-map) (map vector (range 0 40 2) (range)))]
      (reset! db oracle)
      (let [m @db
            ov (vec oracle)]
        (testing "page returns the correct ordered window"
          (is (= (subvec ov 5 10) (xsorted/page m 5 5)))
          (is (= (take 3 ov) (xsorted/page m 0 3))))
        (testing "page stops cleanly at the end of the collection"
          (is (= (subvec ov 18 20) (xsorted/page m 18 5)))
          (is (= 2 (count (xsorted/page m 18 100)))))
        (testing "from-index streams from a rank to the end"
          (is (= (subvec ov 17 20) (xsorted/from-index m 17))))))))

(deftest pagination-on-set
  (with-open [db (xdb/xit-db :memory)]
    (let [oracle (into (sorted-set) (range 0 40 2))]
      (reset! db oracle)
      (let [s @db
            ov (vec oracle)]
        (testing "page returns the correct ordered window of members"
          (is (= (subvec ov 5 10) (xsorted/page s 5 5)))
          (is (= (take 3 ov) (xsorted/page s 0 3))))
        (testing "page stops cleanly at the end of the collection"
          (is (= (subvec ov 18 20) (xsorted/page s 18 5)))
          (is (= 2 (count (xsorted/page s 18 100)))))
        (testing "from-index streams from a rank to the end"
          (is (= (subvec ov 17 20) (xsorted/from-index s 17))))))))

(deftest pagination-is-lazy
  (testing "from-index returns a lazy seq and does not realise a large collection"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (into (sorted-map) (map vector (range 2000) (range))))
      (let [m @db
            p (xsorted/page m 0 5)]
        (is (instance? clojure.lang.LazySeq (xsorted/from-index m 0)))
        (is (= 5 (count p)))
        (is (= (map vector (range 5) (range 5))
               (map (juxt key val) p))))))
  (testing "from-index on a set is lazy over a large collection"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db (into (sorted-set) (range 2000)))
      (let [s @db]
        (is (instance? clojure.lang.LazySeq (xsorted/from-index s 0)))
        (is (= (range 0 5) (xsorted/page s 0 5)))))))

(deftest doc-example-timestamp-id-secondary-index
  (testing "build a timestamp -> id secondary index and page through it"
    (with-open [db (xdb/xit-db :memory)]
      ;; Events arrive out of order; index them by their (unique) timestamp.
      (let [base   (Instant/parse "2024-01-01T00:00:00Z")
            events (for [i (shuffle (range 100))]
                     {:id i :ts (.plusSeconds base i)})]
        (reset! db (sorted-map))
        (doseq [e events]
          (swap! db assoc (:ts e) (:id e)))

        (testing "rank gives the chronological position of a timestamp"
          (is (= 0 (xsorted/rank @db base)))
          (is (= 50 (xsorted/rank @db (.plusSeconds base 50)))))

        (testing "page serves a chronological window of [ts id] pairs"
          (let [pg (xsorted/page @db 10 5)]
            (is (= [(.plusSeconds base 10)
                    (.plusSeconds base 11)
                    (.plusSeconds base 12)
                    (.plusSeconds base 13)
                    (.plusSeconds base 14)]
                   (map key pg)))
            (is (= [10 11 12 13 14] (map val pg)))))

        (testing "paging to the end stops cleanly"
          (is (= 3 (count (xsorted/page @db 97 10)))))))))
