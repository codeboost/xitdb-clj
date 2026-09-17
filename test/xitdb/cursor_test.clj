(ns xitdb.cursor-test
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]))

(deftest cursor-updates-return-the-value-at-their-path
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {:outer {:counter 10} :untouched 99})
    (let [cursor (xdb/xdb-cursor (xdb/xdb-cursor db [:outer]) [:counter])]
      (is (= 11 (swap! cursor inc)))
      (is (= 21 (swap! cursor + 10)))
      (is (= 24 (swap! cursor + 1 2)))
      (is (= 34 (swap! cursor + 1 2 3 4)))
      (is (= 25 (reset! cursor 25)))
      (is (= 25 @cursor))
      (is (nil? (reset! cursor nil)))
      (is (= {:outer {:counter nil} :untouched 99} (xdb/materialize @db))))
    (let [cursor (xdb/xdb-cursor db [:outer])
          result (reset! cursor {:counter 42})]
      (is (= {:counter 42} (xdb/materialize result))))
    (is (= {:outer {:counter 42} :untouched 99}
           (xdb/materialize (swap! (xdb/xdb-cursor db []) identity))))))

(deftest cursor-history-returns-scoped-snapshots-and-a-root-history-index
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {:nested {:v 1} :untouched 99})
    (let [cursor (xdb/xdb-cursor db [:nested])
          [index before after] (binding [xdb/*return-history?* true]
                                 (swap! cursor assoc :v 2))]
      (is (= [1 {:v 1} {:v 2}] (mapv xdb/materialize [index before after])))
      (is (= after (:nested (xdb/deref-at db index))))
      (is (= [2 {:v 2} nil]
             (mapv xdb/materialize
                   (binding [xdb/*return-history?* true] (reset! cursor nil)))))
      (is (= {:v 1} (xdb/materialize before)))
      (is (= {:v 2} (xdb/materialize after))))))

(deftest CursorTest
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {:foo {:bar [1 2 3 {:hidden true} 5]}})
    (let [cursor1 (xdb/xdb-cursor db [:foo :bar])
          cursor2 (xdb/xdb-cursor db [:foo :bar 2])
          cursor3 (xdb/xdb-cursor db [:foo :bar 3 :hidden])]
      (testing "Cursors return the value at keypath"
        (is (= [1 2 3 {:hidden true} 5] (xdb/materialize @cursor1)))
        (is (= 3 @cursor2))
        (is (= true @cursor3)))

      (testing "reset! on the cursor changes the underlying database"
        (reset! cursor3 :changed)
        (is (= :changed @cursor3))
        (is (= :changed (get-in @db [:foo :bar 3 :hidden])))
        (is (= [1 2 3 {:hidden :changed} 5]) (xdb/materialize @cursor1)))

      (testing "swap! mutates the value at cursor"
        (swap! cursor1 assoc-in [3 :hidden] :changed-by-swap!)
        (is (= [1 2 3 {:hidden :changed-by-swap!} 5]) (xdb/materialize @cursor1))
        (is (= :changed-by-swap! @cursor3))
        (is (= 3 @cursor2)))

      (testing "Correctly handles invalid cursor path"
        (is (nil? @(xdb/xdb-cursor db [:foo :bar 999])))))))

(deftest cursor-into-sorted-map
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {:idx (sorted-map 1 {:name "a"} 2 {:name "b"})})
    (let [c (xdb/xdb-cursor db [:idx 1 :name])]
      (testing "read through a sorted-map key"
        (is (= "a" @c)))
      (testing "reset! through a sorted-map key writes back to the db"
        (reset! c "A")
        (is (= "A" @c))
        (is (= "A" (get-in (xdb/materialize @db) [:idx 1 :name])))
        (is (= "b" (get-in (xdb/materialize @db) [:idx 2 :name])))))))

(deftest cursor-into-sorted-set
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {:tags (sorted-set "a" "b" "c")})
    (let [c (xdb/xdb-cursor db [:tags])]
      (testing "read a sorted set through a cursor"
        (is (= ["a" "b" "c"] (seq (xdb/materialize @c)))))
      (testing "swap! mutates the sorted set at the cursor"
        (swap! c conj "d")
        (is (= ["a" "b" "c" "d"] (seq (xdb/materialize @c))))))))

(deftest cursor-into-sorted-set-member-is-rejected
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {:tags (sorted-set "a" "b" "c")})
    (testing "writing into a sorted-set member throws a clear, specific error
              (members are immutable keys; use conj/disj on the set itself)"
      (let [c (xdb/xdb-cursor db [:tags "a"])
            ex (is (thrown? IllegalArgumentException (reset! c "z")))]
        (is (re-find #"sorted-set member" (.getMessage ex)))))))

(deftest cursor-write-to-absent-map-key-stores-the-key
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {:existing 1})
    (reset! (xdb/xdb-cursor db [:brand-new]) 42)
    (testing "the new entry is a real key/value pair, not a keyless slot"
      (is (= {:existing 1 :brand-new 42} (xdb/materialize @db)))
      (is (= #{:existing :brand-new} (set (keys @db)))))))

(deftest cursor-into-hash-set-member-is-rejected
  (with-open [db (xdb/xit-db :memory)]
    (reset! db {:tags #{:a :b}})
    (testing "a member is stored under its own hash, so overwriting it in place
              would desync the set; the write is refused and nothing changes"
      (let [c  (xdb/xdb-cursor db [:tags :a])
            ex (is (thrown? IllegalArgumentException (reset! c :z)))]
        (is (re-find #"set member" (.getMessage ex)))
        (is (= #{:a :b} (xdb/materialize (get @db :tags))))
        (is (= 1 (count db)) "the refused write did not append a history entry")))))
