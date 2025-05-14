(ns xitdb.deep-merge-test
  (:require
    [clojure.test :refer :all]
    [xitdb.test-utils :as tu :refer [with-db]]))

(deftest DeepMergeTest
  (testing "deep nested merges"
    (let [db (tu/test-db)]
      ;; Set up initial nested map
      (reset! db {"users" {:profile     {:name     "John"
                                         :settings {:theme "dark"}}
                           :permissions {:read  true
                                         :write false}}})

      ;; Define a recursive deep merge function
      (let [deep-merge (fn deep-merge [& maps]
                         (apply merge-with (fn [x y]
                                             (if (and (map? x) (map? y))
                                               (deep-merge x y)
                                               y))
                                maps))]

        ;; Perform a deep merge
        (swap! db deep-merge {"users" {:profile     {:settings {:notifications true}}
                                       :permissions {:admin false}}})

        (is (= {"users" {:profile     {:name     "John"
                                       :settings {:theme         "dark"
                                                  :notifications true}}
                         :permissions {:read  true
                                       :write false
                                       :admin false}}}
               @db))

        ;; Another deep merge, overwriting some values
        (swap! db deep-merge {"users" {:profile {:name     "Jane"
                                                 :settings {:theme "light"}}
                                       :stats   {:visits 10}}})

        (is (= {"users" {:profile     {:name     "Jane"
                                       :settings {:theme         "light"
                                                  :notifications true}}
                         :permissions {:read  true
                                       :write false
                                       :admin false}
                         :stats       {:visits 10}}}
               @db)))

      ;; Using update-in for targeted nested merges
      (swap! db update-in ["users" :profile :settings]
             merge {:language "en" :fontSize "medium"})

      (is (= {"users" {:profile     {:name     "Jane"
                                     :settings {:theme         "light"
                                                :notifications true
                                                :language      "en"
                                                :fontSize      "medium"}}
                       :permissions {:read  true
                                     :write false
                                     :admin false}
                       :stats       {:visits 10}}}
             @db))
      (is (tu/db-equal-to-atom? db)))))