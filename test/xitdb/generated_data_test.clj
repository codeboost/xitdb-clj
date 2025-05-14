(ns xitdb.generated-data-test
  (:require
    [clojure.edn :as edn]
    [clojure.test :refer :all]
    [xitdb.gen-map :as gen-map]
    [xitdb.test-utils :as tu :refer [with-db]]))

(defn uuid [s]
  (java.util.UUID/fromString s))

(defn load-edn-file [filename]
  (edn/read-string {:readers {'inst  #'clojure.instant/read-instant-date
                              'uuid  #'uuid}}
                   (slurp filename)))

(deftest ComplexTest1
  (with-db [db (tu/test-db)]
    (let [m (load-edn-file "test-resources/map-1.edn")]
      (reset! db m)
      (is (= m @db)))))

(deftest ComplexTest2
  (with-db [db (tu/test-db)]
    (let [m (load-edn-file "test-resources/map-2.edn")]
      (reset! db m)
      (is (= m @db))

      (testing "Random seeking of keypaths"
        (let [paths (gen-map/random-map-paths m 30)]
          (doseq [path paths]
            (let [keypath (first path)
                  value (second path)]
              (is (= value (get-in @db keypath) (get-in m keypath)))))))

      (testing "Merging two huge maps"
        (let [m2 (load-edn-file "test-resources/map-1.edn")]
          (swap! db merge m2)
          (is (= @db (merge m m2)))
          (is (= (count @db) (count (merge m m2)))))))))


