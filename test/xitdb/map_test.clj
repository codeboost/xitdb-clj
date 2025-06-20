(ns xitdb.map-test
  (:require
    [clojure.test :refer :all]
    [xitdb.test-utils :as tu :refer [with-db]]))

(deftest map-with-complex-keys
  (with-db [db (tu/test-db)]
    (testing "Composite values as keys"
      (reset! db {:foo {{:bar :baz} 42}})
      (is (= {:foo {{:bar :baz} 42}}
             (tu/materialize @db)))

      (reset! db {:foo {[1 :bar] 31
                        [2 :baz] 42}})
      (is (= {:foo {[1 :bar] 31
                    [2 :baz] 42}}
             (tu/materialize @db)))

      (swap! db update :foo dissoc [2 :baz])
      
      (is (= {:foo {[1 :bar] 31}}
             (tu/materialize @db))))))