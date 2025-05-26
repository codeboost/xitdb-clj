(ns xitdb.map-test
  (:require
    [clojure.test :refer :all]
    [xitdb.test-utils :as tu :refer [with-db]]))

(deftest map-with-complex-keys
  (with-db [db (tu/test-db)]
    (reset! db {:foo {{:bar :baz} 42}})
    #_(reset! db {:foo {[1 :bar] 31
                        [2 :baz] 42}})))