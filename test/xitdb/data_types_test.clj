(ns xitdb.data-types-test
  (:require
    [clojure.test :refer :all]
    [xitdb.test-utils :as tu :refer [with-db]]))


(deftest KeywordsAndStrings
  (testing "Should correctly handle key and string keys and values"
    (with-db [db (tu/test-db)]
      (reset! db {:foo "foo"
                  "foo" "more-foo"
                  ":foo" :foo})
      (is (= {:foo "foo"
              "foo" "more-foo"
              ":foo" :foo} @db))
      (swap! db dissoc "foo" ":foo")
      (is (= {:foo "foo"} @db))
      (swap! db dissoc :foo)
      (is (= {} @db)))))

(deftest KeywordsAndStringsInSets
  (testing "Should correctly handle keywords and strings in sets"
    (with-db [db (tu/test-db)]
      (reset! db #{:foo "foo" ":foo"})
      (is (= #{:foo "foo" ":foo"} @db))

      (swap! db conj :bar)
      (is (= #{:foo "foo" ":foo" :bar} @db))

      (swap! db disj :foo ":foo")
      (is (= #{"foo" :bar} @db))

      (swap! db disj "foo")
      (is (= #{:bar} @db)))))

(deftest KeywordsAndStringsInVectors
  (testing "Should correctly handle keywords and strings in vectors"
    (with-db [db (tu/test-db)]
      (reset! db [:foo "foo" ":foo"])
      (is (= [:foo "foo" ":foo"] @db))

      (swap! db conj :bar)
      (is (= [:foo "foo" ":foo" :bar] @db))

      (swap! db assoc 0 "changed")
      (is (= ["changed" "foo" ":foo" :bar] @db))

      (swap! db #(into [] (remove #{":foo"} %)))
      (is (= ["changed" "foo" :bar] @db)))))

(deftest KeywordsAndStringsInLists
  (testing "Should correctly handle keywords and strings in lists"
    (with-db [db (tu/test-db)]
      (reset! db '(:foo "foo" ":foo"))
      (is (= '(:foo "foo" ":foo") @db))

      (swap! db conj :bar)
      (is (= '(:bar :foo "foo" ":foo") @db)))))

(deftest NilValuesInMaps
  (testing "Should correctly handle nil values and distinguish from missing keys"
    (with-db [db (tu/test-db)]
      ;; Test nil values vs missing keys
      (reset! db {:existing-key nil :other-key "value"})
      (is (contains? @db :existing-key))
      (is (= nil (get @db :existing-key)))
      (is (= nil (get @db :missing-key)))
      (is (not (contains? @db :missing-key)))
      
      ;; Test entryAt with nil values
      (is (= [:existing-key nil] (find @db :existing-key)))
      (is (= nil (find @db :missing-key)))
      
      ;; Test nil as a key
      (reset! db {nil "nil-value" :other "other-value"})
      (is (contains? @db nil))
      (is (= "nil-value" (get @db nil)))
      (is (= [nil "nil-value"] (find @db nil))))))

