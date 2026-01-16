(ns xitdb.freeze-test
  "Tests for the `freeze!` function"
  (:require
    [clojure.test :refer :all]
    [xitdb.db :as xdb]
    [xitdb.test-utils :as tu]))

(deftest freeze-array-list-test
  (testing "without freeze"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {})

      (swap! db (fn [moment]
                  (let [moment (assoc moment :fruits ["apple" "pear" "grape"])
                        moment (assoc moment :food (:fruits moment))
                        moment (update moment :food conj "eggs" "rice" "fish")]
                    moment)))

      (is (= {:fruits ["apple" "pear" "grape" "eggs" "rice" "fish"]
              :food ["apple" "pear" "grape" "eggs" "rice" "fish"]}
             (xdb/materialize @db)))))

  (testing "with freeze"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {})

      (swap! db (fn [moment]
                  (let [moment (assoc moment :fruits ["apple" "pear" "grape"])
                        moment (assoc moment :food (xdb/freeze! (:fruits moment)))
                        moment (update moment :food conj "eggs" "rice" "fish")]
                    moment)))

      (is (= {:fruits ["apple" "pear" "grape"]
              :food ["apple" "pear" "grape" "eggs" "rice" "fish"]}
             (xdb/materialize @db)))))

  (testing "with freeze and modifying return value"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {})

      (swap! db (fn [moment]
                  (let [moment (assoc moment :fruits ["apple" "pear" "grape"])
                        moment (assoc moment :food (conj (xdb/freeze! (:fruits moment))
                                                         "eggs" "rice" "fish"))]
                    moment)))

      (is (= {:fruits ["apple" "pear" "grape"]
              :food ["apple" "pear" "grape" "eggs" "rice" "fish"]}
             (xdb/materialize @db))))))

(deftest freeze-linked-array-list-test
  (testing "without freeze"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {})

      (swap! db (fn [moment]
                  (let [moment (assoc moment :fruits '("apple" "pear" "grape"))
                        moment (assoc moment :food (:fruits moment))
                        moment (update moment :food conj "eggs" "rice" "fish")]
                    moment)))

      (is (= {:fruits ["fish" "rice" "eggs" "apple" "pear" "grape"]
              :food ["fish" "rice" "eggs" "apple" "pear" "grape"]}
             (xdb/materialize @db)))))

  (testing "with freeze"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {})

      (swap! db (fn [moment]
                  (let [moment (assoc moment :fruits '("apple" "pear" "grape"))
                        moment (assoc moment :food (xdb/freeze! (:fruits moment)))
                        moment (update moment :food conj "eggs" "rice" "fish")]
                    moment)))

      (is (= {:fruits ["apple" "pear" "grape"]
              :food ["fish" "rice" "eggs" "apple" "pear" "grape"]}
             (xdb/materialize @db)))))

  (testing "with freeze and modifying return value"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {})

      (swap! db (fn [moment]
                  (let [moment (assoc moment :fruits '("apple" "pear" "grape"))
                        moment (assoc moment :food (conj (xdb/freeze! (:fruits moment))
                                                         "eggs" "rice" "fish"))]
                    moment)))

      (is (= {:fruits ["apple" "pear" "grape"]
              :food ["fish" "rice" "eggs" "apple" "pear" "grape"]}
             (xdb/materialize @db))))))

(deftest freeze-hash-map-test
  (testing "without freeze"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {})

      (swap! db (fn [moment]
                  (let [moment (assoc moment :fruits {:names ["apple" "pear" "grape"]})
                        moment (assoc moment :food (:fruits moment))
                        moment (update-in moment [:food :names] conj "eggs" "rice" "fish")]
                    moment)))

      (is (= {:fruits {:names ["apple" "pear" "grape" "eggs" "rice" "fish"]}
              :food {:names ["apple" "pear" "grape" "eggs" "rice" "fish"]}}
             (xdb/materialize @db)))))

  (testing "with freeze"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {})

      (swap! db (fn [moment]
                  (let [moment (assoc moment :fruits {:names ["apple" "pear" "grape"]})
                        moment (assoc moment :food (xdb/freeze! (:fruits moment)))
                        moment (update-in moment [:food :names] conj "eggs" "rice" "fish")]
                    moment)))

      (is (= {:fruits {:names ["apple" "pear" "grape"]}
              :food {:names ["apple" "pear" "grape" "eggs" "rice" "fish"]}}
             (xdb/materialize @db)))))

  (testing "with freeze and modifying return value"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {})

      (swap! db (fn [moment]
                  (let [moment (assoc moment :fruits {:names ["apple" "pear" "grape"]})
                        moment (assoc moment :food (update (xdb/freeze! (:fruits moment))
                                                           :names conj
                                                           "eggs" "rice" "fish"))]
                    moment)))

      (is (= {:fruits {:names ["apple" "pear" "grape"]}
              :food {:names ["apple" "pear" "grape" "eggs" "rice" "fish"]}}
             (xdb/materialize @db))))))

(deftest freeze-hash-set-test
  (testing "without freeze"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {})

      (swap! db (fn [moment]
                  (let [moment (assoc moment :fruits #{"apple" "pear" "grape"})
                        moment (assoc moment :food (:fruits moment))
                        moment (update moment :food conj "eggs" "rice" "fish")]
                    moment)))

      (is (= {:fruits #{"apple" "pear" "grape" "eggs" "rice" "fish"}
              :food #{"apple" "pear" "grape" "eggs" "rice" "fish"}}
             (xdb/materialize @db)))))

  (testing "with freeze"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {})

      (swap! db (fn [moment]
                  (let [moment (assoc moment :fruits #{"apple" "pear" "grape"})
                        moment (assoc moment :food (xdb/freeze! (:fruits moment)))
                        moment (update moment :food conj "eggs" "rice" "fish")]
                    moment)))

      (is (= {:fruits #{"apple" "pear" "grape"}
              :food #{"apple" "pear" "grape" "eggs" "rice" "fish"}}
             (xdb/materialize @db)))))

  (testing "with freeze and modifying return value"
    (with-open [db (xdb/xit-db :memory)]
      (reset! db {})

      (swap! db (fn [moment]
                  (let [moment (assoc moment :fruits #{"apple" "pear" "grape"})
                        moment (assoc moment :food (conj (xdb/freeze! (:fruits moment))
                                                         "eggs" "rice" "fish"))]
                    moment)))

      (is (= {:fruits #{"apple" "pear" "grape"}
              :food #{"apple" "pear" "grape" "eggs" "rice" "fish"}}
             (xdb/materialize @db))))))

