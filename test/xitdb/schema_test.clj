(ns xitdb.schema-test
  (:require [xitdb.common :as common]
            [xitdb.db :as xdb]
            [malli.core :as m]
            [malli.util :as mu]
            [xitdb.util.operations :as operations]
            [xitdb.util.schema :as sch]
            [xitdb.util.conversion :as conv]
            [clojure.test :refer :all]))

(def AddressSchema
  [:map
   [:street :string]
   [:city :string]
   [:zip :int]
   [:lonlat [:tuple :double :double]]])

(def UserSchema
  [:map
   [:first-name :string]
   [:last-name :string]
   [:address AddressSchema]])

(def ExtendedAddressSchema
  [:map
   [:street :string]
   [:city :string]
   [:zip :int]
   [:lonlat [:tuple :double :double]]
   [:country :string]])

(deftest extract-schema-test
  (let [schema-map {[:users :*]          UserSchema
                    [:users :* :address] ExtendedAddressSchema
                    [:coll]           [:set [:map
                                             [:name :string]
                                             [:price :int]]]}]
    (is (= UserSchema (sch/extract-schema schema-map [:users "1234"])))
    (is (= ExtendedAddressSchema (sch/extract-schema schema-map [:users "1234" :address])))
    (sch/extract-schema schema-map [:coll])))

(deftest extract-schema-nested-test
  (let [schema-map {[:users :*] UserSchema}]
    (is (= UserSchema (sch/extract-schema schema-map [:users "1234"])))
    (is (= AddressSchema (sch/extract-schema schema-map [:users "1234" :address])))))


(deftest index-of-key-in-schema-test
  (let [schema-map {[:users :*] UserSchema}
        extracted  (sch/extract-schema schema-map [:users "1234"])]
    (is (= UserSchema extracted))

    (is (= 0 (sch/index-of-key-in-schema extracted :first-name)))
    (is (= 1 (sch/index-of-key-in-schema extracted :last-name)))
    (is (= 2 (sch/index-of-key-in-schema extracted :address)))))

(def UserRecord {:first-name "John"
                 :last-name  "Doe"
                 :address    {:street "123 Main St"
                              :city   "San Francisco"
                              :zip    94107
                              :lonlat [37.7749 -122.4194]}})

(deftest DBReadingTest
  (let [schema-map {[:users :*] UserSchema}
        db         (xdb/xit-db :memory)]
    (binding [conv/*current-schema* schema-map]
      (reset! db {:users {"12345" UserRecord}})

      (testing "Should be equal to stored record"
        (is (= {:users {"12345" UserRecord}} (common/materialize @db))))

      (testing "Should show the hidden xdb/values array"
        (binding [operations/*show-hidden-keys?* true]
          (is (= {:users {"12345"
                          #:xdb{:values ["John" "Doe" #:xdb{:values ["123 Main St" "San Francisco" 94107 [37.7749 -122.4194]]}]}}}
                 (common/materialize @db))))))))

;; Complex nested schema test
(def CompanySchema
  [:map
   [:name :string]
   [:founded :int]])

(def Equipment
  [:map
   [:name :string]
   [:price :int]])

(def DepartmentSchema
  [:map
   [:name :string]
   [:budget :int]
   [:equipment [:set Equipment]]])

(def ComplexCompanyRecord
  {:name        "TechCorp"
   :founded     2010
   :departments {"engineering" {:name      "Engineering"
                                :budget    1000000
                                :equipment #{{:name "High-End Workstation", :price 3500}
                                             {:name "Server Rack", :price 12000}
                                             {:name "Development Licenses", :price 5000}}}
                 "marketing"   {:name      "Marketing"
                                :budget    500000
                                :equipment #{{:name "DSLR Camera", :price 2200}
                                             {:name "Exhibition Booth", :price 8500}}}}})

(deftest SetMapTest
  (let [db (xdb/xit-db :memory)
        dbval {:colls {"1" {:name  "My Coll"
                            :items #{{:name "Keys" :price 20}
                                     {:name "Seyk" :price 32}}}}}]
    (binding [conv/*current-schema* {[:colls :*] [:map
                                                  [:name :string]
                                                  [:items [:set [:map
                                                                 [:name :string]
                                                                 [:price :int]]]]]}]
      (reset! db {:colls {"1" {:name  "My Coll"
                               :items #{{:name "Keys" :price 20}
                                        {:name "Seyk" :price 32}}}}})

      (is (= dbval (common/materialize @db)))

      (binding [operations/*show-hidden-keys?* true]
        (is (= {:colls {"1" #:xdb{:values ["My Coll" #{#:xdb{:values ["Seyk" 32]} #:xdb{:values ["Keys" 20]}}]}}}
               (common/materialize @db)))))))


(deftest NestedKeypathTest
  (let [complex-schema-map {[:companies :*]                               CompanySchema
                            [:companies :* :departments :*]               DepartmentSchema
                            [:companies :* :departments :* :equipment :*] Equipment}
        db                 (xdb/xit-db :memory)]
    (binding [conv/*current-schema* complex-schema-map]
      (reset! db {:companies {"techcorp" ComplexCompanyRecord}})

      (binding [operations/*show-hidden-keys?* false]
        (common/materialize @db))

      #_(testing "Complex nested schema optimization works"
          (let [materialized (common/materialize @db)]
            (println "Expected:" ComplexCompanyRecord)
            (println "Actual:" (get-in materialized [:companies "techcorp"]))
            (is (= {:companies {"techcorp" ComplexCompanyRecord}} materialized))))

      #_(testing "Shows nested :xdb/values arrays in debug mode"
          (binding [operations/*show-hidden-keys?* true]
            (let [debug-result     (common/materialize @db)
                  company          (get-in debug-result [:companies "techcorp"])
                  engineering-dept (get-in company [:departments "engineering"])]

              ;; Company should use schema optimization
              (is (contains? company :xdb/values))
              (is (= "TechCorp" (first (get company :xdb/values))))

              ;; Department should use schema optimization
              (is (contains? engineering-dept :xdb/values))
              (is (= "Engineering" (first (get engineering-dept :xdb/values))))

              ;; Equipment set should be stored in the :xdb/values array at index 2
              (let [dept-values   (get engineering-dept :xdb/values)
                    equipment-set (nth dept-values 2)]
                (is (set? equipment-set))
                (is (= 3 (count equipment-set))))))

          (testing "Schema-optimized fields are properly reconstructed"
            ;; Test without debug mode to see normal reconstruction
            (let [normal-result    (common/materialize @db)
                  company          (get-in normal-result [:companies "techcorp"])
                  engineering-dept (get-in company [:departments "engineering"])]
              (is (= "Engineering" (:name engineering-dept)))
              (is (= 1000000 (:budget engineering-dept)))
              (is (= 3 (count (:equipment engineering-dept))))
              (is (set? (:equipment engineering-dept)))))

          (testing "Keypath patterns match correctly"
            ;; Test that different keypath patterns are resolved correctly
            (is (= CompanySchema (sch/extract-schema complex-schema-map [:companies "techcorp"])))
            (is (= DepartmentSchema (sch/extract-schema complex-schema-map [:companies "techcorp" :departments "engineering"])))
            (let [equipment-schema (sch/extract-schema complex-schema-map [:companies "techcorp" :departments "engineering" :equipment :*])]
              (println "Equipment schema found:" equipment-schema)
              (is (= Equipment equipment-schema))))))))