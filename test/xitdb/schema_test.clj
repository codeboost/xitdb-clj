(ns xitdb.schema-test
  (:require [xitdb.common :as common]
            [xitdb.db :as xdb]
            [malli.core :as m]
            [xitdb.util.operations :as operations]
            [xitdb.util.schema :as schema]
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
(def ComplexCompanySchema
  [:map
   [:name :string]
   [:founded :int]
   [:departments [:map-of :string DepartmentSchema]]])

(defn UserRecord
  "A sample user record conforming to UserSchema."
  []
  {:first-name "John"
   :last-name  "Doe"
   :address    {:street "123 Main St"
                :city   "Springfield"
                :zip    12345
                :lonlat [42.3601 -71.0589]}})

(deftest UsertRecordTest
  (let [db (xdb/xit-db :memory)]
    (binding [conv/*current-schema* UserSchema]
      (reset! db (UserRecord))
      (is (= (UserRecord)
             (common/materialize @db))))))


(deftest DbWithSchemaTest
  (let [db (xdb/xit-db :memory)]
    (binding [conv/*current-schema* ComplexCompanySchema]
      (reset! db ComplexCompanyRecord)
      (binding [operations/*show-hidden-keys?* false]
        (is (= ComplexCompanyRecord
               (common/materialize @db)))))))

(deftest simple-nested-test
  (let [schema [:map
                [:addresses [:set [:map
                                   [:street :string]
                                   [:city :string]
                                   [:zipcode :int]]]]]
        dbval {:addresses #{{:street "123 Main St" :city "Boston" :zipcode 12345}
                            {:street "456 Elm St" :city "Cambridge" :zipcode 67890}
                            {:street "343 Elm St" :city "Foo" :zipcode 54}}}
        db (xdb/xit-db :memory)]
    (binding [conv/*current-schema* schema]
      (reset! db dbval)
      (is (= dbval
             (common/materialize @db))))))

(deftest simple-nested-map
  (let [schema [:map
                [:addresses [:map-of :keyword [:map
                                               [:street :string]
                                               [:city :string]
                                               [:zipcode :int]]]]]
        dbval  {:addresses {:home  {:street "123 Main St" :city "Boston" :zipcode 12345}
                            :work  {:street "456 Elm St" :city "Cambridge" :zipcode 67890}
                            :other {:street "343 Elm St" :city "Foo" :zipcode 54}}}
        db     (xdb/xit-db :memory)]
    (is (m/validate schema dbval))
    (binding [conv/*current-schema* schema]
      (reset! db dbval)

      (testing "stored as xdb/values?"
        (binding [operations/*show-hidden-keys?* true]
          (is (= #:xdb{:values [{:home #:xdb{:values ["123 Main St" "Boston" 12345]},
                                 :work #:xdb{:values ["456 Elm St" "Cambridge" 67890]},
                                 :other #:xdb{:values ["343 Elm St" "Foo" 54]}}]}
                 (common/materialize @db)))))

      (testing "Correctly reconstructs?"
        (is (= dbval (common/materialize @db))))

      (common/materialize @db))))


(deftest complex-nested-map
  (let [schema [:map [:people
                      [:map-of :string
                       [:map
                        [:addresses [:map-of :keyword [:map
                                                       [:street :string]
                                                       [:city :string]
                                                       [:zipcode :int]]]]]]]]
        dbval  {:people {"john" {:addresses {:home  {:street "123 Main St" :city "Boston" :zipcode 12345}
                                             :work  {:street "456 Elm St" :city "Cambridge" :zipcode 67890}
                                             :other {:street "343 Elm St" :city "Foo" :zipcode 54}}}
                         "jane" {:addresses {:home  {:street "789 Oak St" :city "Somerville" :zipcode 54321}
                                             :work  {:street "101 Pine St" :city "Arlington" :zipcode 98765}
                                             :other {:street "202 Maple St" :city "Lexington" :zipcode 11223}}}}}
        db     (xdb/xit-db :memory)]

    (is (m/validate schema dbval))
    (binding [conv/*current-schema* schema]
      (reset! db dbval)
      (testing "Correctly reconstructs?"
        (is (= dbval (common/materialize @db))))
      (testing "Stores correctly"
        (binding [operations/*show-hidden-keys?* true]
          (is (= #:xdb{:values [{"jane" #:xdb{:values [{:home #:xdb{:values ["789 Oak St" "Somerville" 54321]},
                                                        :work #:xdb{:values ["101 Pine St" "Arlington" 98765]},
                                                        :other #:xdb{:values ["202 Maple St" "Lexington" 11223]}}]},
                                 "john" #:xdb{:values [{:home #:xdb{:values ["123 Main St" "Boston" 12345]},
                                                        :work #:xdb{:values ["456 Elm St" "Cambridge" 67890]},
                                                        :other #:xdb{:values ["343 Elm St" "Foo" 54]}}]}}]}
                 (common/materialize @db)))))


      (testing "get-in works"
        (is (= 11223
               (get-in @db [:people "jane" :addresses :other :zipcode])))))))


(deftest flexi-schema-test
  (let [schema [:map-of :keyword [:map
                                  [:street :string]
                                  [:city :string]
                                  [:zipcode :int]]]
        dbval  {:home  {:street "123 Main St" :city "Boston" :zipcode 12345}
                :work  {:street "456 Elm St" :city "Cambridge" :zipcode 67890 :country "USA"}
                :other {:street "343 Elm St" :city "Foo" :zipcode 54 "state" "MA"}}
        db     (xdb/xit-db :memory)]

    (is (m/validate schema dbval))
    (binding [conv/*current-schema* schema]
      (reset! db dbval)
      (testing "Correctly reconstructs?"
        (is (= dbval (common/materialize @db))))

      (testing "Stores extra keys as map keys"
        (binding [operations/*show-hidden-keys?* true]
          (is (= {:home #:xdb{:values ["123 Main St" "Boston" 12345]},
                  :work {:country "USA", :xdb/values ["456 Elm St" "Cambridge" 67890]},
                  :other {"state" "MA", :xdb/values ["343 Elm St" "Foo" 54]}}

                 (common/materialize @db))))))))



