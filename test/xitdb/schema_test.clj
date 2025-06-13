(ns xitdb.schema-test
  (:require [xitdb.common :as common]
            [xitdb.db :as xdb]
            [malli.core :as m]
            [malli.util :as mu]
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

{:xdb/schema {[:users :*]          UserSchema
              [:users :* :address] ExtendedAddressSchema}}

(deftest extract-schema-test
  (let [schema-map {[:users :*]          UserSchema
                    [:users :* :address] ExtendedAddressSchema}]
    (is (= UserSchema (sch/extract-schema schema-map [:users "1234"])))
    (is (= ExtendedAddressSchema (sch/extract-schema schema-map [:users "1234" :address])))))

(deftest extract-schema-nested-test
  (let [schema-map {[:users :*] UserSchema}]
    (is (= UserSchema (sch/extract-schema schema-map [:users "1234"])))
    (is (= AddressSchema (sch/extract-schema schema-map [:users "1234" :address])))))


(deftest index-of-key-in-schema-test
  (let [schema-map {[:users :*] UserSchema}
        extracted (sch/extract-schema schema-map [:users "1234"])]
    (is (= UserSchema extracted))

    (is (= 0 (sch/index-of-key-in-schema extracted :first-name)))
    (is (= 1 (sch/index-of-key-in-schema extracted :last-name)))
    (is (= 2 (sch/index-of-key-in-schema extracted :address)))))


(map first (m/children UserSchema))

(deftest DbSwapTest
  (let [schema-map {[:users :*] UserSchema}
        db (xdb/xit-db :memory)]
    (binding [conv/schema-for-keypath (fn [keypath]
                                        (sch/extract-schema schema-map keypath))]
      (reset! db {:users {"12345" {:first-name "John"
                                   :last-name "Doe"
                                   :address {:street "123 Main St"
                                             :city "San Francisco"
                                             :zip 94107
                                             :lonlat [37.7749 -122.4194]}}}})
      (common/materialize @db))))