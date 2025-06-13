(ns xitdb.schema-test
  (:require [xitdb.common :as common]
            [xitdb.db :as xdb]
            [malli.core :as m]
            [malli.util :as mu]
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

{:xdb/schema {[:users :*] UserSchema
              [:users :* :address] ExtendedAddressSchema}}

(defn extract-schema [schema-map keypath]
  (let [matching-patterns (filter
                            (fn [pattern]
                              (and (= (count pattern) (count keypath))
                                (every? true?
                                        (map (fn [p k] (or (= p :*) (= p k)))
                                             pattern keypath))))
                            (keys schema-map))]
    (when (seq matching-patterns)
      (let [best-pattern (apply max-key (fn [pattern] (count (remove #(= % :*) pattern))) matching-patterns)]
        (get schema-map best-pattern)))))

(deftest extract-schema-test
  (let [schema-map {[:users :*] UserSchema
                    [:users :* :address] ExtendedAddressSchema}]
    (is (= UserSchema (extract-schema schema-map [:users "1234"])))
    (is (= ExtendedAddressSchema (extract-schema schema-map [:users "1234" :address])))))

(deftest extract-schema-nested-test
  (let [schema-map {[:users :*] UserSchema}]
    (is (= UserSchema (extract-schema schema-map [:users "1234"])))
    (is (= AddressSchema (extract-schema schema-map [:users "1234" :address])))))

