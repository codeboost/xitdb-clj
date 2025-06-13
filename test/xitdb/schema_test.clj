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

{:xdb/schema {[:users :*]          UserSchema
              [:users :* :address] ExtendedAddressSchema}}

(defn extract-schema [schema-map keypath]
  (let [exact-matching-patterns (filter
                                  (fn [pattern]
                                    (and (= (count pattern) (count keypath))
                                         (every? true?
                                                 (map (fn [p k] (or (= p :*) (= p k)))
                                                      pattern keypath))))
                                  (keys schema-map))]
    (if (seq exact-matching-patterns)
      (let [best-pattern (apply max-key (fn [pattern] (count (remove #(= % :*) pattern))) exact-matching-patterns)]
        (get schema-map best-pattern))
      (let [shorter-patterns (filter
                               (fn [pattern]
                                 (and (< (count pattern) (count keypath))
                                      (every? true?
                                              (map (fn [p k] (or (= p :*) (= p k)))
                                                   pattern (take (count pattern) keypath)))))
                               (keys schema-map))]
        (when (seq shorter-patterns)
          (let [best-pattern   (apply max-key (fn [pattern] (count (remove #(= % :*) pattern))) shorter-patterns)
                base-schema    (get schema-map best-pattern)
                remaining-path (drop (count best-pattern) keypath)]
            (mu/get-in base-schema remaining-path)))))))

(deftest extract-schema-test
  (let [schema-map {[:users :*]          UserSchema
                    [:users :* :address] ExtendedAddressSchema}]
    (is (= UserSchema (extract-schema schema-map [:users "1234"])))
    (is (= ExtendedAddressSchema (extract-schema schema-map [:users "1234" :address])))))

(deftest extract-schema-nested-test
  (let [schema-map {[:users :*] UserSchema}]
    (is (= UserSchema (extract-schema schema-map [:users "1234"])))
    (let [extracted (extract-schema schema-map [:users "1234" :address])]
      (is (= AddressSchema (m/form extracted))))))




