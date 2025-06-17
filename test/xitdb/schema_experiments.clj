(ns xitdb.schema-experiments
  (:require
    [clojure.test :refer :all]
    [xitdb.util.schema :as schema]))

(def UserSchema
  [:map
   [:user [:map
           [:id :int]
           [:name :string]
           [:metadata [:map-of :keyword :string]]

           [:devices [:map-of :string [:map [:type :string]
                                       [:model :string]
                                       [:os :string]]]]
           [:addresses [:vector [:map
                                 [:street :string]
                                 [:city :string]
                                 [:zipcode :int]]]]

           [:roles [:set [:map
                          [:name :string]
                          [:permissions [:set :string]]]]]]]
   [:stats [:map-of :string :int]]
   [:timestamp :int]])


(def UserValue
  {:user      {:id        123
               :name      "John Doe"
               :metadata  {:department "Engineering"
                           :level      "Senior"
                           :location   "Remote"}
               :devices   {"laptop" {:type  "laptop"
                                     :model "Dell XPS 15"
                                     :os    "Windows 10"}}
               :addresses [{:street  "123 Main St"
                            :city    "Boston"
                            :zipcode 12345}]}
   :stats     {"logins"   42
               "posts"    15
               "comments" 8}
   :timestamp 1640995200})


(deftest sub-schema-test
  (is (= [:map [:street :string] [:city :string] [:zipcode :int]])
      (schema/sub-schema UserSchema [:user :addresses 0]))

  (is (= [:map-of :string [:map [:type :string] [:model :string] [:os :string]]]
         (schema/sub-schema UserSchema [:user :devices])))


  (is (= [:map [:type :string] [:model :string] [:os :string]]
         (schema/sub-schema UserSchema [:user :devices "foo"])
         (schema/sub-schema UserSchema [:user :devices "laptop"]))))



