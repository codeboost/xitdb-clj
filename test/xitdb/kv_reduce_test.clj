(ns xitdb.kv-reduce-test
  (:require
    [clojure.test :refer :all]
    [xitdb.test-utils :as tu :refer [with-db]]))

(deftest kv-reduce-test
  (with-db [db (tu/test-db)]
    (testing "IKVReduce implementation for XITDBHashMap"
      (reset! db {:a 1 :b 2 :c 3})
      
      ;; Test basic reduce operation
      (let [sum (reduce-kv (fn [acc k v] (+ acc v)) 0 @db)]
        (is (= 6 sum)))
      
      ;; Test early termination with reduced
      (let [first-key (reduce-kv (fn [acc k v] (reduced k)) nil @db)]
        (is (contains? #{:a :b :c} first-key)))
      
      ;; Test key-value accumulation
      (let [kvs (reduce-kv (fn [acc k v] (conj acc [k v])) [] @db)]
        (is (= 3 (count kvs)))
        (is (every? #(and (keyword? (first %)) (number? (second %))) kvs)))
      
      ;; Test with nested maps
      (reset! db {:outer {:inner {:value 42}}})
      (let [nested-value (reduce-kv (fn [acc k v] 
                                     (if (= k :outer)
                                       (reduce-kv (fn [acc2 k2 v2]
                                                   (if (= k2 :inner) 
                                                     (get v2 :value)
                                                     acc2)) acc v)
                                       acc)) 
                                   nil @db)]
        (is (= 42 nested-value))))))

(deftest array-kv-reduce-test
  (with-db [db (tu/test-db)]
    (testing "IKVReduce implementation for XITDBArrayList"
      (reset! db [10 20 30 40])
      
      ;; Test basic reduce-kv operation (sum of indices * values)
      (let [weighted-sum (reduce-kv (fn [acc idx val] (+ acc (* idx val))) 0 @db)]
        (is (= 200 weighted-sum))) ; 0*10 + 1*20 + 2*30 + 3*40 = 0 + 20 + 60 + 120 = 200
      
      ;; Test early termination with reduced
      (let [first-val (reduce-kv (fn [acc idx val] (reduced val)) nil @db)]
        (is (= 10 first-val)))
      
      ;; Test index-value accumulation
      (let [idx-vals (reduce-kv (fn [acc idx val] (conj acc [idx val])) [] @db)]
        (is (= [[0 10] [1 20] [2 30] [3 40]] idx-vals)))
      
      ;; Test with nested vectors
      (reset! db [[1 2] [3 4] [5 6]])
      (let [sum-by-index (reduce-kv (fn [acc idx inner-vec] 
                                     (+ acc (reduce-kv (fn [acc2 idx2 val2] 
                                                        (+ acc2 (* idx idx2 val2))) 
                                                      0 inner-vec)))
                                   0 @db)]
        ;; idx=0: 0*0*1 + 0*1*2 = 0
        ;; idx=1: 1*0*3 + 1*1*4 = 4  
        ;; idx=2: 2*0*5 + 2*1*6 = 12
        ;; Total: 16
        (is (= 16 sum-by-index))))))