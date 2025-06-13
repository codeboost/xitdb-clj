(ns xitdb.util.schema
  (:require
    [malli.core :as m]
    [malli.util :as mu]))

(defn extract-schema [schema-map keypath]
  (let [exact-matching-patterns (filter
                                  (fn [pattern]
                                    (and (= (count pattern) (count keypath))
                                         (every? true?
                                                 (map (fn [p k] (or (= p :*) (= p k)))
                                                      pattern keypath))))
                                  (keys schema-map))]
    (if (seq exact-matching-patterns)
      (let [best-pattern (apply max-key (fn [pattern] (count (remove #(= % :*) pattern))) exact-matching-patterns)
            result (get schema-map best-pattern)]
        (if (m/schema? result) (m/form result) result))
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
                remaining-path (drop (count best-pattern) keypath)
                result         (mu/get-in base-schema remaining-path)]
            (if (m/schema? result) (m/form result) result)))))))

(defn index-of-key-in-schema [schema key]
  (when schema
    (let [children (m/children schema)]
      (some->> children
               (map-indexed (fn [idx [entry-key _props _schema]]
                              (when (= entry-key key)
                                idx)))
               (remove nil?)
               first))))

(defn schema-keys [schema]
  (map first (m/children schema)))