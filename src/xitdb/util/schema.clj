(ns xitdb.util.schema
  (:require
    [malli.core :as m]
    [malli.util :as mu]))


(defn schema-map-keys [schema]
  (when (= (m/type schema) :map)
    (mapv first (m/children schema))))

(defn- map-entry-schema
  "Pick the schema for one key inside a [:map …] entry vector.
   Handles both [:key child-schema] and
   [:key {:optional true …} child-schema] shapes."
  [entry]
  ;; entry looks like [:user {:optional true} [:map …]]       (3+ elems)
  ;; or [:user [:map …]]                                      (2 elems)
  (let [[_ maybe-meta schema] (concat entry [nil nil])]      ; pad with nils
    (if (map? maybe-meta) schema maybe-meta)))

(defn sub-schema
  "Returns the sub-schema found by following `path` inside `schema`."
  [schema path]
  (loop [sch  schema
         ks   (seq path)]
    (if (nil? ks)
      sch
      (let [k  (first ks)
            ks (next ks)]
        (case (m/type sch)
          :map     (let [next-sch (->> (rest sch)                 ; skip :map tag
                                       (some #(when (= k (first %))
                                                (map-entry-schema %))))]
                     (recur next-sch ks))

          ;; ------------------------------------ map-of (key schema _ value schema)
          :map-of  (let [[_ _ value-sch] sch]
                     (recur value-sch ks))

          ;; ------------------------------------ vectors / lists / seqs
          (:vector :sequential :list :set)
          (let [[_ elem-sch] sch]
            (recur elem-sch ks))

          ;; ------------------------------------ unsupported?
          (throw (ex-info "Don't know how to step into this schema type"
                          {:type (m/type sch) :schema sch})))))))


(defn index-of-key-in-schema [schema key]
  (when schema
    (let [children (m/children schema)]
      (some->> children
               (map-indexed (fn [idx [entry-key _props _schema]]
                              (when (= entry-key key)
                                idx)))
               (remove nil?)
               first))))


(defn map-schema? [schema]
  (= :map (some-> schema m/type)))

(defn extract-read-schema
  "Extracts a schema at the given keypath from a schema map.
   For collection schemas (:set, :vector, :list, :sequential), returns the element schema.
   For :map-of schemas, returns the value schema.
   For other schemas, returns the schema itself."
  [schema-map keypath]
  (let [extracted (some-> schema-map (sub-schema keypath))
        type  (some-> extracted m/type)]
    (cond
      (some->> type (contains? #{:set :vector :list :sequential}))
      (second extracted)

      (some->> type (contains? #{:map-of}))
      (nth extracted 2)

      :else
      extracted)))