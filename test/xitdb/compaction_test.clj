(ns xitdb.compaction-test
  (:require
    [clojure.java.io :as io]
    [clojure.test :refer :all]
    [xitdb.db :as xdb]
    [xitdb.sorted :as sorted])
  (:import
    [java.nio.file FileAlreadyExistsException Files]
    [java.nio.file.attribute FileAttribute]
    [java.time Instant]
    [java.security MessageDigest]
    [java.util Date UUID]))

(def ^:dynamic *temp-dir* nil)

(use-fixtures :each
  (fn [f]
    (let [dir (.toFile (Files/createTempDirectory "xitdb-compaction" (make-array FileAttribute 0)))]
      (try
        (binding [*temp-dir* dir] (f))
        (finally
          (doseq [file (reverse (file-seq dir))]
            (io/delete-file file)))))))

(defn- new-path []
  (.getPath (io/file *temp-dir* (str (UUID/randomUUID) ".xdb"))))

(defn- location [kind]
  (if (= :memory kind) :memory (new-path)))

(def ^:private storage-pairs
  [[:memory :memory] [:memory :file] [:file :memory] [:file :file]])

(def ^:private values
  [["nil" nil]
   ["boolean" false]
   ["integer" Long/MIN_VALUE]
   ["floating point" -12.5]
   ["short string" "hi"]
   ["long Unicode string" (apply str (repeat 3000 "λ🌍"))]
   ["keyword" :compaction/value]
   ["character" \λ]
   ["UUID" #uuid "123e4567-e89b-12d3-a456-426614174000"]
   ["instant" (Instant/parse "2026-01-02T03:04:05.123456789Z")]
   ["date" (Date. 123456789)]
   ["empty map" {}]
   ["empty vector" []]
   ["empty list" '()]
   ["empty set" #{}]
   ["empty sorted map" (sorted-map)]
   ["empty sorted set" (sorted-set)]
   ["hash map" {nil nil :a 1 "a" false true :yes \x "char"
                 #uuid "123e4567-e89b-12d3-a456-426614174000" :uuid
                 [1 2] {:collection-key true}}]
   ["vector" [nil true false 1 -2 3.5 "hello" :ns/key \x]]
   ["list" '(nil true false 1 -2 3.5 "hello" :ns/key)]
   ["hash set" #{nil true false 1 "one" :one [1 2]}]
   ["sorted map" (sorted-map 3 {:nested [1 2]} 1 nil 2 #{:a :b})]
   ["sorted set" (sorted-set "z" "a" "λ")]
   ["nested collections"
    {:vector [{:list (list #{:a :b} (sorted-map 2 [nil] 1 []))}]
     :set #{[1 2] {:a [3 4]}}
     :sorted (sorted-map "a" (sorted-set 3 1 2) "b" {:empty []})}]
   ["deep nesting" (reduce (fn [v i] {i [v]}) :leaf (range 40))]])

(deftest compact-preserves-values-test
  (doseq [[source-kind target-kind] storage-pairs
          [label value] values]
    (testing (str label ", " source-kind " -> " target-kind)
      (let [target (location target-kind)]
        (with-open [source (xdb/xit-db (location source-kind))]
          (reset! source {:obsolete "discard this history"})
          (reset! source value)
          (let [original-type (type @source)]
            (with-open [compacted (xdb/compact source target)]
              (is (= value (xdb/materialize @compacted)))
              (is (= original-type (type @compacted)))
              (is (= 1 (count compacted)))
              (is (= value (xdb/materialize (xdb/deref-at compacted 0))))
              (reset! compacted {:new "transaction"})
              (is (= 2 (count compacted)))
              (is (= value (xdb/materialize (xdb/deref-at compacted 0)))))
            (is (= 2 (count source)))
            (is (= value (xdb/materialize @source)))
            (is (= {:obsolete "discard this history"}
                   (xdb/materialize (xdb/deref-at source 0))))))
        (when (= :file target-kind)
          (with-open [reopened (xdb/xit-db target)]
            (is (= {:new "transaction"} (xdb/materialize @reopened)))
            (is (= value (xdb/materialize (xdb/deref-at reopened 0))))))))))

(deftest compact-empty-database-test
  (doseq [[source-kind target-kind] storage-pairs]
    (testing (str source-kind " -> " target-kind)
      (let [target (location target-kind)]
        (with-open [source (xdb/xit-db (location source-kind))]
          (with-open [compacted (xdb/compact source target)]
            (is (zero? (count compacted)))
            (is (nil? @compacted))
            (reset! compacted {:first true})
            (is (= 1 (count compacted)))
            (is (= {:first true} (xdb/materialize @compacted))))
          (is (zero? (count source)))
          (is (nil? @source)))
        (when (= :file target-kind)
          (with-open [reopened (xdb/xit-db target)]
            (is (= {:first true} (xdb/materialize @reopened)))
            (is (= 1 (count reopened)))))))))

(deftest compact-large-collections-remain-writable-test
  ;; Cross the array-index and B-tree node boundaries, then exercise writes
  ;; through the copied pointers and verify the compacted snapshot survives.
  (doseq [[source-kind target-kind] storage-pairs
          [label value mutate] [["map" (into {} (map (fn [i] [i {:value i}]) (range 300)))
                                 #(assoc (dissoc % 150) 301 {:value :new})]
                                ["vector" (vec (range 300)) #(conj (assoc % 150 :changed) 300)]
                                ["list" (apply list (range 300)) #(conj (pop %) :new)]
                                ["set" (set (range 300)) #(conj (disj % 150) 301)]
                                ["sorted map" (into (sorted-map) (map (fn [i] [i {:value i}]) (range 300)))
                                 #(assoc (dissoc % 150) 301 {:value :new})]
                                ["sorted set" (into (sorted-set) (range 300)) #(conj (disj % 150) 301)]]]
    (testing (str label ", " source-kind " -> " target-kind)
      (let [target (location target-kind)
            changed (mutate value)]
        (with-open [source (xdb/xit-db (location source-kind))]
          (reset! source value)
          (swap! source mutate)
          (with-open [compacted (xdb/compact source target)]
            (is (= changed (xdb/materialize @compacted)))
            (is (= (count changed) (count @compacted)))
            (when (sorted? value)
              (is (sorted? @compacted))
              (is (= (seq changed) (xdb/materialize (seq @compacted))))
              (is (= (rseq changed) (xdb/materialize (rseq @compacted))))
              (is (= (subseq changed >= 145 <= 155) (xdb/materialize (subseq @compacted >= 145 <= 155))))
              (is (= (nth (vec changed) 151) (xdb/materialize (nth @compacted 151))))
              (is (= 150 (sorted/rank @compacted 151)))
              (is (= (take 5 (drop 148 changed)) (xdb/materialize (sorted/page @compacted 148 5)))))
            (swap! compacted mutate)
            (is (= (mutate changed) (xdb/materialize @compacted)))
            (is (= changed (xdb/materialize (xdb/deref-at compacted 0)))))
          (is (= changed (xdb/materialize @source)))
          (is (= value (xdb/materialize (xdb/deref-at source 0)))))
        (when (= :file target-kind)
          (with-open [reopened (xdb/xit-db target)]
            (is (= (mutate changed) (xdb/materialize @reopened)))))))))

(deftest compact-nested-cursors-and-shared-values-test
  (with-open [source (xdb/xit-db :memory)]
    (reset! source {:left {:items [1 2] :tags (sorted-set 1 2)}})
    (swap! source #(assoc % :right (xdb/freeze! (:left %))))
    (with-open [compacted (xdb/compact source :memory)]
      (let [before (xdb/materialize @source)]
        (swap! (xdb/xdb-cursor compacted [:left :items]) conj 3)
        (swap! (xdb/xdb-cursor compacted [:left :tags]) conj 3)
        (is (= {:items [1 2 3] :tags (sorted-set 1 2 3)}
               (xdb/materialize (:left @compacted))))
        (is (= (:right before) (xdb/materialize (:right @compacted))))
        (is (= before (xdb/materialize (xdb/deref-at compacted 0)))))
        (swap! (xdb/xdb-cursor source [:right :items]) conj 4)
        (is (= [1 2] (xdb/materialize (get-in @compacted [:right :items]))))
        (is (= [1 2] (xdb/materialize (get-in @source [:left :items])))))))

(deftest compact-reclaims-space-and-survives-source-deletion-test
  (let [source-path (new-path)
        target (new-path)
        expected {:kept (vec (range 100))}]
    (with-open [source (xdb/xit-db source-path)]
      (dotimes [i 40]
        (reset! source {:version i :obsolete (apply str (repeat 10000 (str i)))}))
      (reset! source expected)
      (with-open [compacted (xdb/compact source target)]
        (is (= expected (xdb/materialize @compacted)))
        (is (= 1 (count compacted))))
      (is (= 41 (count source))))
    (is (< (.length (io/file target)) (.length (io/file source-path))))
    (io/delete-file source-path)
    (with-open [reopened (xdb/xit-db target)]
      (is (= expected (xdb/materialize @reopened)))
      (with-open [again (xdb/compact reopened :memory)]
        (is (= expected (xdb/materialize @again)))
        (is (= 1 (count again))))
      (swap! reopened assoc :independent true)
      (is (= (assoc expected :independent true) (xdb/materialize @reopened))))))

(deftest compact-rejects-existing-targets-test
  (let [source-path (new-path)
        target (new-path)]
    (spit target "do not overwrite")
    (with-open [source (xdb/xit-db source-path)]
      (reset! source {:source true})
      (doseq [path [source-path target (.getPath *temp-dir*)]]
        (is (thrown? FileAlreadyExistsException (xdb/compact source path))))
      (is (= "do not overwrite" (slurp target)))
      (is (= {:source true} (xdb/materialize @source)))
      ;; A rejected target must release the source's transaction lock.
      (is (= {:source true :usable true} (xdb/materialize (swap! source assoc :usable true)))))))

(deftest compact-rejects-reentrant-transactions-test
  (with-open [source (xdb/xit-db :memory)]
    (reset! source {:value 1})
    (let [target (new-path)]
      (is (thrown-with-msg? IllegalStateException #"compact should not be called"
                           (swap! source (fn [value]
                                           (xdb/compact source target)
                                           value))))
      (is (not (.exists (io/file target))))
      (is (= {:value 1} (xdb/materialize @source)))
      (is (= 1 (count source)))
      (with-open [compacted (xdb/compact source target)]
        (is (= {:value 1} (xdb/materialize @compacted)))))))

(deftest compact-source-and-target-hash-independently-test
  (with-open [source (xdb/xit-db :memory)]
    (reset! source {})
    (let [delegate (MessageDigest/getInstance "SHA-1")
          pause-once? (atom true)
          hashing-started (promise)
          target-written (promise)
          pause (fn []
                  (when (compare-and-set! pause-once? true false)
                    (deliver hashing-started true)
                    (when (= ::timeout (deref target-written 5000 ::timeout))
                      (throw (ex-info "Timed out waiting for target write" {})))))
          digest (proxy [MessageDigest] ["SHA-1"]
                   (engineGetDigestLength [] 20)
                   (engineUpdate
                     ([b] (.update delegate (byte b)) (pause))
                     ([b offset length] (.update delegate b offset length) (pause)))
                   (engineDigest [] (.digest delegate))
                   (engineReset [] (.reset delegate)))]
      ;; Pause a source write midway through hashing its key. A target write
      ;; must not consume or reset that partial hash, even though locks differ.
      (set! (.-md (.-rwdb source)) digest)
      (with-open [compacted (xdb/compact source :memory)]
        (let [writer (future (reset! source {:left 1}))]
          (try
            (is (= true (deref hashing-started 5000 ::timeout)))
            (reset! compacted {:right 2})
            (finally
              (deliver target-written true)))
          (try
            (is (not= ::timeout (deref writer 5000 ::timeout)))
            (is (= 1 (get @source :left)))
            (is (= 2 (get @compacted :right)))
            (finally
              (future-cancel writer))))))))

(deftest compact-cleans-up-failed-copy-test
  (let [source-path (new-path)
        target (new-path)
        source (xdb/xit-db source-path)]
    (reset! source {:data "requires reading the source file"})
    (.close source)
    (is (thrown? java.io.IOException (xdb/compact source target)))
    (is (not (.exists (io/file target))))
    ;; The failed attempt must leave the destination available for a retry.
    (with-open [reopened (xdb/xit-db source-path)
                compacted (xdb/compact reopened target)]
      (is (= {:data "requires reading the source file"} (xdb/materialize @compacted))))))
