(ns xitdb.compat-mode-test
  "Compatibility mode, exercised with real files written and read through the
  xitdb-java API."
  (:require
    [clojure.java.io :as io]
    [clojure.test :refer :all]
    [xitdb.db :as db]
    [xitdb.replication :as replication]
    [xitdb.util.conversion :as conversion])
  (:import
    [io.github.radarroark.xitdb
     CoreBufferedFile Database Database$Bytes Database$ContextFunction Database$Uint
     Database$UnexpectedTagException Hasher
     RandomAccessBufferedFile ReadArrayList ReadCountedHashMap ReadCountedHashSet ReadCursor
     ReadHashMap ReadHashSet ReadLinkedArrayList WriteArrayList
     WriteCountedHashMap WriteCountedHashSet WriteHashMap WriteHashSet]
    [java.io File]
    [java.nio.file Files]
    [java.security MessageDigest]
    [java.util Arrays]))

(def ^:private sha1-id (Hasher/stringToId "sha1"))
(def ^:private clj1-id (Hasher/stringToId "clj1"))

(defn- hasher [id]
  (Hasher. (MessageDigest/getInstance "SHA-1") (int id)))

(defn- temp-path
  "A path in the temp directory with nothing at it yet."
  ^String []
  (let [file (File/createTempFile "xitdb-compat-" ".db")]
    (.delete file)
    (.getPath file)))

(defmacro ^:private with-temp-paths
  "Binds each symbol to a fresh temp path and deletes the files afterwards."
  [syms & body]
  `(let [~@(mapcat (fn [s] [s `(temp-path)]) syms)]
     (try
       ~@body
       (finally
         ~@(map (fn [s] `(.delete (File. ~s))) syms)))))

(defn- file-bytes ^bytes [^String path]
  (Files/readAllBytes (.toPath (File. path))))

(deftest mode-option
  (with-temp-paths [default-path explicit-path unknown-path]
    (testing "a new database is in Clojure mode unless asked otherwise"
      (with-open [d (db/xit-db default-path)]
        (reset! d {:a 1})
        (is (= 1 (:a @d)))
        (is (= :clojure (db/db-mode d))))
      (with-open [d (db/xit-db explicit-path {:mode :clojure})]
        (reset! d {:a 1})
        (is (= 1 (:a @d)))
        (is (= :clojure (db/db-mode d))))
      (with-open [d (db/xit-db :memory)]
        (is (= :clojure (db/db-mode d))))
      (with-open [d (db/xit-db :memory {:mode :compat})]
        (is (= :compat (db/db-mode d)))))
    (testing "an unknown mode is refused before the file is created"
      (is (thrown-with-msg? IllegalArgumentException #":nope"
                            (db/xit-db unknown-path {:mode :nope})))
      (is (not (.exists (File. unknown-path))))
      (is (thrown? IllegalArgumentException (db/xit-db :memory {:mode :nope}))))))

(defn- header-id
  "The hash id in the header of the file at `path`, read through the Java API."
  [^String path]
  (let [core (CoreBufferedFile. (RandomAccessBufferedFile. (File. path) "r"))]
    (try
      (.hashId (.-header (Database. core (hasher 0))))
      (finally (.close core)))))

(deftest new-files-record-their-mode-in-the-header
  (with-temp-paths [clojure-path compat-path empty-path]
    (.close (db/xit-db clojure-path))
    (is (= clj1-id (header-id clojure-path)))
    (.close (db/xit-db compat-path {:mode :compat}))
    (is (= sha1-id (header-id compat-path)))
    (testing "an existing empty file counts as new"
      (.createNewFile (File. empty-path))
      (.close (db/xit-db empty-path {:mode :compat}))
      (is (= sha1-id (header-id empty-path))))))

(defn- java-write!
  "Writes a file the way a plain Java app would: a history list holding one
  moment, a WriteHashMap passed to `f`."
  [^String path id f]
  (let [core (CoreBufferedFile. (RandomAccessBufferedFile. (File. path) "rw"))]
    (try
      (let [jdb  (Database. core (hasher id))
            hist (WriteArrayList. (.rootCursor jdb))]
        (.appendContext hist (.getSlot hist -1)
                        (reify Database$ContextFunction
                          (run [_ cursor] (f (WriteHashMap. cursor)) nil)))
        (.flush core))
      (finally (.close core)))))

(deftest an-existing-file-opens-in-the-mode-of-its-header
  (doseq [[label id] [["id 0" 0] ["id sha1" sha1-id] ["some other id" (Hasher/stringToId "abcd")]]]
    (testing (str "foreign file, " label)
      (with-temp-paths [path]
        (java-write! path id (fn [^WriteHashMap m] (.put m "k" (Database$Bytes. "v"))))
        (testing "opens in compat mode without :mode, keeping its header"
          (with-open [d (db/xit-db path)]
            (is (= :compat (db/db-mode d)))
            (is (= "v" (get @d "k")))
            (is (thrown? IllegalArgumentException (swap! d assoc :b 1))))
          (is (= id (header-id path))))
        (testing "opens with {:mode :compat}"
          (with-open [d (db/xit-db path {:mode :compat})]
            (is (= "v" (get @d "k")))))
        (testing "is rejected with {:mode :clojure}, without modifying it"
          (let [before (file-bytes path)]
            (is (thrown-with-msg? IllegalArgumentException #"key hash format.*without :mode"
                                  (db/xit-db path {:mode :clojure})))
            (is (Arrays/equals before (file-bytes path))))))))
  (testing "a clj1 file opens in Clojure mode and is rejected with {:mode :compat}"
    (with-temp-paths [path]
      (with-open [d (db/xit-db path)]
        (reset! d {"a" 1 :b 2}))
      (let [before (file-bytes path)]
        (is (thrown-with-msg? IllegalArgumentException #"clj1"
                              (db/xit-db path {:mode :compat})))
        (is (Arrays/equals before (file-bytes path))))
      (doseq [opts [nil {:mode :clojure}]]
        (with-open [d (db/xit-db path opts)]
          (is (= :clojure (db/db-mode d)))
          (is (= {"a" 1 :b 2} (db/materialize @d)))))))
  (testing "a compat file created by xitdb-clj reopens in compat mode"
    (with-temp-paths [path]
      (with-open [d (db/xit-db path {:mode :compat})]
        (reset! d {"a" 1}))
      (is (thrown? IllegalArgumentException (db/xit-db path {:mode :clojure})))
      (doseq [opts [nil {:mode :compat}]]
        (with-open [d (db/xit-db path opts)]
          (is (= :compat (db/db-mode d)))
          (is (= {"a" 1} (db/materialize @d))))))))

(defn- rich-java-file!
  "A foreign file with every hash collection reachable by string key."
  [path id]
  (java-write! path id
    (fn [^WriteHashMap m]
      (.put m "name" (Database$Bytes. "alice"))
      (.put m "age" (Database$Uint. 30))
      (.put m "" (Database$Bytes. "empty key"))
      (.put m "clé→😀" (Database$Bytes. "non ascii key"))
      (let [profile (WriteHashMap. (.putCursor m "profile"))]
        (.put profile "city" (Database$Bytes. "Paris")))
      (let [members (WriteHashSet. (.putCursor m "members"))]
        (.put members "x")
        (.put members "é"))
      (let [counted (WriteCountedHashMap. (.putCursor m "counted"))]
        (.put counted "k" (Database$Bytes. "v")))
      (let [tags (WriteArrayList. (.putCursor m "tags"))]
        (.append tags (Database$Bytes. "t1"))))))

(defn- in-swap
  "Calls `f` with the writable value inside a swap! that keeps the data as is,
  returning what `f` returned."
  [d f]
  (let [result (atom nil)]
    (swap! d (fn [m] (reset! result (f m)) m))
    @result))

(deftest looks-up-string-keys-in-foreign-files
  (doseq [id [0 sha1-id]]
    (testing (str "header id " id)
      (with-temp-paths [path]
        (rich-java-file! path id)
        (with-open [d (db/xit-db path {:mode :compat})]
          (doseq [[label view] [["through @d" (fn [f] (f @d))]
                                 ["inside swap!" (fn [f] (in-swap d f))]]]
            (testing label
              (view
                (fn [m]
                  (is (= "alice" (get m "name")))
                  (is (= 30 (get m "age")))
                  (is (= "empty key" (get m "")))
                  (is (= "non ascii key" (get m "clé→😀")))
                  (is (= "Paris" (get-in m ["profile" "city"])))
                  (is (= "v" (get-in m ["counted" "k"])))
                  (is (= "t1" (get-in m ["tags" 0])))
                  (is (contains? m "name"))
                  (is (not (contains? m "missing")))
                  (is (nil? (get m "missing")))
                  (is (contains? (get m "members") "x"))
                  (is (contains? (get m "members") "é"))
                  (is (not (contains? (get m "members") "y")))
                  (is (= 8 (count m)))))))
          (is (= #{"x" "é"} (db/materialize (get @d "members")))))))))

(def ^:private non-string-keys
  [:foo :ns/foo 42 1.5 nil true \x [] ["a"] {} {"a" 1} #{} '("a") (java.util.UUID/randomUUID)
   (java.util.Date. 1000) (java.time.Instant/ofEpochSecond 1) (Object.)])

(deftest reading-with-a-non-string-key-finds-nothing
  (with-temp-paths [path]
    (rich-java-file! path 0)
    (with-open [d (db/xit-db path {:mode :compat})]
      (doseq [[label view] [["through @d" (fn [f] (f @d))]
                            ["inside swap!" (fn [f] (in-swap d f))]]
              k            non-string-keys]
        (testing (str label ", key " (pr-str k))
          (view
            (fn [m]
              (is (nil? (get m k)))
              (is (= ::not-found (get m k ::not-found)))
              (is (not (contains? m k)))
              (is (nil? (find m k)))
              (is (nil? (get-in m [k "city"])))
              (is (nil? (get-in m ["profile" k])))
              (is (= ::not-found (get-in m ["profile" k] ::not-found)))
              (let [members (get m "members")]
                (is (nil? (get members k)))
                (is (= ::not-found (get members k ::not-found)))
                (is (not (contains? members k))))))))
      (testing "keyword and function-call lookups"
        (is (nil? (:name @d)))
        (is (= ::not-found (:name @d ::not-found)))
        (is (nil? (@d :name)))
        (is (nil? (in-swap d #(:name %))))
        (is (nil? (:x (get @d "members"))))
        (is (nil? ((get @d "members") :x))))
      (testing "equality with maps and sets holding non-string keys is false, not an error"
        (is (not= {:name 1} (get @d "profile")))
        (is (not= (get @d "profile") {:name 1}))
        (is (not= #{:x :y} (get @d "members")))
        (is (not= (get @d "members") #{:x :y}))))))

(deftest clojure-mode-still-rejects-unsupported-lookup-keys
  (with-open [d (db/xit-db :memory)]
    (reset! d {:m {:a 1} :s #{:a}})
    (is (thrown? IllegalArgumentException (get @d (Object.))))
    (is (thrown? IllegalArgumentException (contains? (:s @d) (Object.))))
    (is (not (contains? @d (Object.))))
    (is (thrown? IllegalArgumentException (swap! d dissoc (Object.))))
    (is (thrown? IllegalArgumentException (swap! d update :s disj (Object.))))
    (is (= 2 (count @d)))
    (is (nil? (get @d nil)))
    (is (not (contains? @d nil)))))

(deftest removing-a-non-string-key-changes-nothing
  (with-temp-paths [path]
    (rich-java-file! path 0)
    (with-open [d (db/xit-db path {:mode :compat})]
      (let [before (db/materialize @d)]
        (doseq [k non-string-keys]
          (testing (pr-str k)
            (is (= before (db/materialize (swap! d dissoc k))))
            (is (= before (db/materialize (swap! d update "members" disj k))))
            (testing "on the read-only views too"
              (is (= before (db/materialize (dissoc @d k))))
              (is (= #{"x" "é"} (db/materialize (disj (get @d "members") k)))))))
        (testing "a string key is still removed"
          (is (not (contains? (swap! d dissoc "name") "name")))
          (is (= #{"é"} (db/materialize (get (swap! d update "members" disj "x") "members")))))))))

(def ^:private rejected-writes
  "Each writes at least one non-string hash map key or hash set member, at
  various depths and through each write route."
  {"reset! keyword key"          #(reset! % {:b 1})
   "reset! integer key"          #(reset! % {1 "a"})
   "reset! nil key"              #(reset! % {nil "a"})
   "reset! collection key"       #(reset! % {["a"] 1})
   "reset! set member"           #(reset! % #{1})
   "reset! nil set member"       #(reset! % #{nil})
   "reset! one bad key of many"  #(reset! % {"a" 1 "b" 2 :c 3})
   "reset! nested map"           #(reset! % {"a" {:b 1}})
   "reset! nested set"           #(reset! % {"s" #{1}})
   "reset! map in vector"        #(reset! % {"a" [{:b 1}]})
   "reset! map in list"          #(reset! % {"a" (list {:b 1})})
   "reset! set in vector"        #(reset! % {"a" [#{:b}]})
   "reset! map in sorted map"    #(reset! % {"a" (sorted-map "b" {:c 1})})
   "reset! deep"                 #(reset! % {"a" {"b" [{"c" #{:d}}]}})
   "swap! assoc"                 #(swap! % assoc :b 1)
   "swap! assoc nil"             #(swap! % assoc nil 1)
   "swap! assoc nested value"    #(swap! % assoc "a" {:b 1})
   "swap! assoc after a good"    #(swap! % assoc "good" 1 :b 2)
   "swap! merge"                 #(swap! % merge {"good" 1 :b 2})
   "swap! conj entry"            #(swap! % conj [:b 1])
   "swap! into"                  #(swap! % into {:b 1})
   "swap! replacing the root"    #(swap! % (constantly {:b 1}))
   "swap! assoc-in top"          #(swap! % assoc-in [:b :c] 1)
   "swap! assoc-in middle"       #(swap! % assoc-in ["profile" :c] 1)
   "swap! assoc-in new middle"   #(swap! % assoc-in ["new" :c] 1)
   "swap! update-in"             #(swap! % update-in ["profile" :c] (constantly 1))
   "swap! conj set member"       #(swap! % update "members" conj :b)
   "swap! conj nil member"       #(swap! % update "members" conj nil)
   "swap! into set"              #(swap! % update "members" into ["good" :b])
   "swap! write in a vector"     #(swap! % update "tags" conj {:b 1})
   "cursor at a bad key"         #(reset! (db/xdb-cursor % [:b]) 1)
   "cursor deep at a bad key"    #(reset! (db/xdb-cursor % ["profile" :b]) 1)
   "cursor swap! at a bad key"   #(swap! (db/xdb-cursor % ["profile" 1]) (constantly 1))
   "cursor writing a bad value"  #(reset! (db/xdb-cursor % ["profile" "city"]) {:b 1})
   "cursor swap! assoc"          #(swap! (db/xdb-cursor % ["profile"]) assoc :b 1)})

(defn- check-rejected-writes
  "Runs every rejected write against `d`, whose contents must stay `before`."
  [d before]
  (doseq [[label write!] rejected-writes]
    (testing label
      (let [history (count d)]
        (is (thrown-with-msg? IllegalArgumentException #"(?i)string" (write! d)))
        (is (= history (count d)))
        (is (= before (db/materialize @d)))))))

(deftest writing-a-non-string-key-throws-and-changes-nothing
  (testing "on a foreign file"
    (with-temp-paths [path]
      (rich-java-file! path 0)
      (let [before (with-open [d (db/xit-db path {:mode :compat})]
                     (let [before (db/materialize @d)]
                       (check-rejected-writes d before)
                       (testing "a later write still works"
                         (swap! d assoc "after" 1)
                         (is (= (assoc before "after" 1) (db/materialize @d))))
                       (assoc before "after" 1)))]
        (testing "after reopening"
          (with-open [d (db/xit-db path {:mode :compat})]
            (is (= before (db/materialize @d))))))))
  (testing "in memory"
    (with-open [d (db/xit-db :memory {:mode :compat})]
      (reset! d {"profile" {"city" "Paris"} "members" #{"x"} "tags" ["t"]})
      (check-rejected-writes d (db/materialize @d))))
  (testing "on a new database, before anything has been committed"
    (doseq [path [(temp-path) :memory]]
      (try
        (with-open [d (db/xit-db path {:mode :compat})]
          (is (thrown? IllegalArgumentException (reset! d {"a" {:b 1}})))
          (is (zero? (count d)))
          (is (nil? @d))
          (is (= {"a" 1} (db/materialize (reset! d {"a" 1}))))
          (is (= 1 (count d))))
        (finally
          (when (string? path) (.delete (File. ^String path))))))))

(deftest non-string-values-are-fine
  (with-open [d (db/xit-db :memory {:mode :compat})]
    (let [value {"kw"     :kw
                 "int"    1
                 "double" 1.5
                 "bool"   true
                 "nil"    nil
                 "vec"    [1 :a nil [2]]
                 "list"   '(1 :a)
                 "map"    {"nested" :v}
                 "set"    #{"s"}
                 "sorted" (sorted-map :k 1)}]
      (reset! d value)
      (is (= value (db/materialize @d)))
      (is (= :kw (get @d "kw")))
      (is (= 1 (get-in @d ["sorted" :k])))
      (is (= {"a" 2} (db/materialize (swap! d (constantly {"a" 2}))))))))

(defn- with-java-moment
  "Opens the file at `path` through the Java API, without saying which id it
  expects, and calls `f` with a read cursor on the latest moment."
  [^String path f]
  (let [core (CoreBufferedFile. (RandomAccessBufferedFile. (File. path) "r"))]
    (try
      (f (.getCursor (ReadArrayList. (.rootCursor (Database. core (hasher 0)))) -1))
      (finally (.close core)))))

(defn- java-text
  "The text at `cursor`, or nil when `cursor` is nil."
  [^ReadCursor cursor]
  (when cursor
    (String. ^bytes (.readBytes cursor nil) "UTF-8")))

(defn- java-keys
  "Every stored key of `m` with its format tag, by iterating through Java."
  [^ReadHashMap m]
  (for [^ReadCursor cursor (iterator-seq (.iterator m))]
    (let [bytes-obj (.readBytesObject (.-keyCursor (.readKeyValuePair cursor)) nil)]
      [(String. ^bytes (.value bytes-obj) "UTF-8") (.formatTag bytes-obj)])))

(deftest java-reads-what-compat-mode-writes
  (with-temp-paths [path]
    (with-open [d (db/xit-db path {:mode :compat})]
      (reset! d {"name" "alice"
                 "clé→😀" "non ascii key"
                 "" "empty key"
                 "profile" {"city" "Paris"}
                 "members" #{"x" "é"}})
      (swap! d assoc "added" "later")
      (swap! d assoc-in ["profile" "zip"] "75001")
      (reset! (db/xdb-cursor d ["profile" "country"]) "France"))
    (with-java-moment path
      (fn [moment]
        (let [m (ReadHashMap. moment)]
          (is (= "alice" (java-text (.getCursor m "name"))))
          (is (= "non ascii key" (java-text (.getCursor m "clé→😀"))))
          (is (= "empty key" (java-text (.getCursor m ""))))
          (is (= "later" (java-text (.getCursor m "added"))))
          (is (nil? (.getCursor m "missing")))
          (testing "keys are stored the way Java stores them, with no format tag"
            (is (= #{["name" nil] ["clé→😀" nil] ["" nil] ["profile" nil] ["members" nil] ["added" nil]}
                   (set (java-keys m)))))
          (let [profile (ReadHashMap. (.getCursor m "profile"))]
            (is (= "Paris" (java-text (.getCursor profile "city"))))
            (is (= "75001" (java-text (.getCursor profile "zip"))))
            (is (= "France" (java-text (.getCursor profile "country"))))
            (is (= #{["city" nil] ["zip" nil] ["country" nil]} (set (java-keys profile)))))
          (let [members (ReadHashSet. (.getCursor m "members"))]
            (is (some? (.getCursor members "x")))
            (is (some? (.getCursor members "é")))
            (is (nil? (.getCursor members "y")))
            (is (= 2 (count (iterator-seq (.iterator members)))))))))))

(defn- java-members
  "Every member of `s`, by iterating through Java."
  [^ReadHashSet s]
  (set (for [^ReadCursor cursor (iterator-seq (.iterator s))]
         (java-text (.-keyCursor (.readKeyValuePair cursor))))))

(deftest compat-mode-writes-plain-collections
  (testing "at every depth of a value, including empty ones"
    (with-temp-paths [path]
      (with-open [d (db/xit-db path {:mode :compat})]
        (let [value {"map"   {"deep" {"k" "v"}}
                     "set"   #{"m"}
                     "vec"   [{"in-vec" "v"} #{"in-vec"}]
                     "list"  (list {"in-list" "v"} #{"in-list"})
                     "empty" {}
                     "none"  #{}}]
          (reset! d value)
          (testing "and Clojure reads them back the same, count included"
            (is (= value (db/materialize @d)))
            (is (= 6 (count @d)))
            (is (= 1 (count (get @d "set"))))
            (is (zero? (count (get @d "empty"))))
            (is (zero? (count (get @d "none")))))))
      (with-java-moment path
        (fn [moment]
          (let [m (ReadHashMap. moment)]
            (is (= "v" (java-text (.getCursor (ReadHashMap. (.getCursor (ReadHashMap. (.getCursor m "map")) "deep")) "k"))))
            (is (= #{"m"} (java-members (ReadHashSet. (.getCursor m "set")))))
            (is (empty? (java-keys (ReadHashMap. (.getCursor m "empty")))))
            (is (empty? (java-members (ReadHashSet. (.getCursor m "none")))))
            (let [v (ReadArrayList. (.getCursor m "vec"))]
              (is (= "v" (java-text (.getCursor (ReadHashMap. (.getCursor v 0)) "in-vec"))))
              (is (= #{"in-vec"} (java-members (ReadHashSet. (.getCursor v 1))))))
            (let [l (ReadLinkedArrayList. (.getCursor m "list"))]
              (is (= "v" (java-text (.getCursor (ReadHashMap. (.getCursor l 0)) "in-list"))))
              (is (= #{"in-list"} (java-members (ReadHashSet. (.getCursor l 1)))))))))))
  (testing "when a native value is added to a Java-written file"
    (with-temp-paths [path]
      (rich-java-file! path 0)
      (with-open [d (db/xit-db path {:mode :compat})]
        (swap! d assoc "new-map" {"a" "b"} "new-set" #{"c"})
        (swap! d assoc-in ["profile" "nested"] {"d" "e"})
        (reset! (db/xdb-cursor d ["from-cursor"]) {"f" "g"}))
      (with-java-moment path
        (fn [moment]
          (let [m (ReadHashMap. moment)]
            (is (= "b" (java-text (.getCursor (ReadHashMap. (.getCursor m "new-map")) "a"))))
            (is (= #{"c"} (java-members (ReadHashSet. (.getCursor m "new-set")))))
            (is (= "e" (java-text (.getCursor (ReadHashMap. (.getCursor (ReadHashMap. (.getCursor m "profile")) "nested")) "d"))))
            (is (= "g" (java-text (.getCursor (ReadHashMap. (.getCursor m "from-cursor")) "f"))))))))))

(deftest replacing-a-counted-collection-with-a-native-value-makes-it-plain
  (with-temp-paths [path]
    (rich-java-file! path 0)
    (with-open [d (db/xit-db path {:mode :compat})]
      (swap! d assoc "counted" {"new" "v"})
      (is (= 1 (count (get @d "counted"))))
      (is (= "v" (get-in @d ["counted" "new"]))))
    (with-java-moment path
      (fn [moment]
        (let [replaced (.getCursor (ReadHashMap. moment) "counted")]
          (is (thrown? Database$UnexpectedTagException (ReadCountedHashMap. replaced)))
          (is (= "v" (java-text (.getCursor (ReadHashMap. replaced) "new")))))))))

(deftest editing-a-collection-in-place-keeps-its-kind
  (with-temp-paths [path]
    (java-write! path 0
      (fn [^WriteHashMap m]
        (let [plain (WriteHashMap. (.putCursor m "plain"))]
          (.put plain "a" (Database$Bytes. "1")))
        (let [counted (WriteCountedHashMap. (.putCursor m "counted"))]
          (.put counted "a" (Database$Bytes. "1")))
        (let [plain-set (WriteHashSet. (.putCursor m "plain-set"))]
          (.put plain-set "a"))
        (let [counted-set (WriteCountedHashSet. (.putCursor m "counted-set"))]
          (.put counted-set "a"))))
    (with-open [d (db/xit-db path {:mode :compat})]
      (swap! d update "plain" assoc "b" "2")
      (swap! d update "counted" assoc "b" "2")
      (swap! d update "plain-set" conj "b")
      (swap! d update "counted-set" conj "b")
      (testing "Clojure counts both kinds"
        (is (= 2 (count (get @d "plain"))))
        (is (= 2 (count (get @d "counted"))))
        (is (= 2 (count (get @d "plain-set"))))
        (is (= 2 (count (get @d "counted-set"))))))
    (with-java-moment path
      (fn [moment]
        (let [m (ReadHashMap. moment)]
          (is (= "2" (java-text (.getCursor (ReadHashMap. (.getCursor m "plain")) "b"))))
          (is (= 2 (.count (ReadCountedHashMap. (.getCursor m "counted")))))
          (is (= #{"a" "b"} (java-members (ReadHashSet. (.getCursor m "plain-set")))))
          (is (= 2 (.count (ReadCountedHashSet. (.getCursor m "counted-set"))))))))
    (testing "emptying one and refilling it in the same swap!"
      (with-open [d (db/xit-db path {:mode :compat})]
        (swap! d update "plain" #(-> % empty (assoc "z" "9")))
        (swap! d update "counted" #(-> % empty (assoc "z" "9")))
        (swap! d update "plain-set" #(-> % empty (conj "z")))
        (swap! d update "counted-set" #(-> % empty (conj "z"))))
      (with-java-moment path
        (fn [moment]
          (let [m (ReadHashMap. moment)]
            (is (= ["z"] (map first (java-keys (ReadHashMap. (.getCursor m "plain"))))))
            (let [counted (ReadCountedHashMap. (.getCursor m "counted"))]
              (is (= 1 (.count counted)))
              (is (= "9" (java-text (.getCursor counted "z")))))
            (is (= #{"z"} (java-members (ReadHashSet. (.getCursor m "plain-set")))))
            (is (= 1 (.count (ReadCountedHashSet. (.getCursor m "counted-set")))))))))))

(deftest updating-a-java-written-key-does-not-duplicate-it
  (doseq [id [0 sha1-id]]
    (testing (str "header id " id)
      (with-temp-paths [path]
        (rich-java-file! path id)
        (with-open [d (db/xit-db path {:mode :compat})]
          (swap! d assoc "name" "bob" "from-clj" "hi")
          (swap! d assoc-in ["profile" "city"] "Rome")
          (swap! d update "members" conj "x" "new")
          (swap! d assoc-in ["counted" "k"] "w")
          (reset! (db/xdb-cursor d ["age"]) 31)
          (is (= 9 (count @d)))
          (is (= "bob" (get @d "name")))
          (is (= 31 (get @d "age"))))
        (is (= id (header-id path)))
        (with-java-moment path
          (fn [moment]
            (let [m (ReadHashMap. moment)]
              (is (= "bob" (java-text (.getCursor m "name"))))
              (is (= "hi" (java-text (.getCursor m "from-clj"))))
              (is (= 9 (count (java-keys m))))
              (is (= 1 (count (filter #{["name" nil]} (java-keys m)))))
              (let [profile (ReadHashMap. (.getCursor m "profile"))]
                (is (= "Rome" (java-text (.getCursor profile "city"))))
                (is (= ["city"] (map first (java-keys profile)))))
              (let [members (ReadHashSet. (.getCursor m "members"))]
                (is (= 3 (count (iterator-seq (.iterator members)))))
                (is (some? (.getCursor members "new"))))
              (let [counted (ReadCountedHashMap. (.getCursor m "counted"))]
                (is (= "w" (java-text (.getCursor counted "k"))))
                (is (= 1 (.count counted)))))))))))

(deftest clj-removes-a-java-written-key-for-java-too
  (with-temp-paths [path]
    (rich-java-file! path 0)
    (with-open [d (db/xit-db path {:mode :compat})]
      (swap! d dissoc "name")
      (swap! d update "members" disj "x"))
    (with-java-moment path
      (fn [moment]
        (let [m (ReadHashMap. moment)]
          (is (nil? (.getCursor m "name")))
          (is (nil? (.getCursor (ReadHashSet. (.getCursor m "members")) "x"))))))))

(deftest java-can-keep-writing-a-compat-file
  (with-temp-paths [path]
    (with-open [d (db/xit-db path {:mode :compat})]
      (reset! d {"a" "from clj"}))
    (let [core (CoreBufferedFile. (RandomAccessBufferedFile. (File. path) "rw"))]
      (try
        (let [hist (WriteArrayList. (.rootCursor (Database. core (hasher sha1-id))))]
          (.appendContext hist (.getSlot hist -1)
                          (reify Database$ContextFunction
                            (run [_ cursor]
                              (let [m (WriteHashMap. cursor)]
                                (.put m "a" (Database$Bytes. "from java"))
                                (.put m "b" (Database$Bytes. "new")))
                              nil)))
          (.flush core))
        (finally (.close core))))
    (with-open [d (db/xit-db path {:mode :compat})]
      (is (= {"a" "from java" "b" "new"} (db/materialize @d)))
      (is (= "from java" (get @d "a")))
      (is (= 2 (count @d))))))

(deftest strings-cannot-collide-with-other-key-types
  (let [pairs {"keyword"      {"kw:foo" 1 :foo 2}
               "integer"      {"ki42" 1 42 2}
               "boolean"      {"bltrue" 1 true 2}
               "char"         {"chx" 1 \x 2}
               "empty vector" {"\n" 1 [] 2}
               "empty map"    {"\u000B" 1 {} 2}
               "empty set"    {"\f" 1 #{} 2}}]
    (doseq [[label m] pairs]
      (testing label
        (testing "Clojure mode keeps two entries"
          (with-open [d (db/xit-db :memory)]
            (reset! d m)
            (is (= 2 (count @d)))
            (is (= m (db/materialize @d)))
            (doseq [[k v] m]
              (is (= v (get @d k))))))
        (testing "compat mode refuses the non-string key"
          (with-open [d (db/xit-db :memory {:mode :compat})]
            (is (thrown? IllegalArgumentException (reset! d m)))
            (is (nil? @d)))))))
  (testing "set members"
    (with-open [d (db/xit-db :memory)]
      (reset! d {:s #{"kw:foo" :foo}})
      (is (= 2 (count (:s @d)))))
    (with-open [d (db/xit-db :memory {:mode :compat})]
      (is (thrown? IllegalArgumentException (reset! d {"s" #{"kw:foo" :foo}}))))))

(deftest clojure-hashing-is-unchanged
  (testing "a string key still hashes its format tag and text, not its raw bytes"
    (with-temp-paths [path]
      (with-open [d (db/xit-db path)]
        (reset! d {"foo" "v"}))
      (let [core (CoreBufferedFile. (RandomAccessBufferedFile. (File. path) "r"))]
        (try
          (let [jdb (Database. core (hasher clj1-id))
                m   (ReadCountedHashMap. (.getCursor (ReadArrayList. (.rootCursor jdb)) -1))]
            (is (nil? (.getCursor m "foo")))
            (is (= "v" (java-text (.getCursor m "stfoo")))))
          (finally (.close core))))))
  (testing "native maps and sets are still written counted"
    (with-temp-paths [path]
      (with-open [d (db/xit-db path)]
        (reset! d {:m {:a 1} :s #{:b}}))
      (with-java-moment path
        (fn [moment]
          (is (thrown? Database$UnexpectedTagException (ReadHashMap. moment)))
          (let [m (ReadCountedHashMap. moment)]
            (is (= 1 (.count (ReadCountedHashMap. (.getCursor m (conversion/db-key-hash (.-db ^ReadCursor moment) :m))))))
            (is (= 1 (.count (ReadCountedHashSet. (.getCursor m (conversion/db-key-hash (.-db ^ReadCursor moment) :s)))))))))))
  (testing "the legacy fixture is still rejected, although its header ID 0 would mean compat mode"
    (with-temp-paths [path]
      (Files/copy (.toPath (File. "test-resources/legacy-key-hashes.db"))
                  (.toPath (File. path))
                  ^"[Ljava.nio.file.CopyOption;" (make-array java.nio.file.CopyOption 0))
      (doseq [opts [nil {:mode :clojure} {:mode :compat}]]
        (is (thrown-with-msg? IllegalArgumentException #"Unsupported file format.*[Ee]xport"
                              (db/xit-db path opts)))))))

(deftest open-database-keeps-its-two-argument-form
  (with-temp-paths [path compat-path]
    (let [^Database jdb (db/open-database path "rw")]
      (try
        (is (= clj1-id (.hashId (.-header jdb))))
        (finally (.close (.-core jdb)))))
    (is (thrown? IllegalArgumentException (db/open-database path "rw" :compat)))
    (testing "detects the mode of an existing file"
      (.close (db/xit-db compat-path {:mode :compat}))
      (let [^Database jdb (db/open-database compat-path "rw")]
        (try
          (is (= sha1-id (.hashId (.-header jdb))))
          (finally (.close (.-core jdb))))))))

(deftest compaction-keeps-compat-lookups-working
  (doseq [[label id] [["id 0" 0] ["id sha1" sha1-id]]]
    (testing (str "foreign file, " label)
      (with-temp-paths [path target]
        (rich-java-file! path id)
        (with-open [d (db/xit-db path {:mode :compat})]
          (with-open [compacted (db/compact d target)]
            (is (= "alice" (get @compacted "name")))
            (is (= "Paris" (get-in @compacted ["profile" "city"])))
            (is (contains? (get @compacted "members") "x"))
            (swap! compacted assoc "name" "bob")
            (is (= 8 (count @compacted)))
            (is (thrown? IllegalArgumentException (swap! compacted assoc :b 1)))))
        (is (= id (header-id target)))
        (is (thrown? IllegalArgumentException (db/xit-db target {:mode :clojure})))
        (with-open [d (db/xit-db target)]
          (is (= "bob" (get @d "name")))
          (is (contains? (get @d "members") "é")))
        (with-java-moment target
          (fn [moment]
            (is (= "bob" (java-text (.getCursor (ReadHashMap. moment) "name")))))))))
  (testing "a file created in compat mode"
    (with-temp-paths [path target]
      (with-open [d (db/xit-db path {:mode :compat})]
        (reset! d {"a" {"b" 1} "s" #{"m"}})
        (with-open [compacted (db/compact d target)]
          (is (= 1 (get-in @compacted ["a" "b"])))))
      (is (= sha1-id (header-id target)))
      (with-java-moment target
        (fn [moment]
          (is (some? (.getCursor (ReadHashMap. moment) "a")))))
      (with-open [d (db/xit-db target {:mode :compat})]
        (is (= {"a" {"b" 1} "s" #{"m"}} (db/materialize @d)))
        (swap! d assoc-in ["a" "b"] 2)
        (is (= 2 (get-in @d ["a" "b"]))))))
  (testing "into memory"
    (with-open [d (db/xit-db :memory {:mode :compat})]
      (reset! d {"a" 1})
      (with-open [compacted (db/compact d :memory)]
        (is (= 1 (get @compacted "a")))
        (is (nil? (get @compacted :a)))
        (is (thrown? IllegalArgumentException (swap! compacted assoc :a 1)))
        (is (= 2 (get (swap! compacted assoc "a" 2) "a")))))))

(deftest a-read-value-can-be-written-back-by-reference
  (with-temp-paths [path]
    (rich-java-file! path 0)
    (with-open [d (db/xit-db path {:mode :compat})]
      (swap! d assoc "profile-copy" (get @d "profile"))
      (is (= "Paris" (get-in @d ["profile-copy" "city"]))))))

(deftest a-full-copy-gives-the-replica-the-mode-of-its-source
  (with-temp-paths [java-path clj-path replica-path]
    (rich-java-file! java-path 0)
    (with-open [java-source (db/xit-db java-path)
                clj-source  (db/xit-db clj-path)
                replica     (db/xit-db replica-path {:mode :clojure})]
      (reset! clj-source {:k 1})
      (reset! replica {:mine 1})
      (let [sync! (fn [source]
                    (with-open [in (io/input-stream source :since (replication/replica-state replica))]
                      (replication/replicate! replica in)))]
        (testing "a foreign source puts the replica in compat mode"
          (is (:full-copy? (sync! java-source)))
          (is (= :compat (db/db-mode replica)))
          (is (= "Paris" (get-in @replica ["profile" "city"])))
          (is (thrown? IllegalArgumentException (swap! replica assoc :x 1))))
        (testing "a Clojure-mode source puts it back"
          (is (:full-copy? (sync! clj-source)))
          (is (= :clojure (db/db-mode replica)))
          (is (= {:k 1} (db/materialize @replica))))))
    (testing "the replica reopens in the mode it was copied in"
      (is (thrown? IllegalArgumentException (db/xit-db replica-path {:mode :compat})))
      (with-open [replica (db/xit-db replica-path)]
        (is (= :clojure (db/db-mode replica)))))))
