(ns xitdb.charset-test
  "Stored text must decode the same under every JVM default charset. The
  default charset is fixed at JVM startup, so these tests run the database in
  child JVMs started with an explicit `file.encoding`."
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [xitdb.db :as db])
  (:import [java.util.concurrent TimeUnit]))

;; Text that is not ASCII, in every stored form. Plain strings are stored inline
;; up to 8 bytes, keywords and chars (which carry a format tag) up to 6, so each
;; form appears both short and long. The chars cover 2- and 3-byte UTF-8, and the
;; long string includes a 4-byte surrogate pair.
(def fixture
  {(keyword "\u00e9t\u00e9")        "\u00e9t\u00e9"
   (keyword "na\u00efve-\u00fcber") "caf\u00e9 \u4e2d\u6587 \ud83d\ude00"
   (keyword "\u00e9" "n\u00e4me")   \u00e9
   "\u00e9t\u00e9"                  \u4e2d
   "\u4e2d\u6587 key \ud83d\ude00"  :ascii
   \u00e9                           [\u4e2d "\u00fc" (keyword "\u00e9")]
   :nested                          {"\u00fc" #{(keyword "\u00e9") "\u00e9t\u00e9" \u00e9}
                                     \u4e2d   {(keyword "\u00e9" "\u00fc") "na\u00efve-\u00fcber-\u4e2d\u6587"}}})

(defn- entries-retrieve-themselves?
  "True when every key found by enumerating `v` (a database value) looks up its
  own entry, at every depth. A key decoded with the wrong charset hashes
  differently from the one written, so lookups with it miss."
  [v]
  (cond
    (map? v) (every? (fn [[k ev]]
                       (and (contains? v k)
                            (= (db/materialize ev) (db/materialize (get v k)))
                            (entries-retrieve-themselves? ev)))
                     v)
    (set? v) (every? #(contains? v %) v)
    (coll? v) (every? entries-retrieve-themselves? v)
    :else true))

(defn- check-db
  "Reads `path` and reports, as booleans, whether it holds exactly `fixture`."
  [path]
  (with-open [d (db/xit-db path)]
    (let [v @d]
      {:equal              (= fixture (db/materialize v))
       :original-keys-find (every? (fn [[k fv]] (= fv (db/materialize (get v k)))) fixture)
       :read-keys-find     (entries-retrieve-themselves? v)})))

(defn run-child!
  "Entry point for the child JVM. Runs `ops`, a vector of `[op path]` pairs where
  op is `:write` (store `fixture`), `:rewrite` (materialize the stored value and
  write it back) or `:check` (see `check-db`). Prints one EDN line holding the
  default charset and the result of each check; it contains only ASCII, so the
  parent reads it correctly whatever the child's charset."
  [ops]
  (prn {:charset (str (java.nio.charset.Charset/defaultCharset))
        :checks  (vec (for [[op path] ops
                            :let [result (case op
                                           :write   (with-open [d (db/xit-db path)]
                                                      (reset! d fixture)
                                                      nil)
                                           :rewrite (with-open [d (db/xit-db path)]
                                                      (reset! d (db/materialize @d))
                                                      nil)
                                           :check   (check-db path))]
                            :when result]
                        result))}))

(defn- run-child
  "Runs `ops` (see `run-child!`) in a new JVM whose default charset is `encoding`
  and returns its report."
  [encoding ops]
  (let [code    (str "(require 'xitdb.charset-test)"
                     "(xitdb.charset-test/run-child! " (pr-str ops) ")")
        process (.start (doto (ProcessBuilder.
                                ^java.util.List
                                [(str (System/getProperty "java.home") "/bin/java")
                                 (str "-Dfile.encoding=" encoding)
                                 "-cp" (System/getProperty "java.class.path")
                                 "clojure.main" "-e" code])
                          (.redirectErrorStream true)))
        output  (deref (future (slurp (.getInputStream process))) 120000 ::timeout)]
    (when-not (.waitFor process 5 TimeUnit/SECONDS)
      (.destroyForcibly process))
    (when (= ::timeout output)
      (throw (ex-info "Child JVM timed out" {:encoding encoding :ops ops})))
    (when-not (zero? (.exitValue process))
      (throw (ex-info (str "Child JVM failed:\n" output) {:encoding encoding :ops ops})))
    (edn/read-string (last (str/split-lines output)))))

(def ^:private all-pass
  {:equal true :original-keys-find true :read-keys-find true})

(deftest text-decodes-identically-under-any-default-charset
  (let [utf8-written   (java.io.File/createTempFile "xitdb-utf8-" ".db")
        latin1-written (java.io.File/createTempFile "xitdb-latin1-" ".db")
        utf8-path      (.getAbsolutePath utf8-written)
        latin1-path    (.getAbsolutePath latin1-written)]
    (try
      (.delete utf8-written)
      (.delete latin1-written)
      (testing "data written under UTF-8"
        (is (= {:charset "UTF-8" :checks [all-pass]}
               (run-child "UTF-8" [[:write utf8-path] [:check utf8-path]]))))
      (testing "is read, rewritten and written again under ISO-8859-1"
        (is (= {:charset "ISO-8859-1" :checks [all-pass all-pass all-pass]}
               (run-child "ISO-8859-1" [[:check utf8-path]
                                        [:rewrite utf8-path]
                                        [:check utf8-path]
                                        [:write latin1-path]
                                        [:check latin1-path]]))))
      (testing "and both files read back intact under UTF-8"
        (is (= {:charset "UTF-8" :checks [all-pass all-pass all-pass]}
               (run-child "UTF-8" [[:check utf8-path]
                                   [:check latin1-path]
                                   [:rewrite latin1-path]
                                   [:check latin1-path]]))))
      (finally
        (.delete utf8-written)
        (.delete latin1-written)))))
