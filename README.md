# XITDB Clojure

`xitdb-clj` is a Clojure native interface on top of the immutable database [xitdb-java](https://github.com/radarroark/xitdb-java).


It allows you to work with the database as if it were a Clojure atom.

### Quick example

One code sample is worth a thousand words in a README:

```clojure
(def db (xdb/xit-db "testing.db"))  
  
(reset! db {:users {"1234" {:name "One Two Three Four" :address {:city "Barcelona"}}}})  
  
;; Read the contents of the DB is if it were an atom  
@db  
; => {:users {"1234" {:name "One Two Three Four", :address {:city "Barcelona"}}}}  
  
(swap! db update-in [:users "1234" :address] merge {:street "Gran Via" :postal-code "08010"})  
(get-in @db [:users "1234" :address :street])
```


Yeah, it's extremely cool. Once you start using it, you might pinch yourself when you realise that most of the database-related data munging simply disappears from your code.
There's no translation between your app domain to database and back, it's just Clojure all the way to the disk platter or NAND flash memory chips.

### Clojure data structures, persisted

`xitdb-java` provides an efficient implementation of several data structures:
`HashMap` and `ArrayList` are based on the hash array mapped trie from Phil Bagwell. `LinkedArrayList` is based on the RRB tree, also from Phil Bagwell.
If it rings a bell it's because Clojure's data structures are also built on them.


### Current status

