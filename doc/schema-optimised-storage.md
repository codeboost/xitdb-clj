

# The problem

When storing a collection of hash maps with identical keys,
the keys are stored repeatedly, which leads to a lot more
writing to the file and the growth of the file.
When storing the following records
```
[{:name "Florin" :age 44 :location "ES"} {:name "Alba" :age 32 :location "ES"}
 {:name "Jordi" :age 33 :location "ES"} {:name "Foo" :age 34 :location "AA"}]
```

the database will write the keys `:name`, `:age`, `:location` for each record, which is redundant.
The more records, the more redundancy and space wasted.

## Proposed solution

Keys are defined in a 'schema', which is stored in a hidden key in the root map, indexed by keypath.

Rather than store keys and values in the maps, the maps have a hidden `:xdb/values` array list which is the same size as the schema. The values are stored as items in this array, without the keys.

The records are still maps, they can still contain extraneous keys which are not in the schema (non-strict schema)

This only applies to Collections of Maps.

## Schema

Schema is stored under a hidden key in the root map, `:xdb/schema`.
The schema is defined on keypaths:

```clojure
{:xdb/schema {[:users :*] [:map  
                           [:first-name :string]  
                           [:last-name :string]  
                           [:address [:map  
                                      [:street :string]  
                                      [:city :string]  
                                      [:zip :int]  
                                      [:lonlat [:tuple :double :double]]]]]  
  
              [:users :* :address] [:map  
                                    [:street :string]  
                                    [:city :string]  
                                    [:country :string]
                                    [:zip :int]]}}
```

There are two schemas defined in this example.
The schema `[:users :*]` targets the map items in the collection `:users` at any index or key and the values will look like this:
```clojure
{:first-name "John"
 :last-name "Doe"
 :address {:street "123 Main St"
           :city "San Francisco"
           :zip 94107
           :lonlat [37.7749 -122.4194]}}
```

Note that `[:users :*]` applies to map values in any type of collections, including maps and sets.

The second schema targets the value in the same collection, essentially it overrides the previous `:address` schema and will take precedence. This still needs more thinking though.


## Storing values

Rather than store keyval pairs for each record in the collection, instead, the map is going to have a hidden key `:xdb/values`, an `ArrayList`. This `ArrayList` is going to have the same number of items as the schema keys for the map.
Thus, the user record above would be stored as:
```
{:xdb/values ["Jon" "Doe" {:xdb/values ["123 Main St" "San Francisco" 94107 [37.7749 -122.4194]]}]}
```

Given the keys in schema and the values in `:xdb/values`, it is possible to reconstruct the original map when reading the record.

### Implementation

#### Writing
When writing a map to the database (`conversion/map->WriteHashMapCursor!`), there's a dynamic binding `*current-write-keypath*` which is maintained as the function writes the key values recursively.
If there's a schema defined for `*current-write-keypath*` (via `schema-for-current-keypath`), the values are not associated to the map, but written to the `:xdb/values` array in the function `->vals-array!`.

#### Reading

When reading, we maintain the `*read-keypath*` binding when doing `map-seq` and if there's a schema for `*read-keypath*`, then we read the values from the `:xdb/values` key first, then the rest of the keyvals from the map.

After reading the values from `:xdb/values`, we reconstruct the map entries using the key from the schema and the value from the respective position in `:xdb/values`.

If `*show-hidden-keys?*` binding is set to true (used for debugging/testing) just return the `:xdb/values` array without reconstructing the map entries. 
