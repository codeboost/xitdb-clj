(ns xitdb.xitdb-write-types
  (:require
    [xitdb.common :as common]
    [xitdb.xitdb-util :as util])
  (:import
    (io.github.radarroark.xitdb Slot Tag WriteArrayList WriteCursor WriteHashMap)))

(declare unwrap)
(declare ->XITDBWriteArrayList)

;;---------------------------------------

