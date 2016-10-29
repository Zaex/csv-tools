(ns zaex.csv-tools
  (:require [zaex.rel-tools :as rel]
            [clojure.data.csv :as csv]))

(defn read-csv
  "read the csv file into csv-format (vector of vectors)"
  [file]
  (with-open [in-file (io/reader file)]
    (doall
      (csv/read-csv in-file))))

(read-csv in-csv1)

(defn write-csv
  "write the csv file"
  [file csv-format]
  (with-open [out-file (io/writer file)]
    (csv/write-csv out-file csv-format)))

(defn csv-format>rel
  "assumes first row is column names"
  [[col-names & csv-rest]]
    (persistent!
      (reduce #(conj! %1 (zipmap col-names %2))
              (transient #{})
              csv-rest)))

(csv-format>rel (read-csv in-csv1))

(defn rel>csv-format
  "assumes all rows have the same columns"
  [[{:keys [_ :as ms-keys]} :as ms]]
  ms-keys
  #_(persistent!
    (reduce (fn [csv-form {v :vals}]
              (conj! csv-form v))
            (transient ms-keys)
            ms)))

(rel>csv-format (csv-format>map (read-csv in-csv1)))

(let [init (read-csv in-csv1)
      map-format (csv-format>rel init)
      csv-format (rel>csv-format map-format)]
  (= init csv-format))


(defn merge-csv
  "merge many csvs using right natural joins"
  [csv-list csv-out]
  (->> csv-list
       (transduce (map #(-> %
                            (read-csv)
                            (csv-format>rel)))
                  rel/right-join #{})
       (rel>csv-format)
       (write-csv csv-out)))

(sqlalgo/left-join #{} #{{:a 1 :b 2}})


(conj #{} {:a 1 :b 2})

(merge-csv [in-csv1 in-csv2 in-csv3] out-csv)

