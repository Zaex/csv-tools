(ns csv-tools.core)

(defn csv>rel
  "assumes first row is column names"
  [[col-names & csv-rest]]
  (->> csv-rest
       (transduce (map (partial zipmap col-names))
                  conj!
                  (transient #{}))
       (persistent!)))

(defn rel>csv
  "assumes all rows have the same columns"
  [[{:keys [_ :as columns]} :as rel]]
  (->> rel
       (transduce (map vals)
                  conj!
                  (transient [columns]))
       (persistent!)))
