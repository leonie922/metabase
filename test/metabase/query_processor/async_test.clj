(ns metabase.query-processor.async-test
  (:require [metabase.query-processor.async :as qp.async]
            [expectations :refer [expect]]
            [metabase.test.data :as data]
            [metabase.test.util.async :as tu.async]
            [clojure.core.async :as a]
            [metabase.query-processor :as qp]))

;; running a query async should give you the same results as running that query synchronously
(let [query {:database (data/id)
             :type     :query
             :query    {:source-table (data/id :venues)
                        :fields       [[:field-id (data/id :venues :name)]]
                        :limit        5}}]
  (expect
    (qp/process-query query)
    (tu.async/with-open-channels [result-chan (qp.async/process-query query)]
      (a/alts!! [result-chan (a/timeout 1000)]))))

;; NOCOMMIT
(defn- x []
  (tu.async/with-open-channels [result-chan (qp.async/result-metadata-for-query-async
                                             {:database (data/id)
                                              :type     :query
                                              :query    {:source-table (data/id :venues)
                                                         :fields       [[:field-id (data/id :venues :name)]]}})]
    (first (a/alts!! [result-chan (a/timeout 1000)]))))
