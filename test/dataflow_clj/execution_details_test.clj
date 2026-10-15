(ns dataflow-clj.execution-details-test
  (:require [clojure.test :refer [deftest is testing]]
            [dataflow-clj.core :as dataflow])
  (:import (com.google.api.core ApiFutures)
           (com.google.api.gax.grpc GrpcCallContext)
           (com.google.api.gax.rpc UnaryCallable PagedListResponseFactory)
           (com.google.dataflow.v1beta3 ExecutionState StageSummary WorkerDetails WorkItemDetails
                                       ProgressTimeseries ProgressTimeseries$Point
                                       MetricUpdate MetricStructuredName StragglerSummary Straggler
                                       StragglerInfo StragglerInfo$StragglerDebuggingInfo
                                       StreamingStragglerInfo HotKeyDebuggingInfo HotKeyDebuggingInfo$HotKeyInfo
                                       JobExecutionDetails StageExecutionDetails MetricsV1Beta3Client)
           (com.google.dataflow.v1beta3.stub MetricsV1Beta3Stub MetricsV1Beta3StubSettings)
           (com.google.protobuf Timestamp Value Struct ListValue)
           (java.time LocalDateTime Duration)))

(defn- callable [f]
  (proxy [UnaryCallable] []
    (futureCall
      ([request] (ApiFutures/immediateFuture (f request)))
      ([request context] (ApiFutures/immediateFuture (f request))))))

(def timestamp (.build (-> (Timestamp/newBuilder) (.setSeconds 1) (.setNanos 123))))
(def time-clj (LocalDateTime/of 1970 1 1 0 0 1 123))
(def duration (.build (-> (com.google.protobuf.Duration/newBuilder) (.setSeconds 2) (.setNanos 456))))
(def duration-clj (Duration/ofSeconds 2 456))

(def progress
  (.build (-> (ProgressTimeseries/newBuilder)
              (.setCurrentProgress 0.5)
              (.addDataPoints (.build (-> (ProgressTimeseries$Point/newBuilder)
                                         (.setTime timestamp) (.setValue 0.25))))
              (.addDataPoints (ProgressTimeseries$Point/getDefaultInstance)))))

(def progress-clj
  {:current-progress 0.5 :data-points [{:time time-clj :value 0.25} {:time nil :value 0.0}]})

(def metric
  (let [number (.build (.setNumberValue (Value/newBuilder) 42))
        text (.build (.setStringValue (Value/newBuilder) "value"))
        flag (.build (.setBoolValue (Value/newBuilder) false))
        list-value (.build (.setListValue (Value/newBuilder)
                                         (.build (.addAllValues (ListValue/newBuilder)
                                                               [number text flag (Value/getDefaultInstance)]))))
        structure (.build (.setStructValue (Value/newBuilder)
                                           (.build (.putFields (Struct/newBuilder) "values" list-value))))]
    (.build (-> (MetricUpdate/newBuilder)
                (.setName (.build (-> (MetricStructuredName/newBuilder)
                                     (.setOrigin "user") (.setName "count")
                                     (.putAllContext {"step" "read"}))))
                (.setKind "sum") (.setCumulative true)
                (.setScalar number) (.setMeanSum number) (.setMeanCount number)
                (.setSet list-value) (.setTrie structure) (.setBoundedTrie structure)
                (.setDistribution structure) (.setGauge flag) (.setInternal text)
                (.setUpdateTime timestamp)))))

(def metric-clj
  {:name {:origin "user" :name "count" :context {"step" "read"}}
   :kind "sum" :cumulative true :scalar 42.0 :mean-sum 42.0 :mean-count 42.0
   :set [42.0 "value" false nil] :trie {"values" [42.0 "value" false nil]}
   :bounded-trie {"values" [42.0 "value" false nil]}
   :distribution {"values" [42.0 "value" false nil]}
   :gauge false :internal "value" :update-time time-clj})

(def straggler-info
  (.build (-> (StragglerInfo/newBuilder)
              (.setStartTime timestamp)
              (.putCauses "hot-key"
                          (.build (.setHotKey (StragglerInfo$StragglerDebuggingInfo/newBuilder)
                                              (.build (.putDetectedHotKeys (HotKeyDebuggingInfo/newBuilder) 7
                                                                           (.build (-> (HotKeyDebuggingInfo$HotKeyInfo/newBuilder)
                                                                                       (.setHotKeyAge duration)
                                                                                       (.setKey "key")
                                                                                       (.setKeyTruncated true)))))))))))

(def straggler-info-clj
  {:start-time time-clj
   :causes {"hot-key" {:hot-key {:detected-hot-keys {7 {:hot-key-age duration-clj
                                                     :key "key" :key-truncated true}}}}}})

(def straggler-summary
  (.build (-> (StragglerSummary/newBuilder)
              (.setTotalStragglerCount 2)
              (.putAllStragglerCauseCount {"hot-key" (long 1) "watermark" (long 1)})
              (.addRecentStragglers (.build (.setBatchStraggler (Straggler/newBuilder) straggler-info)))
              (.addRecentStragglers
               (.build (.setStreamingStraggler (Straggler/newBuilder)
                                               (.build (-> (StreamingStragglerInfo/newBuilder)
                                                           (.setStartTime timestamp) (.setEndTime timestamp)
                                                           (.setWorkerName "worker")
                                                           (.setDataWatermarkLag duration)
                                                           (.setSystemWatermarkLag duration)))))))))

(def straggler-summary-clj
  {:total-straggler-count 2 :straggler-cause-count {"hot-key" 1 "watermark" 1}
   :recent-stragglers [{:batch-straggler straggler-info-clj :streaming-straggler nil}
                      {:batch-straggler nil
                       :streaming-straggler {:start-time time-clj :end-time time-clj :worker-name "worker"
                                            :data-watermark-lag duration-clj :system-watermark-lag duration-clj}}]})

(def stage
  (.build (-> (StageSummary/newBuilder)
              (.setStageId "stage") (.setState ExecutionState/EXECUTION_STATE_RUNNING)
              (.setStartTime timestamp) (.setEndTime timestamp)
              (.setProgress progress) (.addMetrics metric) (.setStragglerSummary straggler-summary))))

(def worker
  (.build (-> (WorkerDetails/newBuilder)
              (.setWorkerName "worker")
              (.addWorkItems (.build (-> (WorkItemDetails/newBuilder)
                                        (.setTaskId "task") (.setAttemptId "attempt")
                                        (.setState ExecutionState/EXECUTION_STATE_RUNNING)
                                        (.setStartTime timestamp) (.setEndTime timestamp)
                                        (.setProgress progress) (.addMetrics metric)
                                        (.setStragglerInfo straggler-info))))
              (.addWorkItems (WorkItemDetails/getDefaultInstance)))))

(deftest execution-details-test
  (let [settings (.build (MetricsV1Beta3StubSettings/newBuilder))]
    (doseq [[operation args call-settings response-page expected raw-expected]
            [[dataflow/get-job-execution-details ["project" "job"] (.getJobExecutionDetailsSettings settings)
              (fn [first?] (.build (if first?
                                    (-> (JobExecutionDetails/newBuilder) (.addStages stage) (.setNextPageToken "next"))
                                    (.addStages (JobExecutionDetails/newBuilder) (StageSummary/getDefaultInstance)))))
              [{:stage-id "stage" :state "EXECUTION_STATE_RUNNING" :start-time time-clj :end-time time-clj
                :progress progress-clj :metrics [metric-clj] :straggler-summary straggler-summary-clj}
               {:stage-id "" :state "EXECUTION_STATE_UNKNOWN" :start-time nil :end-time nil
                :progress nil :metrics [] :straggler-summary nil}]
              [stage (StageSummary/getDefaultInstance)]]
             [dataflow/get-stage-execution-details ["project" "job" "stage"] (.getStageExecutionDetailsSettings settings)
              (fn [first?] (.build (if first?
                                    (-> (StageExecutionDetails/newBuilder) (.addWorkers worker) (.setNextPageToken "next"))
                                    (.addWorkers (StageExecutionDetails/newBuilder) (WorkerDetails/getDefaultInstance)))))
              [{:worker-name "worker"
                :work-items [{:task-id "task" :attempt-id "attempt" :state "EXECUTION_STATE_RUNNING"
                              :start-time time-clj :end-time time-clj :progress progress-clj :metrics [metric-clj]
                              :straggler-info straggler-info-clj}
                             {:task-id "" :attempt-id "" :state "EXECUTION_STATE_UNKNOWN"
                              :start-time nil :end-time nil :progress nil :metrics [] :straggler-info nil}]}
               {:worker-name "" :work-items []}]
              [worker (WorkerDetails/getDefaultInstance)]]]
            raw? [false true]]
      (testing (str operation " raw?=" raw?)
        (let [closed (atom false)
              tokens (atom [])
              fetch (callable (fn [request]
                                (is (false? @closed) "Pagination happens before closing the client")
                                (is (= "project" (.getProjectId request)))
                                (is (= "job" (.getJobId request)))
                                (is (= "us-central1" (.getLocation request)))
                                (when (= 3 (count args))
                                  (is (= "stage" (.getStageId request))))
                                (swap! tokens conj (.getPageToken request))
                                (response-page (empty? (.getPageToken request)))))
              paged (callable (fn [request]
                                (.get (.getFuturePagedResponse
                                       ^PagedListResponseFactory (.getPagedListResponseFactory call-settings)
                                       fetch request (GrpcCallContext/createDefault) (.futureCall fetch request)))))
              client (MetricsV1Beta3Client/create
                      (proxy [MetricsV1Beta3Stub] []
                        (getJobExecutionDetailsPagedCallable [] paged)
                        (getStageExecutionDetailsPagedCallable [] paged)
                        (close [] (reset! closed true))))]
          (with-redefs [dataflow/create-client (fn [service] (is (= :metrics service)) client)]
            (let [result (apply operation (conj args {:location "us-central1" :raw? raw?}))]
              (is (vector? result))
              (is (= (if raw? raw-expected expected) result))
              (is (= ["" "next"] @tokens))
              (is (true? @closed)))))))))
