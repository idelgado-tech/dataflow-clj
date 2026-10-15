(ns dataflow-clj.messages-test
  (:require [clojure.test :refer [deftest is]]
            [dataflow-clj.core :as dataflow])
  (:import (com.google.api.core ApiFutures)
           (com.google.api.gax.grpc GrpcCallContext)
           (com.google.api.gax.rpc UnaryCallable PagedListResponseFactory)
           (com.google.dataflow.v1beta3 JobMessage JobMessageImportance
                                       ListJobMessagesResponse MessagesV1Beta3Client)
           (com.google.dataflow.v1beta3.stub MessagesV1Beta3Stub MessagesV1Beta3StubSettings)
           (com.google.protobuf Timestamp)
           (java.time LocalDateTime)))

(defn- callable [f]
  (proxy [UnaryCallable] []
    (futureCall
      ([request] (ApiFutures/immediateFuture (f request)))
      ([request context] (ApiFutures/immediateFuture (f request))))))

(deftest list-job-messages-test
  (let [message (.build (-> (JobMessage/newBuilder)
                           (.setId "message-1")
                           (.setTime (.build (-> (Timestamp/newBuilder)
                                                (.setSeconds 1)
                                                (.setNanos 123))))
                           (.setMessageText "Worker started")
                           (.setMessageImportance JobMessageImportance/JOB_MESSAGE_BASIC)))
        missing-time (JobMessage/getDefaultInstance)]
    (doseq [raw? [false true]]
      (let [closed (atom false)
            tokens (atom [])
            fetch (callable
                   (fn [request]
                     (is (false? @closed) "Fetch every page before closing the client")
                     (is (= "project" (.getProjectId request)))
                     (is (= "job" (.getJobId request)))
                     (is (= "us-central1" (.getLocation request)))
                     (swap! tokens conj (.getPageToken request))
                     (.build (if (empty? (.getPageToken request))
                               (-> (ListJobMessagesResponse/newBuilder)
                                   (.addJobMessages message)
                                   (.setNextPageToken "next"))
                               (-> (ListJobMessagesResponse/newBuilder)
                                   (.addJobMessages missing-time))))))
            settings (.listJobMessagesSettings (.build (MessagesV1Beta3StubSettings/newBuilder)))
            client (MessagesV1Beta3Client/create
                    (proxy [MessagesV1Beta3Stub] []
                      (listJobMessagesPagedCallable []
                        (callable
                         (fn [request]
                           (.get (.getFuturePagedResponse
                                  ^PagedListResponseFactory (.getPagedListResponseFactory settings)
                                  fetch request (GrpcCallContext/createDefault)
                                  (.futureCall fetch request))))))
                      (close [] (reset! closed true))))]
        (with-redefs [dataflow/create-client (fn [service]
                                             (is (= :messages service))
                                             client)]
          (let [result (dataflow/list-job-messages "project" "job"
                                                  {:location "us-central1" :raw? raw?})]
            (is (vector? result))
            (is (= (if raw?
                     [message missing-time]
                     [{:id "message-1"
                       :time (LocalDateTime/of 1970 1 1 0 0 1 123)
                       :message-text "Worker started"
                       :message-importance "JOB_MESSAGE_BASIC"}
                      {:id "" :time nil :message-text ""
                       :message-importance "JOB_MESSAGE_IMPORTANCE_UNKNOWN"}])
                   result))
            (is (= ["" "next"] @tokens))
            (is (true? @closed))))))))
