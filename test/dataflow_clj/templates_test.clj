(ns dataflow-clj.templates-test
  (:require [clojure.test :refer [deftest is testing]]
            [dataflow-clj.core :as dataflow])
  (:import (com.google.api.core ApiFutures)
           (com.google.api.gax.rpc UnaryCallable)
           (com.google.dataflow.v1beta3 Job GetTemplateResponse GetTemplateResponse$TemplateType
                                       LaunchTemplateResponse LaunchFlexTemplateResponse
                                       LaunchTemplateParameters LaunchFlexTemplateParameter
                                       TemplateMetadata ParameterMetadata ParameterMetadataEnumOption
                                       ParameterType RuntimeMetadata SDKInfo SDKInfo$Language
                                       TemplatesServiceClient FlexTemplatesServiceClient)
           (com.google.dataflow.v1beta3.stub TemplatesServiceStub FlexTemplatesServiceStub)
           (com.google.protobuf Any ByteString)
           (com.google.rpc Status)))

(defn- callable [f]
  (proxy [UnaryCallable] []
    (futureCall
      ([request] (ApiFutures/immediateFuture (f request)))
      ([request context] (ApiFutures/immediateFuture (f request))))))

(defn- templates-client [handler closed]
  (TemplatesServiceClient/create
   (proxy [TemplatesServiceStub] []
     (getTemplateCallable [] (callable #(handler :get %)))
     (launchTemplateCallable [] (callable #(handler :launch %)))
     (createJobFromTemplateCallable [] (callable #(handler :create %)))
     (close [] (swap! closed inc)))))

(defn- flex-client [handler closed]
  (FlexTemplatesServiceClient/create
   (proxy [FlexTemplatesServiceStub] []
     (launchFlexTemplateCallable [] (callable #(handler :flex %)))
     (close [] (swap! closed inc)))))

(def parameter
  (.build (-> (ParameterMetadata/newBuilder)
              (.setName "input") (.setLabel "Input") (.setHelpText "Input path")
              (.setIsOptional true) (.addRegexes "gs://.*")
              (.setParamType ParameterType/TEXT)
              (.putAllCustomMetadata {"custom-key" "value"})
              (.setGroupName "source") (.setParentName "mode")
              (.addParentTriggerValues "file")
              (.addEnumOptions (.build (-> (ParameterMetadataEnumOption/newBuilder)
                                          (.setValue "gs://bucket/input")
                                          (.setLabel "Example") (.setDescription "Example path"))))
              (.setDefaultValue "gs://bucket/input") (.setHiddenUi false))))

(def parameter-clj
  {:name "input" :label "Input" :help-text "Input path" :is-optional true
   :regexes ["gs://.*"] :param-type "TEXT" :custom-metadata {"custom-key" "value"}
   :group-name "source" :parent-name "mode" :parent-trigger-values ["file"]
   :enum-options [{:value "gs://bucket/input" :label "Example" :description "Example path"}]
   :default-value "gs://bucket/input" :hidden-ui false})

(def template-response
  (.build (-> (GetTemplateResponse/newBuilder)
              (.setTemplateType GetTemplateResponse$TemplateType/FLEX)
              (.setStatus (.build (-> (Status/newBuilder)
                                     (.setCode 3) (.setMessage "Invalid parameter")
                                     (.addDetails (.build (-> (Any/newBuilder)
                                                             (.setTypeUrl "type.googleapis.com/example.Detail")
                                                             (.setValue (ByteString/copyFromUtf8 "detail"))))))))
              (.setMetadata (.build (-> (TemplateMetadata/newBuilder)
                                       (.setName "template") (.setDescription "Example template")
                                       (.addParameters parameter) (.setStreaming true)
                                       (.setSupportsAtLeastOnce true) (.setSupportsExactlyOnce false)
                                       (.setDefaultStreamingMode "AT_LEAST_ONCE"))))
              (.setRuntimeMetadata (.build (-> (RuntimeMetadata/newBuilder)
                                              (.addParameters parameter)
                                              (.setSdkInfo (.build (-> (SDKInfo/newBuilder)
                                                                       (.setLanguage SDKInfo$Language/JAVA)
                                                                       (.setVersion "2.0"))))))))))

(def template-clj
  {:status {:code 3 :message "Invalid parameter"
            :details [{:type-url "type.googleapis.com/example.Detail" :value "ZGV0YWls"}]}
   :metadata {:name "template" :description "Example template" :parameters [parameter-clj]
              :streaming true :supports-at-least-once true :supports-exactly-once false
              :default-streaming-mode "AT_LEAST_ONCE"}
   :template-type "FLEX"
   :runtime-metadata {:sdk-info {:language "JAVA" :version "2.0"} :parameters [parameter-clj]}})

(deftest template-responses-test
  (let [job (.build (-> (Job/newBuilder) (.setId "job") (.setName "pipeline")))
        job-clj {:id "job" :name "pipeline"
                 :metadata {:sdk-support-status "UNKNOWN" :sdk-version ""}
                 :start-time nil :create-time nil :current-status "JOB_STATE_UNKNOWN"
                 :current-status-time nil :time-to-status nil}]
    (doseq [[operation args factory service method response expected]
            [[dataflow/get-template ["p" "gs://bucket/template"] templates-client :templates :get
              template-response template-clj]
             [dataflow/get-template ["p" "gs://bucket/template"] templates-client :templates :get
              (GetTemplateResponse/getDefaultInstance)
              {:status nil :metadata nil :template-type "UNKNOWN" :runtime-metadata nil}]
             [dataflow/launch-template ["p" "gs://bucket/template" (LaunchTemplateParameters/getDefaultInstance)]
              templates-client :templates :launch
              (.build (.setJob (LaunchTemplateResponse/newBuilder) job)) {:job job-clj}]
             [dataflow/launch-template ["p" "gs://bucket/template" (LaunchTemplateParameters/getDefaultInstance)]
              templates-client :templates :launch (LaunchTemplateResponse/getDefaultInstance) {:job nil}]
             [dataflow/launch-flex-template ["p" (LaunchFlexTemplateParameter/getDefaultInstance)]
              flex-client :flex-templates :flex
              (.build (.setJob (LaunchFlexTemplateResponse/newBuilder) job)) {:job job-clj}]
             [dataflow/launch-flex-template ["p" (LaunchFlexTemplateParameter/getDefaultInstance)]
              flex-client :flex-templates :flex (LaunchFlexTemplateResponse/getDefaultInstance) {:job nil}]
             [dataflow/create-job-from-template ["p" "gs://bucket/template" "pipeline"]
              templates-client :templates :create job job-clj]]
            raw? [false true]]
      (testing (str method " raw?=" raw?)
        (let [closed (atom 0)
              client (factory (fn [actual-method request]
                                (is (= method actual-method))
                                (is (= "p" (.getProjectId request)))
                                (is (= "us-central1" (.getLocation request)))
                                (when (#{:launch :flex} method)
                                  (is (.getValidateOnly request)))
                                response) closed)
              options (cond-> {:location "us-central1" :raw? raw?}
                        (#{:launch :flex} method) (assoc :validate-only true))]
          (with-redefs [dataflow/create-client (fn [actual-service] (is (= service actual-service)) client)]
            (is (= (if raw? response expected) (apply operation (conj args options))))
            (is (= 1 @closed)))
          (is (= (if raw? response expected) (apply operation (conj args (assoc options :client client)))))
          (is (= 1 @closed) "Caller-owned client remains open"))))))
