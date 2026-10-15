(ns dataflow-clj.core
  (:require
   [clojure.string :as str]
   [clojure.tools.logging :as log])
  (:import
   (java.time LocalDateTime
              Duration)
   (com.google.dataflow.v1beta3 Job JobMessage
                                JobsV1Beta3Client
                                StageSummary WorkerDetails WorkItemDetails
                                ProgressTimeseries ProgressTimeseries$Point
                                MetricUpdate MetricStructuredName StragglerSummary Straggler
                                StragglerInfo StragglerInfo$StragglerDebuggingInfo
                                StreamingStragglerInfo HotKeyDebuggingInfo$HotKeyInfo
                                GetJobRequest
                                JobState JobView ListJobsRequest$Filter JobMessageImportance
                                GetTemplateRequest$TemplateView
                                GetTemplateResponse LaunchTemplateResponse LaunchFlexTemplateResponse
                                TemplateMetadata ParameterMetadata ParameterMetadataEnumOption
                                RuntimeMetadata SDKInfo
                                LaunchTemplateParameters LaunchFlexTemplateParameter
                                RuntimeEnvironment DynamicTemplateLaunchParams
                                ListJobsRequest
                                CreateJobRequest UpdateJobRequest CheckActiveJobsRequest
                                SnapshotJobRequest ListJobMessagesRequest
                                GetJobMetricsRequest GetJobExecutionDetailsRequest
                                GetStageExecutionDetailsRequest CreateJobFromTemplateRequest
                                LaunchTemplateRequest GetTemplateRequest LaunchFlexTemplateRequest
                                GetSnapshotRequest DeleteSnapshotRequest ListSnapshotsRequest
                                MessagesV1Beta3Client MetricsV1Beta3Client
                                TemplatesServiceClient FlexTemplatesServiceClient
                                SnapshotsV1Beta3Client)))

;;-------------------------------------
;;               utils
;;-------------------------------------


(defn timestamp->LocalDateTime [timestamp]
  (LocalDateTime/ofInstant (java.time.Instant/ofEpochSecond (.getSeconds timestamp) (.getNanos timestamp)) (java.time.ZoneId/of "UTC")))

(defn duration-between-times-human [date-1 date2]
  (let [d (Duration/between date-1 date2)]
    (format "%sd %sh %sm %ss",
            (.toDaysPart d),
            (.toHoursPart d),
            (.toMinutesPart d),
            (.toSecondsPart d))))

(defn- ->java-enum-name
  "transform a kebab case string or keyword into a java enum"
  [value]
  (-> (if (keyword? value) (name value) (str value))
      str/upper-case
      (str/replace "-" "_")))

;;-------------------------------------
;;           Job State
;;-------------------------------------


;;Job state used for the filter request
(def job-state->value {:active     3
                       :terminated 2
                       :all        1
                       :unknown    0})

;;Job state with full information in Job Object
(def job-status->description {"JOB_STATE_CANCELLED"
                              "indicates that the job has been explicitly cancelled. This is a terminal job state. This state may only be set via a Cloud Dataflow UpdateJob call, and only if the job has not yet reached another terminal state."

                              "JOB_STATE_CANCELLING"
                              "indicates that the job has been explicitly cancelled and is in the process of stopping. Jobs that are cancelling may only transition to JOB_STATE_CANCELLED or JOB_STATE_FAILED."

                              "JOB_STATE_DONE"
                              "indicates that the job has successfully completed. This is a terminal job state. This state may be set by the Cloud Dataflow service, as a transition from JOB_STATE_RUNNING. It may also be set via a Cloud Dataflow UpdateJob call, if the job has not yet reached a terminal state."

                              "JOB_STATE_DRAINED"
                              "indicates that the job has been drained. A drained job terminated by stopping pulling from its input sources and processing any data that remained in-flight when draining was requested. This state is a terminal state, may only be set by the Cloud Dataflow service"

                              "JOB_STATE_DRAINING"
                              "indicates that the job is in the process of draining. A draining job has stopped pulling from its input sources and is processing any data that remains in-flight. This state may be set via a Cloud Dataflow UpdateJob call, but only as a transition from JOB_STATE_RUNNING. Jobs that are draining may only transition to JOB_STATE_DRAINED, JOB_STATE_CANCELLED, or JOB_STATE_FAILED."

                              "JOB_STATE_FAILED"
                              "indicates that the job has failed. This is a terminal job state. This state may only be set by the Cloud Dataflow service, and only as a transition from JOB_STATE_RUNNING."

                              "JOB_STATE_PAUSING"
                              "indicates that the job is in the process of pausing."

                              "JOB_STATE_PAUSED"
                              "indicates that the job has been paused."

                              "JOB_STATE_PENDING"
                              "indicates that the job has been created but is not yet running. Jobs that are pending may only transition to JOB_STATE_RUNNING, or JOB_STATE_FAILED."

                              "JOB_STATE_QUEUED"
                              "indicates that the job has been created but is being delayed until launch. Jobs that are queued may only transition to JOB_STATE_PENDING or JOB_STATE_CANCELLED."

                              "JOB_STATE_RESOURCE_CLEANING_UP"
                              "indicates that the batch job's associated resources are currently being cleaned up after a successful run. Currently, this is an opt-in feature, please reach out to Cloud support team if you are interested."

                              "JOB_STATE_RUNNING"
                              "indicates that the job is currently running."

                              "JOB_STATE_STOPPED"
                              "indicates that the job has not yet started to run."

                              "JOB_STATE_UNKNOWN"
                              "The job's run state isn't specified."

                              "JOB_STATE_UPDATED"
                              "indicates that the job was successfully updated, meaning that this job was stopped and another job was started, inheriting state from this one. This is a terminal job state. This state may only be set by the Cloud Dataflow service, and only as a transition from JOB_STATE_RUNNING."

                              "UNRECOGNIZED"
                              ""})

(def job-status (set (keys job-status->description)))

;;-------------------------------------
;;             Requests
;;-------------------------------------

(defn ->ListJobsRequest [project-id {:keys [filter-state filter location name page-size page-token view]}]
  (when (and filter-state (not (contains? job-state->value filter-state)))
    (throw (ex-info "Unknown job filter" {:filter-state filter-state})))
  (.build (cond-> (-> (ListJobsRequest/newBuilder)
                      (.setProjectId project-id))
            (some? filter) (.setFilter (ListJobsRequest$Filter/valueOf (->java-enum-name filter)))
            (some? location) (.setLocation location)
            (some? name) (.setName name)
            (some? page-size) (.setPageSize page-size)
            (some? page-token) (.setPageToken page-token)
            (some? view) (.setView (JobView/valueOf (->java-enum-name view)))
            filter-state (.setFilter (ListJobsRequest$Filter/forNumber (job-state->value filter-state))))))

(defn ->GetJobRequest [project-id job-id {:keys [location view]}]
  (.build (cond-> (-> (GetJobRequest/newBuilder)
                      (.setProjectId project-id)
                      (.setJobId job-id))
            (some? location) (.setLocation location)
            (some? view) (.setView (JobView/valueOf (->java-enum-name view))))))

(defn ->CreateJobRequest [project-id ^Job job {:keys [location view replace-job-id]}]
  (.build (cond-> (-> (CreateJobRequest/newBuilder)
                      (.setProjectId project-id)
                      (.setJob job))
            (some? location) (.setLocation location)
            (some? view) (.setView (JobView/valueOf (->java-enum-name view)))
            (some? replace-job-id) (.setReplaceJobId replace-job-id))))

(defn ->UpdateJobRequest [project-id job-id ^Job job {:keys [location ^com.google.protobuf.FieldMask update-mask]}]
  (.build (cond-> (-> (UpdateJobRequest/newBuilder)
                      (.setProjectId project-id)
                      (.setJobId job-id)
                      (.setJob job))
            (some? location) (.setLocation location)
            (some? update-mask) (.setUpdateMask update-mask))))

(defn ->CheckActiveJobsRequest [project-id _options]
  (.build (-> (CheckActiveJobsRequest/newBuilder)
              (.setProjectId project-id))))

(defn ->SnapshotJobRequest [project-id job-id {:keys [location ^com.google.protobuf.Duration ttl snapshot-sources description]}]
  (.build (cond-> (-> (SnapshotJobRequest/newBuilder)
                      (.setProjectId project-id)
                      (.setJobId job-id))
            (some? location) (.setLocation location)
            (some? ttl) (.setTtl ttl)
            (some? snapshot-sources) (.setSnapshotSources snapshot-sources)
            (some? description) (.setDescription description))))

(defn ->ListJobMessagesRequest [project-id job-id {:keys [location minimum-importance page-size page-token ^com.google.protobuf.Timestamp start-time ^com.google.protobuf.Timestamp end-time]}]
  (.build (cond-> (-> (ListJobMessagesRequest/newBuilder)
                      (.setProjectId project-id)
                      (.setJobId job-id))
            (some? location) (.setLocation location)
            (some? minimum-importance) (.setMinimumImportance (JobMessageImportance/valueOf (->java-enum-name minimum-importance)))
            (some? page-size) (.setPageSize page-size)
            (some? page-token) (.setPageToken page-token)
            (some? start-time) (.setStartTime start-time)
            (some? end-time) (.setEndTime end-time))))

(defn ->GetJobMetricsRequest [project-id job-id {:keys [location ^com.google.protobuf.Timestamp start-time]}]
  (.build (cond-> (-> (GetJobMetricsRequest/newBuilder)
                      (.setProjectId project-id)
                      (.setJobId job-id))
            (some? location) (.setLocation location)
            (some? start-time) (.setStartTime start-time))))

(defn ->GetJobExecutionDetailsRequest [project-id job-id {:keys [location page-size page-token]}]
  (.build (cond-> (-> (GetJobExecutionDetailsRequest/newBuilder)
                      (.setProjectId project-id)
                      (.setJobId job-id))
            (some? location) (.setLocation location)
            (some? page-size) (.setPageSize page-size)
            (some? page-token) (.setPageToken page-token))))

(defn ->GetStageExecutionDetailsRequest [project-id job-id stage-id {:keys [location page-size page-token ^com.google.protobuf.Timestamp start-time ^com.google.protobuf.Timestamp end-time]}]
  (.build (cond-> (-> (GetStageExecutionDetailsRequest/newBuilder)
                      (.setProjectId project-id)
                      (.setJobId job-id)
                      (.setStageId stage-id))
            (some? location) (.setLocation location)
            (some? page-size) (.setPageSize page-size)
            (some? page-token) (.setPageToken page-token)
            (some? start-time) (.setStartTime start-time)
            (some? end-time) (.setEndTime end-time))))

(defn ->CreateJobFromTemplateRequest [project-id gcs-path job-name {:keys [location parameters ^RuntimeEnvironment environment]}]
  (.build (cond-> (-> (CreateJobFromTemplateRequest/newBuilder)
                      (.setProjectId project-id)
                      (.setGcsPath gcs-path)
                      (.setJobName job-name))
            (some? location) (.setLocation location)
            (some? parameters) (.putAllParameters parameters)
            (some? environment) (.setEnvironment environment))))

(defn ->LaunchTemplateRequest [project-id gcs-path ^LaunchTemplateParameters launch-parameters {:keys [location validate-only ^DynamicTemplateLaunchParams dynamic-template]}]
  (.build (cond-> (-> (LaunchTemplateRequest/newBuilder)
                      (.setProjectId project-id)
                      (.setLaunchParameters launch-parameters))
            (some? gcs-path) (.setGcsPath gcs-path)
            (some? location) (.setLocation location)
            (some? validate-only) (.setValidateOnly validate-only)
            (some? dynamic-template) (.setDynamicTemplate dynamic-template))))

(defn ->GetTemplateRequest [project-id gcs-path {:keys [location view]}]
  (.build (cond-> (-> (GetTemplateRequest/newBuilder)
                      (.setProjectId project-id)
                      (.setGcsPath gcs-path))
            (some? location) (.setLocation location)
            (some? view) (.setView (GetTemplateRequest$TemplateView/valueOf (->java-enum-name view))))))

(defn ->LaunchFlexTemplateRequest [project-id ^LaunchFlexTemplateParameter launch-parameter {:keys [location validate-only]}]
  (.build (cond-> (-> (LaunchFlexTemplateRequest/newBuilder)
                      (.setProjectId project-id)
                      (.setLaunchParameter launch-parameter))
            (some? location) (.setLocation location)
            (some? validate-only) (.setValidateOnly validate-only))))

(defn ->GetSnapshotRequest [project-id snapshot-id {:keys [location]}]
  (.build (cond-> (-> (GetSnapshotRequest/newBuilder)
                      (.setProjectId project-id)
                      (.setSnapshotId snapshot-id))
            (some? location) (.setLocation location))))

(defn ->DeleteSnapshotRequest [project-id snapshot-id {:keys [location]}]
  (.build (cond-> (-> (DeleteSnapshotRequest/newBuilder)
                      (.setProjectId project-id)
                      (.setSnapshotId snapshot-id))
            (some? location) (.setLocation location))))

(defn ->ListSnapshotsRequest [project-id {:keys [location job-id]}]
  (.build (cond-> (-> (ListSnapshotsRequest/newBuilder)
                      (.setProjectId project-id))
            (some? location) (.setLocation location)
            (some? job-id) (.setJobId job-id))))

;;-----------------------------------
;;          Client
;;-----------------------------------

(defn create-client
  "Create a closeable Java client using Application Default Credentials."
  [service]
  (case service
    :jobs (JobsV1Beta3Client/create)
    :messages (MessagesV1Beta3Client/create)
    :metrics (MetricsV1Beta3Client/create)
    :templates (TemplatesServiceClient/create)
    :flex-templates (FlexTemplatesServiceClient/create)
    :snapshots (SnapshotsV1Beta3Client/create)))

(defn- call-with-client [service {:keys [client]} f]
  (try
    (if client
      (f client)
      (with-open [client (create-client service)]
        (f client)))
    (catch Exception e
      (log/errorf e "Exception calling Dataflow [service=%s]" (name service))
      (throw e))))


;;-----------------------------------
;;          Job Parsing
;;-----------------------------------


(defn- job->create-time [^Job job]
  (when (.hasCreateTime job)
    (timestamp->LocalDateTime (.getCreateTime job))))

(defn- job->current-state [^Job job]
  (->
   job
   .getCurrentState
   .toString))

(defn- job->current-state-time [^Job job]
  (when (.hasCurrentStateTime job)
    (timestamp->LocalDateTime (.getCurrentStateTime job))))

(defn- job->name [^Job job]
  (.getName job))

(defn- job->start-time [^Job job]
  (when (.hasStartTime job)
    (timestamp->LocalDateTime (.getStartTime job))))

(defn- metadata->clj [metadata]
  (let [sdk (.getSdkVersion metadata)]
    {:sdk-support-status (str (.getSdkSupportStatus sdk))
     :sdk-version (.getVersion sdk)}))

(defn- Job->clj [^Job job]
  (let [start-time (job->start-time job)
        current-status-time (job->current-state-time job)]
    {:id (.getId job)
     :name (job->name job)
     :metadata (metadata->clj (.getJobMetadata job))
     :start-time start-time
     :create-time (job->create-time job)
     :current-status (job->current-state job)
     :current-status-time current-status-time
     :time-to-status (when (and start-time current-status-time)
                       (duration-between-times-human start-time current-status-time))}))

(defn- ParameterMetadataEnumOption->clj [^ParameterMetadataEnumOption option]
  {:value (.getValue option)
   :label (.getLabel option)
   :description (.getDescription option)})

(defn- ParameterMetadata->clj [^ParameterMetadata parameter]
  {:name (.getName parameter)
   :label (.getLabel parameter)
   :help-text (.getHelpText parameter)
   :is-optional (.getIsOptional parameter)
   :regexes (vec (.getRegexesList parameter))
   :param-type (str (.getParamType parameter))
   :custom-metadata (into {} (.getCustomMetadataMap parameter))
   :group-name (.getGroupName parameter)
   :parent-name (.getParentName parameter)
   :parent-trigger-values (vec (.getParentTriggerValuesList parameter))
   :enum-options (mapv ParameterMetadataEnumOption->clj (.getEnumOptionsList parameter))
   :default-value (.getDefaultValue parameter)
   :hidden-ui (.getHiddenUi parameter)})

(defn- TemplateMetadata->clj [^TemplateMetadata metadata]
  {:name (.getName metadata)
   :description (.getDescription metadata)
   :parameters (mapv ParameterMetadata->clj (.getParametersList metadata))
   :streaming (.getStreaming metadata)
   :supports-at-least-once (.getSupportsAtLeastOnce metadata)
   :supports-exactly-once (.getSupportsExactlyOnce metadata)
   :default-streaming-mode (.getDefaultStreamingMode metadata)})

(defn- SDKInfo->clj [^SDKInfo sdk]
  {:language (str (.getLanguage sdk))
   :version (.getVersion sdk)})

(defn- RuntimeMetadata->clj [^RuntimeMetadata metadata]
  {:sdk-info (when (.hasSdkInfo metadata)
               (SDKInfo->clj (.getSdkInfo metadata)))
   :parameters (mapv ParameterMetadata->clj (.getParametersList metadata))})

(defn- Status->clj [^com.google.rpc.Status status]
  {:code (.getCode status)
   :message (.getMessage status)
   :details (mapv (fn [^com.google.protobuf.Any detail]
                    {:type-url (.getTypeUrl detail)
                     :value (.encodeToString (java.util.Base64/getEncoder)
                                             (.toByteArray (.getValue detail)))})
                  (.getDetailsList status))})

(defn- GetTemplateResponse->clj [^GetTemplateResponse response]
  {:status (when (.hasStatus response)
             (Status->clj (.getStatus response)))
   :metadata (when (.hasMetadata response)
               (TemplateMetadata->clj (.getMetadata response)))
   :template-type (str (.getTemplateType response))
   :runtime-metadata (when (.hasRuntimeMetadata response)
                       (RuntimeMetadata->clj (.getRuntimeMetadata response)))})

(defn- LaunchTemplateResponse->clj [^LaunchTemplateResponse response]
  {:job (when (.hasJob response)
          (Job->clj (.getJob response)))})

(defn- LaunchFlexTemplateResponse->clj [^LaunchFlexTemplateResponse response]
  {:job (when (.hasJob response)
          (Job->clj (.getJob response)))})

(defn- JobMessage->clj [^JobMessage message]
  {:id (.getId message)
   :time (when (.hasTime message)
           (timestamp->LocalDateTime (.getTime message)))
   :message-text (.getMessageText message)
   :message-importance (str (.getMessageImportance message))})

(defn- duration->Duration [^com.google.protobuf.Duration duration]
  (Duration/ofSeconds (.getSeconds duration) (.getNanos duration)))

(defn- Value->clj [^com.google.protobuf.Value value]
  (case (str (.getKindCase value))
    "NUMBER_VALUE" (.getNumberValue value)
    "STRING_VALUE" (.getStringValue value)
    "BOOL_VALUE" (.getBoolValue value)
    "STRUCT_VALUE" (into {} (map (fn [[k v]] [k (Value->clj v)]))
                         (.getFieldsMap (.getStructValue value)))
    "LIST_VALUE" (mapv Value->clj (.getValuesList (.getListValue value)))
    nil))

(defn- MetricStructuredName->clj [^MetricStructuredName metric-name]
  {:origin (.getOrigin metric-name)
   :name (.getName metric-name)
   :context (into {} (.getContextMap metric-name))})

(defn- MetricUpdate->clj [^MetricUpdate metric]
  {:name (when (.hasName metric)
           (MetricStructuredName->clj (.getName metric)))
   :kind (.getKind metric)
   :cumulative (.getCumulative metric)
   :scalar (when (.hasScalar metric) (Value->clj (.getScalar metric)))
   :mean-sum (when (.hasMeanSum metric) (Value->clj (.getMeanSum metric)))
   :mean-count (when (.hasMeanCount metric) (Value->clj (.getMeanCount metric)))
   :set (when (.hasSet metric) (Value->clj (.getSet metric)))
   :trie (when (.hasTrie metric) (Value->clj (.getTrie metric)))
   :bounded-trie (when (.hasBoundedTrie metric) (Value->clj (.getBoundedTrie metric)))
   :distribution (when (.hasDistribution metric) (Value->clj (.getDistribution metric)))
   :gauge (when (.hasGauge metric) (Value->clj (.getGauge metric)))
   :internal (when (.hasInternal metric) (Value->clj (.getInternal metric)))
   :update-time (when (.hasUpdateTime metric)
                  (timestamp->LocalDateTime (.getUpdateTime metric)))})

(defn- ProgressPoint->clj [^ProgressTimeseries$Point point]
  {:time (when (.hasTime point)
           (timestamp->LocalDateTime (.getTime point)))
   :value (.getValue point)})

(defn- ProgressTimeseries->clj [^ProgressTimeseries progress]
  {:current-progress (.getCurrentProgress progress)
   :data-points (mapv ProgressPoint->clj (.getDataPointsList progress))})

(defn- HotKeyInfo->clj [^HotKeyDebuggingInfo$HotKeyInfo hot-key]
  {:hot-key-age (when (.hasHotKeyAge hot-key)
                  (duration->Duration (.getHotKeyAge hot-key)))
   :key (.getKey hot-key)
   :key-truncated (.getKeyTruncated hot-key)})

(defn- StragglerDebuggingInfo->clj [^StragglerInfo$StragglerDebuggingInfo info]
  {:hot-key (when (.hasHotKey info)
              {:detected-hot-keys (into {} (map (fn [[k v]] [k (HotKeyInfo->clj v)]))
                                        (.getDetectedHotKeysMap (.getHotKey info)))})})

(defn- StragglerInfo->clj [^StragglerInfo info]
  {:start-time (when (.hasStartTime info)
                 (timestamp->LocalDateTime (.getStartTime info)))
   :causes (into {} (map (fn [[k v]] [k (StragglerDebuggingInfo->clj v)]))
                 (.getCausesMap info))})

(defn- StreamingStragglerInfo->clj [^StreamingStragglerInfo info]
  {:start-time (when (.hasStartTime info)
                 (timestamp->LocalDateTime (.getStartTime info)))
   :end-time (when (.hasEndTime info)
               (timestamp->LocalDateTime (.getEndTime info)))
   :worker-name (.getWorkerName info)
   :data-watermark-lag (when (.hasDataWatermarkLag info)
                         (duration->Duration (.getDataWatermarkLag info)))
   :system-watermark-lag (when (.hasSystemWatermarkLag info)
                           (duration->Duration (.getSystemWatermarkLag info)))})

(defn- Straggler->clj [^Straggler straggler]
  {:batch-straggler (when (.hasBatchStraggler straggler)
                      (StragglerInfo->clj (.getBatchStraggler straggler)))
   :streaming-straggler (when (.hasStreamingStraggler straggler)
                          (StreamingStragglerInfo->clj (.getStreamingStraggler straggler)))})

(defn- StragglerSummary->clj [^StragglerSummary summary]
  {:total-straggler-count (.getTotalStragglerCount summary)
   :straggler-cause-count (into {} (.getStragglerCauseCountMap summary))
   :recent-stragglers (mapv Straggler->clj (.getRecentStragglersList summary))})

(defn- StageSummary->clj [^StageSummary stage]
  {:stage-id (.getStageId stage)
   :state (str (.getState stage))
   :start-time (when (.hasStartTime stage)
                 (timestamp->LocalDateTime (.getStartTime stage)))
   :end-time (when (.hasEndTime stage)
               (timestamp->LocalDateTime (.getEndTime stage)))
   :progress (when (.hasProgress stage)
               (ProgressTimeseries->clj (.getProgress stage)))
   :metrics (mapv MetricUpdate->clj (.getMetricsList stage))
   :straggler-summary (when (.hasStragglerSummary stage)
                        (StragglerSummary->clj (.getStragglerSummary stage)))})

(defn- WorkItemDetails->clj [^WorkItemDetails work-item]
  {:task-id (.getTaskId work-item)
   :attempt-id (.getAttemptId work-item)
   :state (str (.getState work-item))
   :start-time (when (.hasStartTime work-item)
                 (timestamp->LocalDateTime (.getStartTime work-item)))
   :end-time (when (.hasEndTime work-item)
               (timestamp->LocalDateTime (.getEndTime work-item)))
   :progress (when (.hasProgress work-item)
               (ProgressTimeseries->clj (.getProgress work-item)))
   :metrics (mapv MetricUpdate->clj (.getMetricsList work-item))
   :straggler-info (when (.hasStragglerInfo work-item)
                     (StragglerInfo->clj (.getStragglerInfo work-item)))})

(defn- WorkerDetails->clj [^WorkerDetails worker]
  {:worker-name (.getWorkerName worker)
   :work-items (mapv WorkItemDetails->clj (.getWorkItemsList worker))})

;;-----------------------------------
;;          API
;;-----------------------------------

(defn- request-options [options defaults]
  (merge defaults (dissoc options :client :raw?)))


(defn- job-response->clj [job options]
  (if (:raw? options) job (Job->clj job)))

(defn list-jobs
  "Call Dataflow listJobs. Options accept request fields, :client."
  ([project-id]
   (list-jobs project-id {}))
  ([project-id options]
   (let [request (->ListJobsRequest project-id (request-options options {:filter :all :page-size 20 :location "europe-west1"}))]
     (call-with-client :jobs options
                       (fn [^JobsV1Beta3Client client]
                         (let [response (.listJobs client request)]
                           (mapv #(job-response->clj % options) (.iterateAll response))))))))

(defn aggregated-list-jobs
  "Call Dataflow aggregatedListJobs. Options accept request fields, :client."
  ([project-id]
   (aggregated-list-jobs project-id {}))
  ([project-id options]
   (let [request (->ListJobsRequest project-id (request-options options {:filter :all :page-size 20}))]
     (call-with-client :jobs options
                       (fn [^JobsV1Beta3Client client]
                         (let [response (.aggregatedListJobs client request)]
                           (mapv #(job-response->clj % options) (.iterateAll response))))))))

(defn get-job
  "Call Dataflow getJob. Options accept request fields, :client."
  ([project-id job-id]
   (get-job project-id job-id {}))
  ([project-id job-id options]
   (let [request (->GetJobRequest project-id job-id (request-options options {:location "europe-west1"}))]
     (call-with-client :jobs options
                       (fn [^JobsV1Beta3Client client]
                         (let [response (.getJob client request)]
                           (job-response->clj response options)))))))

(defn create-job
  "Call Dataflow createJob. Options accept request fields, :client."
  ([project-id job]
   (create-job project-id job {}))
  ([project-id job options]
   (let [request (->CreateJobRequest project-id job (request-options options {:location "europe-west1"}))]
     (call-with-client :jobs options
                       (fn [^JobsV1Beta3Client client]
                         (let [response (.createJob client request)]
                           (job-response->clj response options)))))))

(defn update-job
  "Call Dataflow updateJob. Options accept request fields, :client."
  ([project-id job-id job]
   (update-job project-id job-id job {}))
  ([project-id job-id job options]
   (let [request (->UpdateJobRequest project-id job-id job (request-options options {:location "europe-west1"}))]
     (call-with-client :jobs options
                       (fn [^JobsV1Beta3Client client]
                         (let [response (.updateJob client request)]
                           (job-response->clj response options)))))))

(defn cancel-job
  "Request cancellation of a job."
  ([project-id job-id] (cancel-job project-id job-id {}))
  ([project-id job-id options]
   (update-job project-id job-id (.build (.setRequestedState (Job/newBuilder) JobState/JOB_STATE_CANCELLED)) options)))

(defn drain-job
  "Request draining of a streaming job."
  ([project-id job-id] (drain-job project-id job-id {}))
  ([project-id job-id options]
   (update-job project-id job-id (.build (.setRequestedState (Job/newBuilder) JobState/JOB_STATE_DRAINING)) options)))

(defn ^:deprecated check-active-jobs
  "WARNING Exist in the SDK but Not supported by router Yet.
   Call Dataflow checkActiveJobs. Options accept request fields, :client."
  ([project-id]
   (check-active-jobs project-id {}))
  ([project-id options]
   (let [request (->CheckActiveJobsRequest project-id (request-options options {}))]
     (call-with-client :jobs options
                       (fn [^JobsV1Beta3Client client]
                         (let [response (.checkActiveJobs client request)]
                           response))))))

(defn list-active-jobs
  "The current CheckActiveJobsRequest is not implemented yet, this one uses aggregated-list-job with a filter instead.
   List current active jobs.
   Options accept request fields, :client."
  [project-id]
  (seq
   (aggregated-list-jobs    project-id
                            {:filter-state :active})))

(defn create-job-from-template
  "Call Dataflow createJobFromTemplate. Options accept request fields, :client."
  ([project-id gcs-path job-name]
   (create-job-from-template project-id gcs-path job-name {}))
  ([project-id gcs-path job-name options]
   (let [request (->CreateJobFromTemplateRequest project-id gcs-path job-name (request-options options {:location "europe-west1"}))]
     (call-with-client :templates options
                       (fn [^TemplatesServiceClient client]
                         (let [response (.createJobFromTemplate client request)]
                           (job-response->clj response options)))))))

(defn list-job-messages
  "Return job messages as maps with :id, :time, :message-text and :message-importance.
   Options accept request fields, :client and :raw? to return Java messages."
  ([project-id job-id]
   (list-job-messages project-id job-id {}))
  ([project-id job-id options]
   (let [request (->ListJobMessagesRequest project-id job-id (request-options options {:location "europe-west1"}))]
     (call-with-client :messages options
                       (fn [^MessagesV1Beta3Client client]
                         (let [response (.listJobMessages client request)]
                           (mapv #(if (:raw? options) % (JobMessage->clj %))
                                 (.iterateAll response))))))))



;;TODO Test and fix

(defn snapshot-job
  "Call Dataflow snapshotJob. Options accept request fields, :client."
  ([project-id job-id]
   (snapshot-job project-id job-id {}))
  ([project-id job-id options]
   (let [request (->SnapshotJobRequest project-id job-id (request-options options {:location "europe-west1"}))]
     (call-with-client :jobs options
                       (fn [^JobsV1Beta3Client client]
                         (let [response (.snapshotJob client request)]
                           response))))))


(defn get-job-metrics
  "Call Dataflow getJobMetrics. Options accept request fields, :client."
  ([project-id job-id]
   (get-job-metrics project-id job-id {}))
  ([project-id job-id options]
   (let [request (->GetJobMetricsRequest project-id job-id (request-options options {:location "europe-west1"}))]
     (call-with-client :metrics options
                       (fn [^MetricsV1Beta3Client client]
                         (let [response (.getJobMetrics client request)]
                           response))))))

(defn get-job-execution-details
  "Call Dataflow getJobExecutionDetails. Return stage details as maps.
   Options accept request fields, :client and :raw? to return Java objects."
  ([project-id job-id]
   (get-job-execution-details project-id job-id {}))
  ([project-id job-id options]
   (let [request (->GetJobExecutionDetailsRequest project-id job-id (request-options options {:location "europe-west1"}))]
     (call-with-client :metrics options
                       (fn [^MetricsV1Beta3Client client]
                         (let [response (.getJobExecutionDetails client request)]
                           (mapv #(if (:raw? options) % (StageSummary->clj %))
                                 (.iterateAll response))))))))

(defn get-stage-execution-details
  "Call Dataflow getStageExecutionDetails. Return worker details as maps.
   Options accept request fields, :client and :raw? to return Java objects."
  ([project-id job-id stage-id]
   (get-stage-execution-details project-id job-id stage-id {}))
  ([project-id job-id stage-id options]
   (let [request (->GetStageExecutionDetailsRequest project-id job-id stage-id (request-options options {:location "europe-west1"}))]
     (call-with-client :metrics options
                       (fn [^MetricsV1Beta3Client client]
                         (let [response (.getStageExecutionDetails client request)]
                           (mapv #(if (:raw? options) % (WorkerDetails->clj %))
                                 (.iterateAll response))))))))



(defn launch-template
  "Call Dataflow launchTemplate. Return a Clojure map.
   Options accept request fields, :client and :raw? to return the Java response."
  ([project-id gcs-path launch-parameters]
   (launch-template project-id gcs-path launch-parameters {}))
  ([project-id gcs-path launch-parameters options]
   (let [request (->LaunchTemplateRequest project-id gcs-path launch-parameters (request-options options {:location "europe-west1"}))]
     (call-with-client :templates options
                       (fn [^TemplatesServiceClient client]
                         (let [response (.launchTemplate client request)]
                           (if (:raw? options) response (LaunchTemplateResponse->clj response))))))))

(defn get-template
  "Call Dataflow getTemplate. Return template metadata as a Clojure map. Status detail :value fields are Base64 strings.
   Options accept request fields, :client and :raw? to return the Java response."
  ([project-id gcs-path]
   (get-template project-id gcs-path {}))
  ([project-id gcs-path options]
   (let [request (->GetTemplateRequest project-id gcs-path (request-options options {:location "europe-west1"}))]
     (call-with-client :templates options
                       (fn [^TemplatesServiceClient client]
                         (let [response (.getTemplate client request)]
                           (if (:raw? options) response (GetTemplateResponse->clj response))))))))

(defn launch-flex-template
  "Call Dataflow launchFlexTemplate. Return a Clojure map.
   Options accept request fields, :client and :raw? to return the Java response."
  ([project-id launch-parameter]
   (launch-flex-template project-id launch-parameter {}))
  ([project-id launch-parameter options]
   (let [request (->LaunchFlexTemplateRequest project-id launch-parameter (request-options options {:location "europe-west1"}))]
     (call-with-client :flex-templates options
                       (fn [^FlexTemplatesServiceClient client]
                         (let [response (.launchFlexTemplate client request)]
                           (if (:raw? options) response (LaunchFlexTemplateResponse->clj response))))))))

(defn get-snapshot
  "Call Dataflow getSnapshot. Options accept request fields, :client."
  ([project-id snapshot-id]
   (get-snapshot project-id snapshot-id {}))
  ([project-id snapshot-id options]
   (let [request (->GetSnapshotRequest project-id snapshot-id (request-options options {:location "europe-west1"}))]
     (call-with-client :snapshots options
                       (fn [^SnapshotsV1Beta3Client client]
                         (let [response (.getSnapshot client request)]
                           response))))))

(defn delete-snapshot
  "Call Dataflow deleteSnapshot. Options accept request fields, :client."
  ([project-id snapshot-id]
   (delete-snapshot project-id snapshot-id {}))
  ([project-id snapshot-id options]
   (let [request (->DeleteSnapshotRequest project-id snapshot-id (request-options options {:location "europe-west1"}))]
     (call-with-client :snapshots options
                       (fn [^SnapshotsV1Beta3Client client]
                         (let [response (.deleteSnapshot client request)]
                           response))))))

(defn list-snapshots
  "Call Dataflow listSnapshots. Options accept request fields, :client."
  ([project-id]
   (list-snapshots project-id {}))
  ([project-id options]
   (let [request (->ListSnapshotsRequest project-id (request-options options {:location "europe-west1"}))]
     (call-with-client :snapshots options
                       (fn [^SnapshotsV1Beta3Client client]
                         (let [response (.listSnapshots client request)]
                           (.getSnapshotsList response)))))))
