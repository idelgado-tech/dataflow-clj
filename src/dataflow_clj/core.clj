(ns dataflow-clj.core
  (:require
   [clojure.tools.logging :as log])
  (:import
   (java.time LocalDateTime
              Duration)
   (com.google.dataflow.v1beta3 Job
                                JobsV1Beta3Client
                                GetJobRequest
                                ListJobsRequest
                                ListJobsRequest$Filter)))


;;TODO Put in electre-common-time
(defn timestamp->LocalDateTime [timestamp]
  (LocalDateTime/ofInstant (java.time.Instant/ofEpochSecond (.getSeconds timestamp) (.getNanos timestamp)) (java.time.ZoneId/of "UTC")))

(defn duration-between-times-human [date-1 date2]
  (let [d (Duration/between date-1 date2)]
    (format "%sd %sh %sm %ss",
            (.toDaysPart d),
            (.toHoursPart d),
            (.toMinutesPart d),
            (.toSecondsPart d))))

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

(defn ->ListJobsRequest [^String project-id {:keys [filter-state ^String location ^String name page-size]
                                             :as   _options}]
  (.build (cond-> (-> (ListJobsRequest/newBuilder)
                      (.setProjectId project-id))
            filter-state (.setFilter (-> filter-state job-state->value ListJobsRequest$Filter/forNumber))
            location (.setLocation location)
            name (.setName name)
            page-size (.setPageSize page-size))))

(defn ->GetJobsRequest [^String project-id job-id {:keys [^String location]
                                                   :as   _options}]
  (.build (cond-> (-> (GetJobRequest/newBuilder)
                      (.setProjectId project-id)
                      (.setJobId job-id))
            location (.setLocation location))))

(defn- job->create-time [^Job job]
  (timestamp->LocalDateTime (.getCreateTime job)))

(defn- job->current-state [^Job job]
  (->
   job
   .getCurrentState
   .toString))

(defn- job->current-state-time [^Job job]
  (timestamp->LocalDateTime (.getCurrentStateTime job)))

(defn- job->name [^Job job]
  (.getName job))

(defn- job->start-time [^Job job]
  (timestamp->LocalDateTime (.getStartTime job)))

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
     :time-to-status (duration-between-times-human start-time current-status-time)}))

(defn list-jobs [project-id {:as   _options
                             :keys [name page-size filter-state location]
                             :or   {filter-state :all
                                    page-size    20
                                    location     "europe-west1"} }]
  (try
    (let [^JobsV1Beta3Client jobs-client (JobsV1Beta3Client/create)
          ^ListJobsRequest request (->ListJobsRequest project-id {:page-size page-size :filter-state filter-state :location location :name name})
          jobs (.listJobs jobs-client request)]
      (for [job (.iterateAll jobs)]
        (Job->clj job)))
    (catch Exception e 
      (log/errorf e "Exception listing jobs for [project-id=%S]" project-id))))

(defn get-job [project-id job-id]
  (try
    (let [^JobsV1Beta3Client jobs-client (JobsV1Beta3Client/create)
          ^ListJobsRequest request (->GetJobsRequest project-id job-id {:location "europe-west1"})
          job (.getJob jobs-client request)]
      (Job->clj job))
    (catch Exception e
      (log/errorf e "Exception getting job [job-id=%S] for [project-id=%S]" job-id project-id))))

(comment

  (def jobs (list-jobs "project-id" {}))



  (def jobs-items (for [item (.iterateAll jobs)]
                    (Job->clj item)))

  (take 10 jobs-items)

  )