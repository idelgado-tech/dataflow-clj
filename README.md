# dataflow-clj

Clojure wrapper around the [Google Cloud Dataflow Java API](https://github.com/googleapis/google-cloud-java/tree/main/java-dataflow).
Covers the 18 service operations of the six clients in `google-cloud-dataflow` 0.101.0.

## Usage

Authenticate using Google Application Default Credentials, for example with
`gcloud auth application-default login` during local development. Enable the
Dataflow API and grant the appropriate permissions to the calling identity.

```clojure
(require '[dataflow-clj.core :as dataflow])
(import '(com.google.dataflow.v1beta3 LaunchTemplateParameters
                                    LaunchFlexTemplateParameter
                                    RuntimeEnvironment))

(dataflow/list-jobs "project-id" {:filter-state :active :location "europe-west1"})
(dataflow/get-job "project-id" "job-id")
(dataflow/get-job "project-id" "job-id" {:location "us-central1" :view :job-view-all})

(dataflow/launch-template
 "project-id" "gs://bucket/template"
 (.build (-> (LaunchTemplateParameters/newBuilder)
             (.setJobName "my-pipeline")
             (.putAllParameters {"input" "gs://bucket/input"})
             (.setEnvironment (.build (-> (RuntimeEnvironment/newBuilder)
                                         (.setTempLocation "gs://bucket/temp")
                                         (.setNumWorkers 2))))))
 {:location "europe-west1" :validate-only true})

(dataflow/launch-flex-template
 "project-id"
 (.build (-> (LaunchFlexTemplateParameter/newBuilder)
             (.setJobName "my-flex-pipeline")
             (.setContainerSpecGcsPath "gs://bucket/spec.json")
             (.putAllParameters {"input" "gs://bucket/input"})))
 {:location "europe-west1"})

(dataflow/list-job-messages "project-id" "job-id"
                            {:minimum-importance :job-message-warning})
(dataflow/get-job-metrics "project-id" "job-id")
(dataflow/snapshot-job "project-id" "job-id"
                       {:ttl (.build (.setSeconds (com.google.protobuf.Duration/newBuilder) 3600)) :description "backup"})
(dataflow/drain-job "project-id" "job-id")
(dataflow/cancel-job "project-id" "job-id")
```

## Operations

Every operation takes `project-id` first and an optional options map last.
The arguments below appear between them.

| Function | Additional arguments |
| --- | --- |
| `list-jobs` | — |
| `aggregated-list-jobs` | — |
| `get-job` | `job-id` |
| `create-job` | `job` |
| `update-job` | `job-id job` |
| `cancel-job`, `drain-job` | `job-id` |
| `check-active-jobs` | — |
| `snapshot-job` | `job-id` |
| `list-job-messages` | `job-id` |
| `get-job-metrics` | `job-id` |
| `get-job-execution-details` | `job-id` |
| `get-stage-execution-details` | `job-id stage-id` |
| `create-job-from-template` | `gcs-path job-name` |
| `launch-template` | `gcs-path launch-parameters` |
| `get-template` | `gcs-path` |
| `launch-flex-template` | `launch-parameter` |
| `get-snapshot`, `delete-snapshot` | `snapshot-id` |
| `list-snapshots` | — |
