# MongoDB → BigQuery ETL on Dataflow

A production-style, reusable **ETL template** that syncs MongoDB collections
into BigQuery using **Apache Beam** on **Google Cloud Dataflow**.
It comes from the frustation of OOM errors from GitHub Actions since it can not process millions of documents.


It is built as a **Dataflow Flex Template** and runs on a daily schedule
(**Cloud Scheduler → Cloud Workflows → Dataflow**), with **dead-letter
handling**, **Cloud Monitoring metrics**, and a **row-count validation** step.

> This repo is a clean, generic reference implementation. Fork it, drop in
> your own config, and you have a serverless MongoDB → BigQuery sync.
---

## Why Dataflow?

A common first attempt is a cron job or a CI runner (e.g. GitHub Actions) that
loops over documents and inserts them into BigQuery. That works until the data
grows — then you hit memory limits, long runtimes, and zero observability.

Moving to Dataflow gives you:

| Concern        | Cron / CI runner                | Dataflow (this repo)                          |
| -------------- | ------------------------------- | --------------------------------------------- |
| **Scale**      | Single process, OOM on big data | Auto-scales workers, processes in parallel    |
| **Cost**       | Pay for idle runner time        | Pay only while workers run (a few minutes)    |
| **Reliability**| Whole job fails on one bad doc  | Bad docs → dead-letter queue, job keeps going |
| **Secrets**    | Stored in CI, travel to runner  | Pulled from Secret Manager inside GCP         |
| **Observability** | Job logs only               | Native metrics, logs, and alert policies      |

---

## Architecture

```
Cloud Scheduler  ──(daily)──►  Cloud Workflow  ──►  Dataflow Flex Template
                                                          │
                                   ┌──────────────────────┼───────────────────────┐
                                   ▼                       ▼                       ▼
                            Read collection          Read collection          Read collection
                            (MongoDB secondary)      (MongoDB secondary)      (MongoDB secondary)
                                   │                       │                       │
                                   ▼                       ▼                       ▼
                            Normalise BSON ─► JSON   Normalise BSON ─► JSON   Normalise BSON ─► JSON
                                   │   └── failures ──────────────┴──► Pub/Sub Dead Letter Queue
                                   ▼
                            WRITE_TRUNCATE ─► BigQuery table per collection
                                   │
                                   ▼
                            Emit job_success metric ─► Cloud Monitoring ─► Alert policy
```

**Flow:** Cloud Scheduler triggers a Cloud Workflow daily. The workflow
launches the Dataflow Flex Template, which reads every configured collection in
parallel, converts each document's BSON types into a BigQuery-native JSON
column, and writes each collection to its own table with a full
truncate-and-reload. Documents that fail conversion go to a Pub/Sub
dead-letter topic instead of failing the job. A success/failure metric is
emitted to Cloud Monitoring at the end.

---

## Project layout

```
mongodb-bigquery-dataflow-etl/
├── Dockerfile                     # Flex Template image (Beam SDK + launcher)
├── .env.example                   # Copy to .env and fill in your values
├── pipeline/
│   ├── main.py                    # Beam pipeline (read → transform → write)
│   ├── transformations.py         # BSON → BigQuery-JSON normalisation
│   ├── metrics.py                 # Cloud Monitoring custom metric
│   ├── validate.py                # Row-count check: MongoDB vs BigQuery
│   ├── setup.py                   # Ships local modules to Dataflow workers
│   └── requirements.txt           # Python dependencies (pinned)
└── deploy/
    ├── deploy.sh                  # Build image + template + workflow + scheduler
    ├── workflow.yaml              # Cloud Workflows definition
    └── flex-template-metadata.json# Flex Template parameter spec
```

---

## Data model

Every collection is stored with the same simple schema, so you don't have to
maintain a column-per-field schema for each collection:

| Column      | Type     | Description                              |
| ----------- | -------- | ---------------------------------------- |
| `id`        | `STRING` | The Mongo `_id` as a string (primary key)|
| `json_data` | `JSON`   | The full document as native BigQuery JSON|

Because `json_data` is a real BigQuery `JSON` type, you can query nested fields
directly:

```sql
SELECT
  id,
  JSON_VALUE(json_data.email)          AS email,
  JSON_VALUE(json_data.address.city)   AS city
FROM `my-gcp-project.mongo_data.users`
WHERE JSON_VALUE(json_data.status) = 'active';
```
Also you can create views of the parameters in the doc that you want to see in your BigQuery Table.

### BSON type handling

`transformations.py` converts BSON types into explicit, recoverable JSON:

| BSON type    | Stored as                              |
| ------------ | -------------------------------------- |
| `ObjectId`   | `{"oid": "<hex>"}`                     |
| `datetime`   | `{"date": "<ISO8601Z>"}`               |
| `Decimal128` | string                                 |
| `Binary` / `bytes` | base64 string                    |
| `Regex`      | `{"regex": "<pattern>", "options": "<flags>"}` |
| `DBRef`      | `{"ref": "<collection>", "id": ...}`   |
| `NaN` / `Inf`| `null` (not representable in JSON)      |

---

## Prerequisites

- A GCP project with these APIs enabled: `dataflow`, `cloudbuild`,
  `artifactregistry`, `workflows`, `cloudscheduler`, `secretmanager`,
  `pubsub`, `monitoring`.
- An Artifact Registry Docker repository.
- A BigQuery dataset (created up front).
- A GCS bucket for Dataflow staging/temp and the template spec.
- A runtime service account for the workers (see IAM below).
- `gcloud` and `bq` installed and authenticated.

### Runtime service account IAM

Grant the worker service account (`RUNTIME_SA`):

- `roles/dataflow.worker`
- `roles/bigquery.jobUser`
- `roles/bigquery.dataEditor` (on the target dataset)
- `roles/storage.objectAdmin` (on the staging bucket)
- `roles/pubsub.publisher` (on the DLQ topic, if used)
- `roles/secretmanager.secretAccessor` (if `mongo_uri` is a Secret Manager path)
- `roles/monitoring.metricWriter`

The identity running `deploy.sh` also needs
`roles/iam.serviceAccountUser` on `RUNTIME_SA`.

---

## Quick start

### 1. Configure

```bash
cp .env.example .env
# edit .env with your project, bucket, dataset, Mongo URI, collections, etc.
source .env
```

> **Tip:** Don't put a raw Mongo URI in `.env` for production. Store it in
> Secret Manager and set `MONGO_URI` to
> `projects/PROJECT/secrets/NAME/versions/latest` — the pipeline resolves it
> automatically.

### 2. (Optional) Create the DLQ topic and BigQuery dataset

```bash
gcloud pubsub topics create mongo-etl-dlq --project="$GOOGLE_CLOUD_PROJECT"
bq --location="$REGION" mk --dataset "$GOOGLE_CLOUD_PROJECT:$BQ_DATASET"
```

### 3. Deploy

```bash
chmod +x deploy/deploy.sh
./deploy/deploy.sh
```

This builds the image, publishes the Flex Template, deploys the Cloud Workflow,
and (unless `DEPLOY_SCHEDULER=0`) creates the daily Cloud Scheduler trigger.

### 4. Run it

```bash
# Trigger the whole chain via the workflow:
gcloud workflows run mongodb-bigquery-etl \
  --location="$REGION" --project="$GOOGLE_CLOUD_PROJECT"
```

Or launch Dataflow directly (bypassing the workflow), e.g. for one collection
into test tables:

```bash
gcloud dataflow flex-template run "etl-$(date +%s)" \
  --project="$GOOGLE_CLOUD_PROJECT" \
  --region="$REGION" \
  --template-file-gcs-location="gs://$GCS_BUCKET/templates/mongodb-bigquery-etl.json" \
  --service-account-email="$RUNTIME_SA" \
  --staging-location="gs://$GCS_BUCKET/staging" \
  --parameters mongo_uri="$MONGO_URI" \
  --parameters mongo_db="$MONGO_DB" \
  --parameters collections="users" \
  --parameters bq_project="$GOOGLE_CLOUD_PROJECT" \
  --parameters bq_dataset="$BQ_DATASET" \
  --parameters table_suffix="_test"
```

### 5. Validate

```bash
python pipeline/validate.py \
  --mongo_uri="$MONGO_URI" \
  --mongo_db="$MONGO_DB" \
  --bq_project="$GOOGLE_CLOUD_PROJECT" \
  --bq_dataset="$BQ_DATASET" \
  --collections="$COLLECTIONS"
```

---

## Pipeline parameters

| Parameter       | Required | Description                                            |
| --------------- | -------- | ------------------------------------------------------ |
| `mongo_uri`     | ✅       | Mongo connection string **or** Secret Manager path.    |
| `mongo_db`      | ✅       | MongoDB database name.                                 |
| `collections`   | ✅       | Comma-separated collection names.                      |
| `bq_project`    | ✅       | GCP project that owns the BigQuery dataset.            |
| `bq_dataset`    | ✅       | BigQuery dataset to write into.                        |
| `dlq_topic`     | ➖       | Pub/Sub topic for failed docs. Empty = log only.       |
| `table_suffix`  | ➖       | Suffix added to every table name (e.g. `_test`).       |
| `no_secondary_read` | ➖   | Flag: read from the primary instead of a secondary.    |

---

## Run it locally (DirectRunner)

You can run the pipeline on your machine against a dev MongoDB before deploying:

```bash
cd pipeline
pip install -r requirements.txt
python main.py \
  --mongo_uri="mongodb://localhost:27017" \
  --mongo_db="mydb" \
  --collections="users,orders" \
  --bq_project="my-gcp-project" \
  --bq_dataset="mongo_data"
```

(Writing to BigQuery still requires GCP credentials with access to the dataset.)

---

## Monitoring & alerting

The pipeline emits one custom metric:

```
custom.googleapis.com/mongo_bq_etl/job_success   (1 = success, 0 = failure)
```

Create an alert policy that fires when this metric is `0` (or absent) to be
notified of failed runs. You can add similar policies on Dataflow worker logs,
workflow execution failures, and Pub/Sub publishes to the DLQ topic.

---

## Design notes & trade-offs

- **Truncate-and-reload, not upsert.** Each run replaces the table with a fresh
  full copy. This keeps the code simple, guarantees deletes propagate, and
  needs no `updatedAt` field. For very large, mostly-unchanged collections you
  might prefer an incremental load (staging table + `MERGE`, or Change
  Streams) — that's a different trade-off in complexity.
- **Read from a secondary** to avoid loading the primary node. For Atlas
  clusters with a dedicated analytics node, tag the read preference in
  `main.py`.
- **Dead-letter queue** keeps one malformed document from failing the entire
  job, while still capturing it for investigation.
- **Pinned dependencies** and a matching Beam SDK / Dockerfile version avoid
  the classic "works locally, breaks on Dataflow" version-skew issues.

---

## License

MIT — use it freely for your own projects.
