#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Build the Dataflow Flex Template and deploy the Cloud Workflow that runs the
# MongoDB → BigQuery ETL. Optionally wires up a daily Cloud Scheduler trigger.
#
# Configure everything by copying .env.example to .env and editing it, then:
#   source .env
#   chmod +x deploy/deploy.sh
#   ./deploy/deploy.sh
#
# Re-run with SKIP_BUILD=1 to redeploy the workflow without rebuilding the image:
#   SKIP_BUILD=1 ./deploy/deploy.sh
#
# Prerequisites (one-time):
#   * APIs enabled: dataflow, cloudbuild, artifactregistry, workflows,
#     cloudscheduler, secretmanager, pubsub, monitoring.
#   * An Artifact Registry Docker repo (set AR_REPO / AR_REGION below).
#   * The BigQuery dataset ($BQ_DATASET) already exists.
#   * The runtime service account ($RUNTIME_SA) has, at minimum:
#       roles/dataflow.worker, roles/bigquery.jobUser,
#       roles/bigquery.dataEditor (on the dataset),
#       roles/storage.objectAdmin (on the bucket),
#       roles/pubsub.publisher (on the DLQ topic, if used),
#       roles/secretmanager.secretAccessor (if mongo_uri is a secret),
#       roles/monitoring.metricWriter.
#   * The deploying identity has roles/iam.serviceAccountUser on $RUNTIME_SA.
# ---------------------------------------------------------------------------
set -euo pipefail

# --- Required config (from .env / environment) -----------------------------
: "${GOOGLE_CLOUD_PROJECT:?set GOOGLE_CLOUD_PROJECT}"
: "${REGION:?set REGION}"
: "${MONGO_URI:?set MONGO_URI}"
: "${MONGO_DB:?set MONGO_DB}"
: "${COLLECTIONS:?set COLLECTIONS}"
: "${BQ_DATASET:?set BQ_DATASET}"
: "${GCS_BUCKET:?set GCS_BUCKET}"
: "${RUNTIME_SA:?set RUNTIME_SA}"

PROJECT_ID="${GOOGLE_CLOUD_PROJECT}"
DLQ_TOPIC="${DLQ_TOPIC:-}"

# --- Tunable config (override via environment if you like) ------------------
AR_REGION="${AR_REGION:-europe}"
AR_REPO="${AR_REPO:-etl}"
IMAGE_NAME="${IMAGE_NAME:-mongodb-bigquery-etl}"
WORKFLOW_NAME="${WORKFLOW_NAME:-mongodb-bigquery-etl}"
SCHEDULER_JOB="${SCHEDULER_JOB:-mongodb-bigquery-etl-daily}"
SCHEDULE="${SCHEDULE:-0 4 * * *}"          # daily 04:00 by default
DEPLOY_SCHEDULER="${DEPLOY_SCHEDULER:-1}"  # set to 0 to skip Cloud Scheduler

IMAGE_URI="${AR_REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/${IMAGE_NAME}:latest"
TEMPLATE_GCS_PATH="gs://${GCS_BUCKET}/templates/${IMAGE_NAME}.json"

# ---------------------------------------------------------------------------
# 1. Build the container image (skip with SKIP_BUILD=1).
# ---------------------------------------------------------------------------
if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
  echo "==> Skipping Cloud Build (SKIP_BUILD=1)."
else
  echo "==> Building container image with Cloud Build…"
  gcloud builds submit \
    --project="${PROJECT_ID}" \
    --tag="${IMAGE_URI}" \
    .
fi

# ---------------------------------------------------------------------------
# 2. Build the Dataflow Flex Template spec and push it to GCS.
# ---------------------------------------------------------------------------
echo "==> Building Flex Template spec → ${TEMPLATE_GCS_PATH}…"
gcloud dataflow flex-template build "${TEMPLATE_GCS_PATH}" \
  --project="${PROJECT_ID}" \
  --image="${IMAGE_URI}" \
  --sdk-language=PYTHON \
  --metadata-file=deploy/flex-template-metadata.json

# ---------------------------------------------------------------------------
# 3. Deploy the Cloud Workflow (substituting config into the YAML).
# ---------------------------------------------------------------------------
echo "==> Deploying Cloud Workflow '${WORKFLOW_NAME}'…"
WORKFLOW_YAML=$(sed \
  -e "s|__PROJECT__|${PROJECT_ID}|g" \
  -e "s|__REGION__|${REGION}|g" \
  -e "s|__TEMPLATE_PATH__|${TEMPLATE_GCS_PATH}|g" \
  -e "s|__RUNTIME_SA__|${RUNTIME_SA}|g" \
  -e "s|__DLQ_TOPIC__|${DLQ_TOPIC}|g" \
  -e "s|__GCS_BUCKET__|${GCS_BUCKET}|g" \
  -e "s|__MONGO_URI__|${MONGO_URI}|g" \
  -e "s|__MONGO_DB__|${MONGO_DB}|g" \
  -e "s|__COLLECTIONS__|${COLLECTIONS}|g" \
  -e "s|__BQ_DATASET__|${BQ_DATASET}|g" \
  deploy/workflow.yaml)

WORKFLOW_TMP=$(mktemp /tmp/workflow-XXXXXX.yaml 2>/dev/null || mktemp)
echo "${WORKFLOW_YAML}" > "${WORKFLOW_TMP}"
gcloud workflows deploy "${WORKFLOW_NAME}" \
  --project="${PROJECT_ID}" \
  --location="${REGION}" \
  --service-account="${RUNTIME_SA}" \
  --source="${WORKFLOW_TMP}"
rm -f "${WORKFLOW_TMP}"

# ---------------------------------------------------------------------------
# 4. Cloud Scheduler → Cloud Workflow (optional).
# ---------------------------------------------------------------------------
if [[ "${DEPLOY_SCHEDULER}" == "1" ]]; then
  echo "==> Creating/updating Cloud Scheduler job '${SCHEDULER_JOB}' (${SCHEDULE})…"
  WORKFLOW_EXECUTIONS_URI="https://workflowexecutions.googleapis.com/v1/projects/${PROJECT_ID}/locations/${REGION}/workflows/${WORKFLOW_NAME}/executions"
  if gcloud scheduler jobs describe "${SCHEDULER_JOB}" \
        --project="${PROJECT_ID}" --location="${REGION}" &>/dev/null; then
    SCHED_CMD="update"
  else
    SCHED_CMD="create"
  fi
  gcloud scheduler jobs "${SCHED_CMD}" http "${SCHEDULER_JOB}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --schedule="${SCHEDULE}" \
    --time-zone="UTC" \
    --uri="${WORKFLOW_EXECUTIONS_URI}" \
    --http-method=POST \
    --message-body='{}' \
    --oauth-service-account-email="${RUNTIME_SA}" \
    --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform"
else
  echo "==> Skipping Cloud Scheduler (DEPLOY_SCHEDULER=0)."
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "==> Done."
echo "    Image     : ${IMAGE_URI}"
echo "    Template  : ${TEMPLATE_GCS_PATH}"
echo "    Workflow  : ${WORKFLOW_NAME} (${REGION})"
[[ "${DEPLOY_SCHEDULER}" == "1" ]] && echo "    Scheduler : ${SCHEDULER_JOB} — '${SCHEDULE}' UTC"
echo ""
echo "Trigger a manual run:"
echo "  gcloud workflows run ${WORKFLOW_NAME} --location=${REGION} --project=${PROJECT_ID}"
