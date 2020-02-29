#!/usr/bin/env bash
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

if [[ -z "${GOOGLE_CLOUD_PROJECT:-}" ]]; then
  echo "You must set GOOGLE_CLOUD_PROJECT to the project id hosting Cloud Run C++ Hello World"
  exit 1
fi

GOOGLE_CLOUD_PROJECT="${GOOGLE_CLOUD_PROJECT:-}"
readonly GOOGLE_CLOUD_PROJECT
GOOGLE_CLOUD_REGION="${REGION:-us-central1}"
readonly GOOGLE_CLOUD_REGION
GOOGLE_CLOUD_SPANNER_INSTANCE="${GOOGLE_CLOUD_SPANNER_INSTANCE:-gke-batch-job}"
readonly GOOGLE_CLOUD_SPANNER_INSTANCE
GOOGLE_CLOUD_SPANNER_DATABASE="${GOOGLE_CLOUD_SPANNER_DATABASE:-gke-batch-job-db}"
readonly GOOGLE_CLOUD_SPANNER_DATABASE

# Print out what we are about to do.
cat <<_EOF_
Configuring project ${GOOGLE_CLOUD_PROJECT} to run the gke-batch-job example.
This script willl enable a number of services in the project, including: Cloud
Spanner, Cloud Container Registry (GCR), Google Container Engine (GKE) and
Cloud Build. The script will also create a service account to run jobs in GKE,
a Cloud Spanner instance to hold a job queue, a GKE cluster to run batch jobs
coordinated through this queue, build Docker images with the code for GKE, and
store these images in the container registry for your project.

These actions will incur billing costs.

_EOF_
read -p "Are you sure you want to continue? (y/n) " -n 1 -r
echo
if ! [[ "${REPLY}" =~ ^[Yy]$ ]]; then
  echo "Aborting script execution at your request."
  exit 0
fi

echo "$(date -u): =============== Activating services in the project"
gcloud services enable cloudbuild.googleapis.com \
    "--project=${GOOGLE_CLOUD_PROJECT}"
gcloud services enable containerregistry.googleapis.com \
    "--project=${GOOGLE_CLOUD_PROJECT}"
gcloud services enable spanner.googleapis.com \
    "--project=${GOOGLE_CLOUD_PROJECT}"
gcloud services enable container.googleapis.com \
    "--project=${GOOGLE_CLOUD_PROJECT}"

echo "$(date -u): =============== Creating Cloud Spanner Instance ${GOOGLE_CLOUD_SPANNER_INSTANCE}"
if gcloud spanner instances describe "${GOOGLE_CLOUD_SPANNER_INSTANCE}" \
      "--project=${GOOGLE_CLOUD_PROJECT}"  >/dev/null 2>&1; then
  echo "Instance already exists, reusing"
else
  gcloud spanner instances create "${GOOGLE_CLOUD_SPANNER_INSTANCE}" \
      "--project=${GOOGLE_CLOUD_PROJECT}" \
      "--config=regional-${GOOGLE_CLOUD_REGION}" \
      --description="Scratch data for GKE batch job" \
      --nodes=1
fi

set +e
read -r -d '' DDL <<'_EOF_'
CREATE TABLE generate_object_jobs (
  job_id STRING(128),
  task_id STRING(128),
  bucket STRING(128) NOT NULL,
  object_count INT64 NOT NULL,
  use_hash_prefix BOOL NOT NULL,
  status STRING(32),
  owner STRING(32),
  updated TIMESTAMP OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (job_id, task_id)
_EOF_
readonly DDL
set -e

echo "$(date -u): =============== Creating Cloud Spanner Database ${GOOGLE_CLOUD_SPANNER_DATABASE}"
if gcloud spanner databases describe "${GOOGLE_CLOUD_SPANNER_DATABASE}" \
      "--project=${GOOGLE_CLOUD_PROJECT}" \
      "--instance=${GOOGLE_CLOUD_SPANNER_INSTANCE}" >/dev/null 2>&1; then
  echo "Database already exists, reusing"
else
  gcloud spanner databases create "${GOOGLE_CLOUD_SPANNER_DATABASE}" \
      "--project=${GOOGLE_CLOUD_PROJECT}" \
      "--instance=${GOOGLE_CLOUD_SPANNER_INSTANCE}" \
      "--ddl=${DDL}"
fi

echo "$(date -u): =============== Building Docker images"
gcloud builds submit \
    "--project=${GOOGLE_CLOUD_PROJECT}" \
    "--substitutions=SHORT_SHA=$(git rev-parse --short HEAD)" \
    "--config=cloudbuild.yaml"

# Create the GKE cluster
echo "$(date -u): =============== Creating the batch-jobs GKE cluster"
if gcloud container clusters describe batch-jobs \
      "--project=${GOOGLE_CLOUD_PROJECT}" \
      "--region=${GOOGLE_CLOUD_REGION}" >/dev/null 2>&1; then
  echo "Cluster batch-jobs already exists, reusing"
else
  gcloud container clusters create batch-jobs \
      "--project=${GOOGLE_CLOUD_PROJECT}" \
      "--region=${GOOGLE_CLOUD_REGION}" \
      "--preemptible" \
      "--min-nodes=0" \
      "--max-nodes=60" \
      "--enable-autoscaling"
fi

readonly SA_ID="gke-batch-job"
echo "$(date -u): =============== Creating ${SA_ID} service account"
readonly SA_NAME="${SA_ID}@${GOOGLE_CLOUD_PROJECT}.iam.gserviceaccount.com"
if gcloud iam service-accounts describe "${SA_NAME}" \
     "--project=${GOOGLE_CLOUD_PROJECT}" >/dev/null 2>&1; then
  echo "The ${SA_ID} service account already exists"
else
  gcloud iam service-accounts create "${SA_ID}" \
      "--project=${GOOGLE_CLOUD_PROJECT}" \
      --description="C++ Hello World for Cloud Run"
fi


echo "$(date -u): =============== Grant ${SA_ID} service account permission to operate on Cloud Spanner"
# Grant this service account permission to just update the tables
# (but not create other tables or databases)
gcloud spanner instances add-iam-policy-binding \
    "${GOOGLE_CLOUD_SPANNER_INSTANCE}" \
    "--project=${GOOGLE_CLOUD_PROJECT}" \
    "--member=serviceAccount:${SA_NAME}" \
    "--role=roles/spanner.databaseUser"

echo "$(date -u): =============== Grant ${SA_ID} service account permission to operate on GCS"
gcloud projects add-iam-policy-binding "${GOOGLE_CLOUD_PROJECT}" \
    "--member=serviceAccount:${SA_NAME}" \
    "--role=roles/storage.objectAdmin"

echo "$(date -u): =============== Install a ${SA_ID} service account key into the GKE cluster"
gcloud container clusters get-credentials batch-jobs \
      "--project=${GOOGLE_CLOUD_PROJECT}" \
      "--region=${GOOGLE_CLOUD_REGION}"

if kubectl get secret service-account-key >/dev/null 2>&1; then
  echo "The service account key for the ${SA_ID} service account already exists"
else
  KEY_FILE_DIR="/dev/shm"
  if [[ ! -d "${KEY_FILE_DIR}" ]]; then
     KEY_FILE_DIR="/private/var/tmp"
  fi
  if [[ ! -d "${KEY_FILE_DIR}" ]]; then
     KEY_FILE_DIR="${HOME}"
  fi
  readonly KEY_FILE_DIR

  # Create new keys for this service account and download then to a temporary
  # place:
  gcloud iam service-accounts keys create "${KEY_FILE_DIR}/key.json" \
      "--iam-account=${SA_NAME}"

  # Copy the key to the GKE cluster:
  kubectl create secret generic service-account-key \
      "--from-file=key.json=${KEY_FILE_DIR}/key.json"

  rm "${KEY_FILE_DIR}/key.json"
fi

exit 0
