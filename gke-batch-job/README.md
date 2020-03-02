# Cloud Run for C++

This directory contains an example showing how to run C++ batch jobs using GKE and Cloud Spanner.

The recommended practice to shard batch jobs in a Kubernetes cluster is to use a [work queues][k8s-work-queue-link] to
coordinate which pod execute what shard. In this example we use Cloud Spanner to maintain the work queue, and the output
of the job is a number of randomly named objects in Google Cloud Storage.

[k8s-work-queue-link]: https://kubernetes.io/docs/tasks/job/fine-parallel-processing-work-queue/

## Prerequisites

Install the command-line tools, for example:

```bash
gcloud components install docker-credential-gcr
gcloud auth configure-docker
```

## Bootstrap the Example

Pick the project to run the job, then run the bootstrap script to create all the necessary resources, including a
Google Kubernets Engine (GKE) cluster, a Cloud Spanner instance and database, and a service account to run the job.

```bash
export GOOGLE_CLOUD_PROJECT=...
./bootstrap.sh
```

## Pick a bucket to populate with randomly named objects


```bash
BUCKET_NAME=...
gsutil -p ${GOOGLE_CLOUD_PROJECT} mb gs://${BUCKET_NAME}
```

## Run a batch job to populate the bucket

```bash
./run_job.py --project=${GOOGLE_CLOUD_PROJECT} --bucket=${BUCKET_NAME} --object-count=1000000
```
