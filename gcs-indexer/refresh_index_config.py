#!/usr/bin/env python3
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

"""Creates a k8s config to parallelize refresh_index_for_bucket."""

import argparse
import jinja2 as j2
import os
import random
import time

template = j2.Template("""
apiVersion: batch/v1
kind: Job
metadata:
  name: refresh-index-{{timestamp}}-{{random}}
spec:
  completions: {{parallelism}}
  parallelism: {{parallelism}}
  template:
    metadata:
      name: refresh-bucket-worker
    spec:
      restartPolicy: OnFailure
      volumes:
        - name: service-account-key
          secret:
            secretName: service-account-key
      containers:
        - name: gcs-indexer-tools
          image: gcr.io/{{project_id}}/gcs-indexer-tools:latest
          imagePullPolicy: Always
          command: [
              '/r/refresh_index_worker',
              '--project={{project_id}}',
              '--instance={{instance}}',
              '--database={{database}}',
              '--worker-threads={{worker_threads}}',
              '--job-id={{job_id}}'
          ]
          volumeMounts:
            - name: service-account-key
              mountPath: /var/secrets/service-account-key
          env:
            - name: GOOGLE_APPLICATION_CREDENTIALS
              value: /var/secrets/service-account-key/key.json
            - name: GCS_INDEXER_TASK_ID
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
""")


def check_positive(value):
    as_int = int(value)
    if as_int <= 0:
        raise argparse.ArgumentTypeError(
            '%s is not a valid positive integer value' % value)
    return as_int


parser = argparse.ArgumentParser()
parser.add_argument('--project',
                    default=os.environ.get('GOOGLE_CLOUD_PROJECT'),
                    type=str, required=True,
                    help='configure the Google Cloud Project')
parser.add_argument('--instance', type=str, required=True,
                    help='configure the Cloud Spanner instance id')
parser.add_argument('--database', type=str, required=True,
                    help='configure the Cloud Spanner database id')
parser.add_argument('--job-id', type=str, required=True,
                    help='the indexing job id')
parser.add_argument('--parallelism', default=30, type=check_positive,
                    help='the maximum number of parallel tasks')
args = parser.parse_args()

worker_threads = 2
print(template.render(project_id=args.project, instance=args.instance, database=args.database,
                      job_id=args.job_id, worker_threads=worker_threads, parallelism=args.parallelism,
                      timestamp=int(time.time()), random=random.randint(0, 100000)))
