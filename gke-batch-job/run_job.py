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

"""Create k8s jobs to populate GCS buckets with randomly named objects."""

import argparse
import jinja2 as j2
import os
import random
import subprocess
import time

template = j2.Template("""
apiVersion: batch/v1
kind: Job
metadata:
  name: {{action}}-{{tag}}
spec:
  parallelism: {{parallelism}}
  completions: {{completions}}
  backoffLimit: 100
  template:
    metadata:
      name: generate-randomly-named-objects-{{action}}
    spec:
      restartPolicy: OnFailure
      volumes:
        - name: service-account-key
          secret:
            secretName: service-account-key
      containers:
        - name: gke-batch-job
          image: gcr.io/{{project}}/gke-batch-job:latest
          imagePullPolicy: Always
          command: [
              '/r/gke_batch_job', '{{action}}',
              '--project={{project}}',
              '--instance={{instance}}',
              '--database={{database}}',
              '--job-id={{job_id}}',
              '--bucket={{bucket}}',
              '--object-count={{object_count}}',
              '--task-size={{task_size}}',
              '--task-timeout={{task_timeout}}'
          ]
          resources:
            requests:
              cpu: '10m'
              memory: '8Mi'
          volumeMounts:
            - name: service-account-key
              mountPath: /var/secrets/service-account-key
          env:
            - name: GOOGLE_APPLICATION_CREDENTIALS
              value: /var/secrets/service-account-key/key.json
""")

parser = argparse.ArgumentParser()
parser.add_argument('--project', type=str,
                    default=os.environ.get('GOOGLE_CLOUD_PROJECT'),
                    help='configure the Google Cloud Project')
parser.add_argument('--instance', type=str, default='gke-batch-job',
                    help='configure the Cloud Spanner instance id')
parser.add_argument('--database', type=str, default='gke-batch-job-db',
                    help='configure the Cloud Spanner database id')
parser.add_argument('--parallelism', type=int, default=25,
                    help='pull work from this job in the work queue')
parser.add_argument('--bucket', type=str, required=True,
                    help='pull work from this job in the work queue')
parser.add_argument('--object-count', type=int, required=True,
                    help='pull work from this job in the work queue')
parser.add_argument('--task-size', type=int, default=1000,
                    help='set the size of each work item')
parser.add_argument('--task-timeout', type=int, default=20,
                    help='set the work item timeout in minutes')
args = parser.parse_args()

tag = '%d-%08x' % (int(time.time()), random.randint(0, 1 << 32))
job_id = 'job-id-%s' % tag

with subprocess.Popen(['kubectl', 'apply', '-f', '-'], stdin=subprocess.PIPE,
                      text=True) as schedule:
    schedule.communicate(
        template.render(action='schedule-job', parallelism=1, completions=1,
                        tag=tag, project=args.project, instance=args.instance,
                        database=args.database, bucket=args.bucket,
                        object_count=args.object_count,
                        task_size=args.task_size,
                        task_timeout=args.task_timeout,
                        job_id=job_id))
    schedule.wait()

k8s_job_name = 'job.batch/schedule-job-%s' % tag
print("Waiting for job %s" % k8s_job_name)
subprocess.run(['kubectl', 'wait', '--for=condition=complete', k8s_job_name])

with subprocess.Popen(['kubectl', 'apply', '-f', '-'], stdin=subprocess.PIPE,
                      text=True) as run:
    run.communicate(
        template.render(action='worker', parallelism=args.parallelism,
                        completions=args.parallelism, tag=tag,
                        project=args.project, instance=args.instance,
                        database=args.database, bucket=args.bucket,
                        object_count=args.object_count,
                        task_size=args.task_size,
                        task_timeout=args.task_timeout,
                        job_id=job_id))
    run.wait()

k8s_job_name = 'job.batch/worker-%s' % tag
message = j2.Template("""
GKE job {{k8s_job_name}} is running in the background.  You can monitor this job using:

kubectl get {{k8s_job_name}}

Or wait for its completion using:

kubectl wait --for=condition=complete {{k8s_job_name}}
""")
print(message.render(k8s_job_name=k8s_job_name))
