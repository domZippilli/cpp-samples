#!/usr/bin/env bash
#
# Copyright 2019 Google LLC
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

if [ -z "${NCPU:-}" ]; then
  readonly NCPU=$(nproc)
fi

if [ -z "${PROJECT_ID:-}" ]; then
  readonly PROJECT_ID="cloud-devrel-kokoro-resources"
fi

if [[ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
  readonly GOOGLE_APPLICATION_CREDENTIALS="${KOKORO_KEYSTORE_DIR}/71386_cpp-docs-samples-service-account"
fi

readonly SUBDIR_ROOT="$(cd "$(dirname "$0")/../../"; pwd)"

echo "================================================================"
echo "Change working directory to ${SUBDIR_ROOT} $(date)."
cd "${SUBDIR_ROOT}"

echo "================================================================"
echo "Setup Google Container Registry access $(date)."
gcloud auth configure-docker

readonly DEV_IMAGE="gcr.io/${PROJECT_ID}/cpp/cpp-samples/speech-devtools"
readonly IMAGE="gcr.io/${PROJECT_ID}/cpp/cpp-samples/speech-sample"

has_cache="false"
# Download the cache for local run and pull request.
if [[ -z "${KOKORO_JOB_NAME:-}" ]] ||
   [[ -n "${KOKORO_GITHUB_PULL_REQUEST_NUMBER:-}" ]]; then
  echo "================================================================"
  echo "Download existing image (if available) $(date)."
  if docker pull "${DEV_IMAGE}:latest"; then
    echo "Existing image successfully downloaded."
    has_cache="true"
  fi
fi

echo "================================================================"
echo "Build base image with minimal development tools $(date)."
update_cache="false"

devtools_flags=(
  # Build only for dependencies
  "--target" "devtools"
  "--build-arg" "NCPU=${NCPU}"
  "-t" "${DEV_IMAGE}:latest"
  "-f" "Dockerfile"
)

if "${has_cache}"; then
  devtools_flags+=("--cache-from=${DEV_IMAGE}:latest")
fi

if [[ -n "${KOKORO_JOB_NAME:-}" ]] && \
   [[ -z "${KOKORO_GITHUB_PULL_REQUEST_NUMBER:-}" ]]; then
  devtools_flags+=("--no-cache")
fi

echo "================================================================"
echo "Building docker image with the following flags $(date)."
printf '%s\n' "${devtools_flags[@]}"

readonly DOCKER_LOG_FILE=$(mktemp)

# Temporary allow failure
set +e

if docker build "${devtools_flags[@]}" . > "${DOCKER_LOG_FILE}" 2>&1; then
  update_cache="true"
else
  cat "${DOCKER_LOG_FILE}"
  rm "${DOCKER_LOG_FILE}"
  exit 1
fi

set -e

if [[ "${PRESERVE_LOGS:-}" != "yes" ]]; then
  rm "${DOCKER_LOG_FILE}"
fi

if "${update_cache}" && [[ -z "${KOKORO_GITHUB_PULL_REQUEST_NUMBER:-}" ]]; then
  echo "================================================================"
  echo "Uploading updated base image $(date)."
  # Do not stop the build on a failure to update the cache.
  docker push "${DEV_IMAGE}:latest" || true
fi

build_flags=(
  "--build-arg" "NCPU=${NCPU}"
  "-t" "${IMAGE}:latest"
  "--cache-from=${DEV_IMAGE}:latest"
  "--target=cpp-speech"
  "-f" "Dockerfile"
)

echo "================================================================"
echo "Building docker image with the following flags $(date)."
printf '%s\n' "${build_flags[@]}"

docker build "${build_flags[@]}" .

if [[ "${CHECK_STYLE:-}" ]]; then
  docker run "${IMAGE}:latest" bash -c "cd /home/speech/api/; make tidy"
fi
  
if [[ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
  docker run -v "${GOOGLE_APPLICATION_CREDENTIALS}:/home/service-account.json" \
    "${IMAGE}:latest" bash -c "cd /home/speech/api/; make run_tests"
fi
