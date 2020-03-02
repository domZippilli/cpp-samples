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

# We chose Alpine to build the image because it has good support for creating
# statically-linked, small programs.
ARG DISTRO_VERSION=edge
FROM alpine:${DISTRO_VERSION} AS base

# Create separate targets for each phase, this allows us to cache intermediate
# stages when using Google Cloud Build, and makes the final deployment stage
# small as it contains only what is needed.
FROM base AS devtools

# Install the typical development tools and some additions:
#   - ninja-build is a backend for CMake that often compiles faster than
#     CMake with GNU Make.
#   - Install the boost libraries.
RUN apk update && \
    apk add \
        boost-dev \
        boost-static \
        build-base \
        c-ares-static \
        cmake \
        curl \
        curl-dev \
        curl-static \
        git \
        gcc \
        g++ \
        grpc \
        grpc-dev \
        grpc-cli \
        libc-dev \
        nghttp2-static \
        ninja \
        protobuf \
        protobuf-dev \
        openssl-dev \
        openssl-libs-static \
        tar \
        zlib-static

WORKDIR /var/tmp/build
RUN curl -s -L -o 1.1.0.tar.gz https://github.com/google/crc32c/archive/1.1.0.tar.gz
RUN tar -zxf 1.1.0.tar.gz
WORKDIR /var/tmp/build/crc32c-1.1.0
RUN cmake -S. -B.build -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCRC32C_BUILD_TESTS=OFF \
    -DCRC32C_BUILD_BENCHMARKS=OFF \
    -DCRC32C_USE_GLOG=OFF
RUN cmake --build .build --target install

WORKDIR /var/tmp/build
RUN curl -s -L -o v0.5.0.tar.gz https://github.com/googleapis/cpp-cmakefiles/archive/v0.5.0.tar.gz
RUN tar -zxf v0.5.0.tar.gz
WORKDIR /var/tmp/build/cpp-cmakefiles-0.5.0
RUN cmake -S. -B.build -GNinja \
    -DCMAKE_BUILD_TYPE=Release
RUN cmake --build .build --target install

WORKDIR /var/tmp/build
RUN curl -s -L -o v0.21.0.tar.gz \
    https://github.com/googleapis/google-cloud-cpp-common/archive/v0.21.0.tar.gz
RUN tar -zxf v0.21.0.tar.gz
WORKDIR /var/tmp/build/google-cloud-cpp-common-0.21.0
RUN cmake -S. -B.build -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTING=OFF
RUN cmake --build .build --target install

WORKDIR /var/tmp/build
RUN curl -s -L -o v0.20.0.tar.gz \
    https://github.com/googleapis/google-cloud-cpp/archive/v0.20.0.tar.gz
RUN tar -zxf v0.20.0.tar.gz
WORKDIR /var/tmp/build/google-cloud-cpp-0.20.0
RUN cmake -S. -B.build -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTING=OFF
RUN cmake --build .build --target install

WORKDIR /var/tmp/build
RUN curl -s -L -o v0.9.0.tar.gz \
    https://github.com/googleapis/google-cloud-cpp-spanner/archive/v0.9.0.tar.gz
RUN tar -zxf v0.9.0.tar.gz
WORKDIR /var/tmp/build/google-cloud-cpp-spanner-0.9.0
RUN cmake -S. -B.build -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTING=OFF
RUN cmake --build .build --target install

# Copy the source code to /v/source and compile it.
FROM devtools AS build
COPY . /v/source
WORKDIR /v/source

# Run the CMake configuration step, setting the options to create
# a statically linked C++ program
RUN cmake -S/v/source -B/v/binary -GNinja \
    -DCMAKE_BUILD_TYPE=Release \
    -DBoost_USE_STATIC_LIBS=ON \
    -D_gRPC_USE_STATIC_LIBS=ON \
    -DProtobuf_USE_STATIC_LIBS=ON \
    -DOPENSSL_USE_STATIC_LIBS=ON \
    -DOPENSSL_CRYPTO_LIBRARY=/usr/lib/libcrypto.a \
    -DOPENSSL_SSL_LIBRARY=/usr/lib/libssl.a \
    -DZLIB_LIBRARY=/lib/libz.a \
    -DEXTRA_LIBRARY="/usr/lib/libnghttp2.a;/usr/lib/libcares.a" \
    -DCMAKE_EXE_LINKER_FLAGS="-static"

# Compile the binary and strip it to reduce its size.
RUN cmake --build /v/binary
RUN strip /v/binary/gke_batch_job

# Create the final deployment image, we need the CA certificates for the
# application to verify the Google SSL certificates.
FROM base AS gke-batch-job
RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*
WORKDIR /r

# Copy the program from the previously created stage and make it the entry point.
COPY --from=build /v/binary/gke_batch_job /r

CMD "/bin/false"
