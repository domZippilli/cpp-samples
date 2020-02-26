// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "google/cloud/spanner/client.h"
#include "google/cloud/storage/object_metadata.h"
#include <functional>
#include <future>
#include <vector>

namespace gcs_indexer {

void wait_for_tasks(std::vector<std::future<void>> tasks,
                    std::size_t base_task_count,
                    std::function<void(std::size_t)> const& report_progress);

void insert_object_list(
    google::cloud::spanner::Client spanner_client,
    std::vector<google::cloud::storage::ObjectMetadata> const& objects,
    google::cloud::spanner::Timestamp start, bool discard_output,
    std::atomic<std::uint64_t>& total_insert_count);

struct work_item {
  std::string bucket;
  std::string prefix;
};

work_item make_work_item(std::string const& p);

}  // namespace gcs_indexer
