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

#include "indexer_utils.h"
#include "gcs_indexer_constants.h"

namespace gcs_indexer {

void wait_for_tasks(std::vector<std::future<void>> tasks,
                    std::size_t base_task_count,
                    std::function<void(std::size_t)> const& report_progress) {
  while (not tasks.empty()) {
    using namespace std::chrono_literals;
    using std::chrono::duration_cast;
    auto& w = tasks.back();
    auto status = w.wait_for(10s);
    if (status == std::future_status::ready) {
      tasks.pop_back();
      continue;
    }
    report_progress(tasks.size() + base_task_count);
  }
  report_progress(tasks.size() + base_task_count);
}

void insert_object_list(
    google::cloud::spanner::Client spanner_client,
    std::vector<google::cloud::storage::ObjectMetadata> const& objects,
    google::cloud::spanner::Timestamp start, bool discard_output,
    std::atomic<std::uint64_t>& total_insert_count) {
  if (objects.empty()) return;

  namespace spanner = google::cloud::spanner;
  spanner_client
      .Commit([&objects, start, discard_output](auto) {
        static auto const columns = [] {
          return std::vector<std::string>{std::begin(column_names),
                                          std::end(column_names)};
        }();
        spanner::InsertOrUpdateMutationBuilder builder{
            std::string(table_name), columns};

        for (auto const& object : objects) {
          bool is_archived =
              object.time_deleted().time_since_epoch().count() != 0;
          builder.EmplaceRow(
              object.bucket(), object.name(),
              std::to_string(object.generation()), object.metageneration(),
              is_archived, static_cast<std::int64_t>(object.size()),
              object.content_type(),
              spanner::MakeTimestamp(object.time_created()).value(),
              spanner::MakeTimestamp(object.updated()).value(),
              object.storage_class(),
              spanner::MakeTimestamp(object.time_storage_class_updated())
                  .value(),
              object.md5_hash(), object.crc32c(), start);
        }
        if (discard_output) return spanner::Mutations{};
        return spanner::Mutations{std::move(builder).Build()};
      })
      .value();
  total_insert_count.fetch_add(objects.size());
}

}  // namespace gcs_indexer
