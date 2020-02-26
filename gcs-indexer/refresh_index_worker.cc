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

#include "bounded_queue.h"
#include "gcs_indexer_constants.h"
#include "gcs_indexer_utils.h"
#include <google/cloud/spanner/client.h>
#include <google/cloud/storage/client.h>
#include <boost/program_options.hpp>
#include <algorithm>
#include <iostream>
#include <vector>

namespace po = boost::program_options;
namespace spanner = google::cloud::spanner;
namespace gcs = google::cloud::storage;

std::atomic<std::uint64_t> total_read_count;
std::atomic<std::uint64_t> total_insert_count;

using object_metadata_queue = bounded_queue<std::vector<gcs::ObjectMetadata>>;
using work_item_queue = bounded_queue<gcs_indexer::work_item>;

void insert_worker(object_metadata_queue& queue,
                   spanner::Database const& database, spanner::Timestamp start,
                   bool discard_output) {
  std::ostringstream pool_id;
  pool_id << std::this_thread::get_id();
  auto spanner_client = spanner::Client(spanner::MakeConnection(
      database,
      spanner::ConnectionOptions{}.set_num_channels(1).set_channel_pool_domain(
          std::move(pool_id).str()),
      spanner::SessionPoolOptions{}.set_min_sessions(1)));
  for (auto v = queue.pop(); v.has_value(); v = queue.pop()) {
    gcs_indexer::insert_object_list(spanner_client, *v, start, discard_output,
                                    total_insert_count);
  }
}

void process_one_item(gcs::Client gcs_client, po::variables_map const& vm,
                      gcs_indexer::work_item const& item,
                      spanner::Timestamp const& start) {
  auto const worker_thread_count = vm["worker-threads"].as<unsigned int>();
  auto const max_objects_per_mutation =
      vm["max-objects-per-mutation"].as<int>();
  auto const discard_output = vm["discard-output"].as<bool>();
  auto const discard_input = vm["discard-input"].as<bool>();
  spanner::Database const database(vm["project"].as<std::string>(),
                                   vm["instance"].as<std::string>(),
                                   vm["database"].as<std::string>());

  object_metadata_queue object_queue;

  std::cout << "Starting worker threads [" << worker_thread_count << "] for "
            << "gs://" << item.bucket << "/" << item.prefix << "**"
            << std::endl;
  std::vector<std::future<void>> workers;
  std::generate_n(std::back_inserter(workers), worker_thread_count,
                  [&database, start, &object_queue, discard_output] {
                    return std::async(std::launch::async, insert_worker,
                                      std::ref(object_queue), database, start,
                                      discard_output);
                  });

  auto prefix_option =
      item.prefix.empty() ? gcs::Prefix{} : gcs::Prefix(item.prefix);
  std::vector<gcs::ObjectMetadata> buffer;
  auto upload_start = std::chrono::steady_clock::now();
  auto flush = [&](bool force) {
    if (buffer.empty()) return;
    if (not force and buffer.size() < max_objects_per_mutation) return;
    if (not discard_input) object_queue.push(std::move(buffer));
    buffer.clear();
  };
  for (auto& o : gcs_client.ListObjects(item.bucket, std::move(prefix_option),
                                        gcs::Versions{true})) {
    if (not o) break;
    buffer.push_back(*std::move(o));
    ++total_read_count;
    flush(false);
  }
  flush(true);
  std::cout << "ListObjects done [" << total_read_count.load() << "]\n";
  object_queue.shutdown();

  auto report_progress = [&object_queue, upload_start](std::size_t active) {
    using std::chrono::duration_cast;
    using std::chrono::milliseconds;
    auto read_count = total_read_count.load();
    auto insert_count = total_insert_count.load();
    auto elapsed = std::chrono::steady_clock::now() - upload_start;
    if ((read_count == 0 and insert_count == 0) or elapsed.count() == 0) return;
    auto log = [elapsed](char const* action, std::uint64_t count) {
      auto rate = count * 1000 / duration_cast<milliseconds>(elapsed).count();
      std::cout << "  " << action << " " << count << " objects (" << rate
                << " objects/s)\n";
    };
    log("Read", read_count);
    log("Upload", insert_count);
    std::cout << "  " << active << " task(s) still active, queue={";
    object_queue.print_stats(std::cout);
    std::cout << "}\n";
  };

  gcs_indexer::wait_for_tasks(std::move(workers), 0, report_progress);
  std::cout << "DONE [" << total_insert_count.load() << "]\n";
}

std::optional<gcs_indexer::work_item> pick_next_prefix(
    spanner::Client spanner_client, std::string const& job_id,
    std::string const& task_id) {
  auto const select_statement = R"sql(
SELECT bucket, prefix
  FROM gcs_indexing_jobs
 WHERE owner IS NULL
   AND job_id = @job_id
 LIMIT 1
)sql";
  auto const update_statement = R"sql(
UPDATE gcs_indexing_jobs
   SET status = 'WORKING',
       owner = @task_id,
       updated = PENDING_COMMIT_TIMESTAMP()
 WHERE job_id = @job_id
   AND bucket = @bucket
   AND prefix = @prefix)sql";

  std::optional<gcs_indexer::work_item> item;
  spanner_client
      .Commit([&](spanner::Transaction const& txn)
                  -> google::cloud::StatusOr<spanner::Mutations> {
        auto rows = spanner_client.ExecuteQuery(
            txn, spanner::SqlStatement(select_statement,
                                       {{"job_id", spanner::Value(job_id)}}));
        auto it = rows.begin();
        // There is no more work to do, exit the commit loop and have this
        // function return an empty optional.
        if (it == rows.end()) return spanner::Mutations{};
        auto row = std::move(*it);
        if (!row) return std::move(row).status();

        auto values = std::move(row)->values();

        auto update_result = spanner_client.ExecuteDml(
            txn, spanner::SqlStatement(update_statement,
                                       {{"job_id", spanner::Value(job_id)},
                                        {"task_id", spanner::Value(task_id)},
                                        {"bucket", values[0]},
                                        {"prefix", values[1]}}));
        if (!update_result) return std::move(update_result).status();

        // TODO(coryan) - show and tell on the need for `template ` here.
        item = gcs_indexer::work_item{values[0].get<std::string>().value(),
                                      values[1].get<std::string>().value()};
        return spanner::Mutations{};
      })
      .value();

  return item;
};

void mark_done(spanner::Client spanner_client, std::string const& task_id,
               std::string const& job_id, gcs_indexer::work_item const& item) {
  spanner_client.Commit(
      [&](auto txn) -> google::cloud::StatusOr<spanner::Mutations> {
        auto const statement = R"sql(
UPDATE gcs_indexing_jobs
   SET status = 'DONE',
       updated = PENDING_COMMIT_TIMESTAMP()
 WHERE job_id = @job_id
   AND bucket = @bucket
   AND prefix = @prefix)sql";
        auto result = spanner_client.ExecuteDml(
            txn, spanner::SqlStatement(
                     statement, {
                                    {"job_id", spanner::Value(job_id)},
                                    {"bucket", spanner::Value(item.bucket)},
                                    {"prefix", spanner::Value(item.prefix)},
                                }));
        if (!result) return std::move(result).status();
        return spanner::Mutations{};
      });
}

int main(int argc, char* argv[]) try {
  auto get_env = [](std::string_view name) -> std::string {
    auto value = std::getenv(name.data());
    return value == nullptr ? std::string{} : value;
  };

  auto default_thread_count = [](int thread_per_core) -> int {
    auto const cores = std::thread::hardware_concurrency();
    if (cores == 0) return thread_per_core;
    return static_cast<int>(cores * thread_per_core);
  };

  // This magic value is approximately 20000 (the spanner limit for "things
  // changed by a single transaction") divided by the number of columns affected
  // by each object.
  int const default_max_objects_per_mutation = 1200;

  po::positional_options_description positional;
  positional.add("bucket", -1);
  po::options_description options("Create a GCS indexing database");
  options.add_options()("help", "produce help message")
      //
      ("job-id", po::value<std::string>()->required(),
       "read buckets and prefixes to index from the gcs_indexing_jobs table")
      //
      ("task-id",
       po::value<std::string>()->required()->default_value(
           "GCS_INDEXER_TASK_ID"),
       "read buckets and prefixes to index from the gcs_indexing_jobs table")
      //
      ("project",
       po::value<std::string>()->default_value(get_env("GOOGLE_CLOUD_PROJECT")),
       "set the Google Cloud Platform project id")
      //
      ("instance", po::value<std::string>()->required(),
       "set the Cloud Spanner instance id")
      //
      ("database", po::value<std::string>()->required(),
       "set the Cloud Spanner database id")
      //
      ("worker-threads",
       po::value<unsigned int>()->default_value(default_thread_count(16)),
       "the number of threads uploading data to Cloud Spanner")
      //
      ("discard-input", po::value<bool>()->default_value(false),
       "discard all data read from GCS, used for testing")
      //
      ("discard-output", po::value<bool>()->default_value(false),
       "discard data before sending it to Cloud Spanner, used for testing")
      //
      ("max-objects-per-mutation",
       po::value<int>()->default_value(default_max_objects_per_mutation));

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv)
                .options(options)
                .positional(positional)
                .run(),
            vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << options << "\n";
    return 0;
  }
  for (auto arg : {"project", "instance", "database"}) {
    if (vm.count(arg) != 1 || vm[arg].as<std::string>().empty()) {
      std::cout << "The --" << arg
                << " option must be set to a non-empty value\n"
                << options << "\n";
      return 1;
    }
  }

  auto const start =
      spanner::MakeTimestamp(std::chrono::system_clock::now()).value();

  auto const worker_thread_count = vm["worker-threads"].as<unsigned int>();

  spanner::Database const database(vm["project"].as<std::string>(),
                                   vm["instance"].as<std::string>(),
                                   vm["database"].as<std::string>());
  auto const max_objects_per_mutation =
      vm["max-objects-per-mutation"].as<int>();

  std::cout << "Reading indexing jobs" << std::endl;
  auto const job_id = vm["job-id"].as<std::string>();
  auto const task_id = vm["task-id"].as<std::string>();
  auto spanner_client = spanner::Client(spanner::MakeConnection(database));
  auto gcs_client = gcs::Client::CreateDefaultClient().value();
  auto next_prefix = [&spanner_client, &gcs_client, &job_id, &task_id] {
    return pick_next_prefix(spanner_client, job_id, task_id);
  };
  for (auto item = next_prefix(); item; item = next_prefix()) {
    process_one_item(gcs_client, vm, *item, start);
    mark_done(spanner_client, task_id, job_id, *item);
  }

  return 0;
} catch (std::exception const& ex) {
  std::cerr << "Standard exception caught " << ex.what() << '\n';
  return 1;
}
