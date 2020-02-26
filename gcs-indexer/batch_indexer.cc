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
#include <google/cloud/spanner/client.h>
#include <google/cloud/storage/client.h>
#include <boost/program_options.hpp>
#include <algorithm>
#include <iostream>
#include <vector>

namespace po = boost::program_options;
namespace spanner = google::cloud::spanner;
namespace gcs = google::cloud::storage;

struct work_item {
  std::string bucket;
  std::string prefix;
};

work_item make_work_item(std::string const& p) {
  auto pos = p.find_first_of('/');
  auto bucket = p.substr(0, pos);
  auto prefix = pos == std::string::npos ? std::string{} : p.substr(pos + 1);
  return {std::move(bucket), std::move(prefix)};
}

std::atomic<std::uint64_t> total_read_count;
std::atomic<std::uint64_t> total_insert_count;

void insert_object_list(spanner::Client spanner_client,
                        std::vector<gcs::ObjectMetadata> const& objects,
                        spanner::Timestamp start, bool discard_output) {
  if (objects.empty()) return;

  spanner_client
      .Commit([&objects, start, discard_output](auto) {
        static auto const columns = [] {
          return std::vector<std::string>{std::begin(column_names),
                                          std::end(column_names)};
        }();
        spanner::InsertOrUpdateMutationBuilder builder{std::string(table_name),
                                                       columns};

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

using object_metadata_queue = bounded_queue<std::vector<gcs::ObjectMetadata>>;
using work_item_queue = bounded_queue<work_item>;

void insert_worker(object_metadata_queue& queue, spanner::Database database,
                   spanner::Timestamp start, bool discard_output) {
  std::ostringstream pool_id;
  pool_id << std::this_thread::get_id();
  auto spanner_client = spanner::Client(spanner::MakeConnection(
      std::move(database),
      spanner::ConnectionOptions{}.set_num_channels(1).set_channel_pool_domain(
          std::move(pool_id).str()),
      spanner::SessionPoolOptions{}.set_min_sessions(1)));
  for (auto v = queue.pop(); v.has_value(); v = queue.pop()) {
    process_vector(*v, spanner_client, start, discard_output);
  }
}

void list_worker(object_metadata_queue& dst, work_item_queue& src,
                 int max_objects_per_mutation, bool discard_input) {
  auto gcs_client = gcs::Client::CreateDefaultClient().value();

  for (auto item = src.pop(); item.has_value(); item = src.pop()) {
    auto prefix_option =
        item->prefix.empty() ? gcs::Prefix{} : gcs::Prefix(item->prefix);
    std::vector<gcs::ObjectMetadata> buffer;
    auto flush = [&](bool force) {
      if (buffer.empty()) return;
      // TODO(...) - we should use a better estimation of the mutation cost,
      //    e.g. the number of columns affected.
      if (not force and buffer.size() < max_objects_per_mutation) return;
      if (not discard_input) dst.push(std::move(buffer));
      buffer.clear();
    };
    for (auto& o : gcs_client.ListObjects(
             item->bucket, std::move(prefix_option), gcs::Versions{true})) {
      if (not o) break;
      buffer.push_back(*std::move(o));
      ++total_read_count;
      flush(false);
    }
    flush(true);
  }
}

std::optional<std::string> get_one_prefix(spanner::Client spanner_client,
                                          gcs::Client gcs_client,
                                          std::string const& task_id,
                                          std::string const& job_id) {
  return std::optional<std::string>{};
};

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
      ("task-id", po::value<std::string>()->required(),
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
  auto const reader_thread_count = vm["reader-threads"].as<unsigned int>();

  spanner::Database const database(vm["project"].as<std::string>(),
                                   vm["instance"].as<std::string>(),
                                   vm["database"].as<std::string>());
  auto const max_objects_per_mutation =
      vm["max-objects-per-mutation"].as<int>();

  object_metadata_queue object_queue;
  work_item_queue work_queue;

  std::cout << "Starting worker threads [" << worker_thread_count << "]"
            << std::endl;
  std::vector<std::future<void>> workers;
  std::generate_n(std::back_inserter(workers), worker_thread_count,
                  [&database, start, &object_queue,
                   discard_output = vm["discard-output"].as<bool>()] {
                    return std::async(std::launch::async, insert_worker,
                                      std::ref(object_queue), database, start,
                                      discard_output);
                  });

  std::cout << "Starting reader threads [" << reader_thread_count << "]"
            << std::endl;
  std::vector<std::future<void>> readers;
  std::generate_n(std::back_inserter(readers), reader_thread_count,
                  [&work_queue, &object_queue, max_objects_per_mutation,
                   discard_input = vm["discard-input"].as<bool>()] {
                    return std::async(std::launch::async, list_worker,
                                      std::ref(object_queue),
                                      std::ref(work_queue),
                                      max_objects_per_mutation, discard_input);
                  });

  auto report_progress = [&object_queue,
                          upload_start = std::chrono::steady_clock::now()](
                             std::size_t active) {
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

  std::cout << "Populating work queue" << std::endl;
  auto const job_id = vm["job-id"].as<std::string>();
  auto const task_id = vm["task-id"].as<std::string>();
  auto spanner_client = spanner::Client(spanner::MakeConnection(database));
  auto gcs_client = gcs::Client::CreateDefaultClient().value();
  auto get_one_prefix = [&spanner_client, &gcs_client, &job_id, &task_id] {
    return pick_one_prefix(spanner_+client, gcs_client, job_id, task_id);
  };
    for (auto bucket = get_one_prefix(client, ); bucket; bucket = get_one_prefix()) {
      process_one_prefix(client, *bucket);
      mark_done(client, *bucket);
    }
  }

  // Tell the workers that no more data is coming so they can exit.
  work_queue.shutdown();

  auto wait_for_tasks = [&report_progress](std::vector<std::future<void>> tasks,
                                           std::size_t base_task_count) {
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
  };

  std::cout << "Waiting for readers" << std::endl;
  wait_for_tasks(std::move(readers), workers.size());
  object_queue.shutdown();

  std::cout << "Waiting for writers" << std::endl;
  wait_for_tasks(std::move(workers), 0);

  std::cout << "DONE\n";

  return 0;
} catch (std::exception const& ex) {
  std::cerr << "Standard exception caught " << ex.what() << '\n';
  return 1;
}
