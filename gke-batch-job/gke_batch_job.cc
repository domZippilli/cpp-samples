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

#include <crc32c/crc32c.h>
#include <google/cloud/spanner/client.h>
#include <google/cloud/spanner/database_admin_client.h>
#include <google/cloud/storage/client.h>
#include <boost/program_options.hpp>
#include <functional>
#include <future>
#include <iostream>
#include <random>
#include <thread>
#include <vector>

namespace po = boost::program_options;
namespace gcs = google::cloud::storage;
namespace spanner = google::cloud::spanner;

std::atomic<std::int64_t> total_object_count;

/// Create a object name fragment at random.
std::string random_alphanum_string(std::mt19937_64& gen, int n) {
  static char const alphabet[] = "abcdefghijklmnopqrstuvwxyz0123456789";
  std::string result;
  std::generate_n(std::back_inserter(result), n, [&gen] {
    return alphabet[std::uniform_int_distribution<int>(
        0, sizeof(alphabet) - 1)(gen)];
  });
  return result;
}

/// Compute the hashed object name.
std::string hashed_name(bool use_hash_prefix, std::string object_name) {
  if (not use_hash_prefix) return std::move(object_name);

  // Just use the last 32-bits of the hash
  auto const hash = crc32c::Crc32c(object_name);

  char buf[16];
  std::snprintf(buf, sizeof(buf), "%08x_", hash);
  return buf + object_name;
}

struct work_item {
  std::string job_id;
  std::string task_id;
  std::string bucket;
  std::int64_t object_count;
  bool use_hash_prefix;
};

/// Create all the work items for a given task
std::vector<work_item> create_work_items(po::variables_map const& vm) {
  auto const bucket = vm["bucket"].as<std::string>();
  auto const object_count = vm["object-count"].as<long>();
  auto const use_hash_prefix = vm["use-hash-prefix"].as<bool>();
  auto const job_id = vm["job-id"].as<std::string>();
  auto const task_size = vm["task-size"].as<long>();

  std::vector<work_item> results;
  std::mt19937_64 generator(std::random_device{}());
  for (long offset = 0; offset < object_count; offset += task_size) {
    std::ostringstream os;
    os << "name-" << random_alphanum_string(generator, 32) << "-offset-"
       << std::setw(8) << std::setfill('0') << std::hex
       << static_cast<std::int64_t>(offset);
    auto task_id = std::move(os).str();
    auto const task_objects_count =
        (std::min)(task_size, object_count - offset);
    results.push_back(work_item{job_id, task_id, bucket, task_objects_count,
                                use_hash_prefix});
  }
  return results;
}

/// Create named objects as prescribed by @p item
void process_one_item(gcs::Client gcs_client, std::mt19937_64& generator,
                      work_item const& item) {
  for (std::int64_t i = 0; i != item.object_count; ++i) {
    auto object_name = item.task_id + "/object-" + std::to_string(i);
    auto hashed = hashed_name(item.use_hash_prefix, std::move(object_name));
    std::ostringstream os;
    os << "Task ID: " << item.task_id << "\nUse Hash Prefix: " << std::boolalpha
       << item.use_hash_prefix << "\nHashed Name: " << hashed
       << "\nObject Index: " << i << "\n";
    gcs_client.InsertObject(item.bucket, hashed, std::move(os).str()).value();
    ++total_object_count;
  }
}

/// Run a local worker thread.
void worker_thread(std::vector<work_item> const& work_items,
                   unsigned int thread_count, int thread_id) {
  // Initialize a random bit source with some small amount of entropy.
  std::mt19937_64 generator(std::random_device{}());
  gcs::Client gcs_client = gcs::Client::CreateDefaultClient().value();
  for (std::size_t i = 0; i != work_items.size(); ++i) {
    // Shard the work across the threads.
    if (i % thread_count != thread_id) continue;
    process_one_item(gcs_client, generator, work_items[i]);
  }
}

/// Create N parallel threads calling process_one_item().
std::vector<std::future<void>> launch_threads(po::variables_map const& vm) {
  auto const bucket = vm["bucket"].as<std::string>();
  auto const thread_count = [&vm] {
    auto const tc = vm["thread-count"].as<unsigned int>();
    return tc == 0 ? 1U : tc;
  }();

  std::cout << "Creating randomly named objects in " << bucket << std::endl;
  auto const work_items = create_work_items(vm);
  if (work_items.empty()) return {};

  int thread_id = 0;
  std::vector<std::future<void>> threads(thread_count);
  std::generate_n(threads.begin(), thread_count, [&] {
    return std::async(std::launch::async, worker_thread, work_items,
                      thread_count, thread_id++);
  });
  return threads;
}

/// Create the generate_object_jobs table in spanner.
void create_jobs_table(po::variables_map const& vm) {
  std::string generate_objects_table_ddl = R"sql(
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
)sql";
  auto database = spanner::Database(vm["project"].as<std::string>(),
                                    vm["instance"].as<std::string>(),
                                    vm["database"].as<std::string>());
  auto client =
      spanner::DatabaseAdminClient(spanner::MakeDatabaseAdminConnection());
  auto f =
      client.UpdateDatabase(std::move(database), {generate_objects_table_ddl});
  using namespace std::chrono_literals;
  std::cout << "Waiting for DDL changes to complete ";
  while (f.wait_for(1s) == std::future_status::timeout) {
    std::cout << '.' << std::flush;
  }
  auto ddl = f.get();
  if (not ddl) throw std::runtime_error(ddl.status().message());
  std::cout << " DONE" << std::endl;
}

/// Insert rows in the `generate_object_jobs` table describing a job.
void schedule_job(po::variables_map const& vm) {
  auto const job_id = vm["job-id"].as<std::string>();
  spanner::Database const database(vm["project"].as<std::string>(),
                                   vm["instance"].as<std::string>(),
                                   vm["database"].as<std::string>());

  std::cout << "Populating work queue for job " << job_id << "\n";
  auto client = spanner::Client(spanner::MakeConnection(database));

  auto const insert_statement = R"sql(
INSERT INTO generate_object_jobs
       (job_id, task_id, bucket, object_count, use_hash_prefix, updated)
VALUES (@job_id, @task_id, @bucket, @object_count, @use_hash_prefix,
        PENDING_COMMIT_TIMESTAMP())
)sql";

  // Cloud Spanner supports at most 20'000 mutations per transaction, and each
  // column counts as its own mutation. In our case we have 8 columns per
  // insert, which limits us to 2'500 rows. We use 2'000 to stay safe in case
  // new columns are added:
  //
  // More information at:
  //   https://cloud.google.com/spanner/quotas#limits_for_creating_reading_updating_and_deleting_data
  auto const max_rows = 2'000L;
  auto const work_items = create_work_items(vm);

  std::vector<spanner::SqlStatement> statements;
  auto flush = [&client, &statements] {
    client
        .Commit([&](auto txn) {
          auto result = client.ExecuteBatchDml(txn, statements).value();
          return spanner::Mutations{};
        })
        .value();
    std::cout << "Committed " << statements.size() << " work items\n";
    statements.clear();
  };
  for (auto const& wi : work_items) {
    statements.push_back(spanner::SqlStatement(
        insert_statement,
        {{"job_id", spanner::Value(wi.job_id)},
         {"task_id", spanner::Value(wi.task_id)},
         {"bucket", spanner::Value(wi.bucket)},
         {"object_count", spanner::Value(wi.object_count)},
         {"use_hash_prefix", spanner::Value(wi.use_hash_prefix)}}));
    if (statements.size() >= max_rows) flush();
  }
  if (not statements.empty()) flush();
  std::cout << "DONE\n";
}

/// Pick the next task out of the job queue.
std::optional<work_item> pick_next_work_item(
    spanner::Client spanner_client, std::string const& job_id,
    std::string const& worker_id, std::chrono::minutes const& task_timeout,
    std::mt19937_64& generator) {
  auto const select_statement = R"sql(
SELECT task_id
     , bucket
     , object_count
     , use_hash_prefix
     , status
     , owner
  FROM generate_object_jobs
 WHERE job_id = @job_id
   AND (status IS NULL
    OR  (status = 'WORKING'
   AND   TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), updated, MINUTE) > @task_timeout
        )
       )
 LIMIT 10000
)sql";

  auto get_work_items = [&] {
    std::vector<spanner::Row> available_work_items;
    available_work_items.reserve(10000);
    auto statement = spanner::SqlStatement(
        select_statement,
        {{"job_id", spanner::Value(job_id)},
         {"task_timeout", spanner::Value(task_timeout.count())}});
    for (auto& r : spanner_client.ExecuteQuery(std::move(statement))) {
      if (not r) break;
      available_work_items.push_back(*std::move(r));
    }
    std::cout << "Fetched " << available_work_items.size() << " rows"
              << std::endl;
    return available_work_items;
  };

  auto const update_statement = R"sql(
UPDATE generate_object_jobs
   SET status = 'WORKING',
       owner = @worker_id,
       updated = PENDING_COMMIT_TIMESTAMP()
 WHERE job_id = @job_id
   AND task_id = @task_id
   AND (owner = @previous_owner
        OR (owner IS NULL AND @previous_owner IS NULL)))sql";

  std::optional<work_item> item;
  spanner_client
      .Commit([&](spanner::Transaction const& txn)
                  -> google::cloud::StatusOr<spanner::Mutations> {
        item = {};
        for (auto available_work_items = get_work_items();
             not available_work_items.empty();
             available_work_items = get_work_items()) {
          while (not available_work_items.empty()) {
            item = {};
            std::cout << "Picking one row at random from "
                      << available_work_items.size() << " rows\n";

            auto const idx = std::uniform_int_distribution<std::size_t>(
                0, available_work_items.size() - 1)(generator);
            auto const& row = available_work_items[idx];
            auto const& values = row.values();

            auto task_id = values[0];
            auto previous_owner = values[5];

            std::cout << "Trying to claim work item "
                      << task_id.get<std::string>().value()
                      << " list size = " << available_work_items.size()
                      << std::endl;
            auto update_result =
                spanner_client
                    .ExecuteDml(txn,
                                spanner::SqlStatement(
                                    update_statement,
                                    {{"job_id", spanner::Value(job_id)},
                                     {"task_id", task_id},
                                     {"previous_owner", previous_owner},
                                     {"worker_id", spanner::Value(worker_id)}}))
                    .value();
            std::cout << "Updated " << update_result.RowsModified() << " rows"
                      << std::endl;
            if (update_result.RowsModified() == 1) {
              item = work_item{job_id, values[0].get<std::string>().value(),
                               values[1].get<std::string>().value(),
                               values[2].get<std::int64_t>().value(),
                               values[3].get<bool>().value()};
              return spanner::Mutations{};
            }
            available_work_items.erase(available_work_items.begin() + idx);
          }
        }
        return spanner::Mutations{};
      })
      .value();

  return item;
}

/// Set the status for a work item.
void set_item_status(spanner::Client spanner_client, work_item const& item,
                     std::string worker_id,
                     google::cloud::optional<std::string> new_status) {
  spanner_client.Commit([&](auto txn) {
    auto const statement = R"sql(
UPDATE generate_object_jobs
   SET status = @new_status,
       updated = PENDING_COMMIT_TIMESTAMP()
 WHERE job_id = @job_id
   AND task_id = @task_id
   AND owner = @worker_id)sql";
    auto result =
        spanner_client
            .ExecuteDml(
                txn, spanner::SqlStatement(
                         statement, {{"new_status", spanner::Value(new_status)},
                                     {"job_id", spanner::Value(item.job_id)},
                                     {"task_id", spanner::Value(item.task_id)},
                                     {"worker_id", spanner::Value(worker_id)}}))
            .value();
    return spanner::Mutations{};
  });
}

bool has_working_tasks(spanner::Client spanner_client,
                       std::string const& job_id) {
  auto const select_statement = R"sql(
SELECT COUNT(*)
  FROM generate_object_jobs
 WHERE job_id = @job_id
   AND status = 'WORKING'
)sql";

  auto row = spanner::GetSingularRow(
                 spanner_client.ExecuteQuery(spanner::SqlStatement(
                     select_statement, {{"job_id", spanner::Value(job_id)}})))
                 .value();
  return std::get<0>(row.get<std::tuple<std::int64_t>>().value()) != 0;
}

/// Run the worker thread for a GKE batch job.
void worker(po::variables_map const& vm) {
  spanner::Database const database(vm["project"].as<std::string>(),
                                   vm["instance"].as<std::string>(),
                                   vm["database"].as<std::string>());
  auto const job_id = vm["job-id"].as<std::string>();
  auto const task_timeout = std::chrono::minutes(vm["task-timeout"].as<int>());
  std::mt19937_64 generator(std::random_device{}());
  std::string worker_id = "worker-id-" + random_alphanum_string(generator, 16);

  std::cout << "worker_id[" << worker_id << "]: processing work queue ["
            << job_id << "]" << std::endl;
  auto spanner_client = spanner::Client(spanner::MakeConnection(database));
  auto gcs_client = gcs::Client::CreateDefaultClient().value();
  auto next_item = [&] {
    return pick_next_work_item(spanner_client, job_id, worker_id, task_timeout,
                               generator);
  };
  auto process_item = [&](work_item const& item) {
    try {
      process_one_item(gcs_client, generator, item);
      set_item_status(spanner_client, item, worker_id, "DONE");
    } catch (std::exception const& ex) {
      set_item_status(spanner_client, item, worker_id, {});
    }
  };
  for (auto item = next_item(); item; item = next_item()) {
    std::cout << "  working on task " << item->task_id << "\n";
    process_item(*item);
  }

  std::cout << "worker_id[" << worker_id << "]: waiting for stale jobs ["
            << job_id << "]" << std::endl;
  while (has_working_tasks(spanner_client, job_id)) {
    std::cout << "  waiting for slow tasks\n";
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(60s);
    if (auto item = next_item()) {
      std::cout << "  working on [stale] task " << item->task_id << "\n";
      process_item(*item);
    }
  }

  std::cout << "worker_id[" << worker_id << "]: DONE with queue [" << job_id
            << "] created " << total_object_count << " objects" << std::endl;
}

void create_objects(po::variables_map const& vm) {
  auto tasks = launch_threads(vm);
  std::cout << "Launched " << tasks.size() << " tasks... waiting" << std::endl;

  auto report_progress =
      [upload_start = std::chrono::steady_clock::now()](std::size_t active) {
        using std::chrono::duration_cast;
        using std::chrono::milliseconds;
        auto count = total_object_count.load();
        auto elapsed = std::chrono::steady_clock::now() - upload_start;
        if (count == 0 or elapsed.count() == 0) return;
        auto rate = count * 1000 / duration_cast<milliseconds>(elapsed).count();
        std::cout << "  Uploaded " << count << " objects (" << rate
                  << " objects/s, " << active << " task(s) still active)"
                  << std::endl;
      };
  while (not tasks.empty()) {
    using namespace std::chrono_literals;
    auto& w = tasks.back();
    auto status = w.wait_for(10s);
    if (status == std::future_status::ready) {
      tasks.pop_back();
      continue;
    }
    report_progress(tasks.size());
  }
  report_progress(tasks.size());
  std::cout << "DONE (" << total_object_count.load() << ")\n";
}

int main(int argc, char* argv[]) try {
  auto get_env = [](std::string_view name) -> std::string {
    auto value = std::getenv(name.data());
    return value == nullptr ? std::string{} : value;
  };

  unsigned int const default_thread_count = []() -> unsigned int {
    if (std::thread::hardware_concurrency() == 0) return 16;
    return 16 * std::thread::hardware_concurrency();
  }();
  auto const default_object_count = 1'000'000L;
  auto const default_minimum_item_size = 1'000L;
  auto const default_task_timeout = 10;

  po::positional_options_description positional;
  positional.add("action", 1);
  po::options_description desc(
      "Populate a GCS Bucket with randomly named objects");
  desc.add_options()("help", "produce help message")
      //
      ("action", po::value<std::string>()->default_value("help")->required(),
       "create the task list in the generate_jobs_queue table")
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
      ("bucket", po::value<std::string>()->required(),
       "set the destination bucket name")
      //
      ("object-count", po::value<long>()->default_value(default_object_count),
       "set the number of objects created by the job")
      //
      ("use-hash-prefix", po::value<bool>()->default_value(true),
       "prefix the object names with a hash to avoid hotspotting in GCS")
      //
      ("thread-count",
       po::value<unsigned int>()->default_value(default_thread_count))
      //
      ("task-size", po::value<long>()->default_value(default_minimum_item_size),
       "each work item created by schedule-job should contain this number of "
       "objects")
      //
      ("task-timeout", po::value<int>()->default_value(default_task_timeout),
       "set the timeout (in minutes) for tasks in the WORKING state")
      //
      ("job-id", po::value<std::string>()->required(),
       "use this job id in the generate_jobs_queue table");

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv)
                .options(desc)
                .positional(positional)
                .run(),
            vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << "\n";
    return 0;
  }
  auto help = [&desc](po::variables_map const&) {
    std::cout << "Usage: " << desc << "\n";
  };

  using action_function = std::function<void(po::variables_map const&)>;
  std::map<std::string, action_function> const actions{
      {"help", help},
      {"create-jobs-table", create_jobs_table},
      {"schedule-job", schedule_job},
      {"worker", worker},
      {"create-objects", create_objects},
  };

  auto const action_name = vm["action"].as<std::string>();
  auto const a = actions.find(action_name);
  if (a == actions.end()) {
    std::cerr << "Unknown action " << action_name << "\n";
    std::cerr << desc << "\n";
    return 1;
  }
  a->second(vm);

  return 0;
} catch (std::exception const& ex) {
  std::cerr << "Standard exception caught " << ex.what() << '\n';
  return 1;
}
