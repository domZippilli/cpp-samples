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
std::string random_name_portion(std::mt19937_64& gen, int n) {
  static char const alphabet[] =
      "abcdefghijklmnopqrstuvwxyz"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "0123456789"
      "/-_.~@+=";
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

/// Create randomly named objects as prescribed by @p item
void process_one_item(gcs::Client gcs_client, std::mt19937_64& generator,
                      work_item const& item) {
  std::string basename;
  for (std::int64_t i = 0; i != item.object_count; ++i) {
    // Generating a random name is expensive, so we re-use the same name over
    // 100 objects.
    if (i % 100 == 0) basename = random_name_portion(generator, 32) + '-';
    auto object_name = basename + std::to_string(i);
    auto hashed = hashed_name(item.use_hash_prefix, std::move(object_name));
    std::ostringstream os;
    os << "Basename: " << basename << "\nUse Hash Prefix: " << std::boolalpha
       << item.use_hash_prefix << "\nHashed Name: " << hashed
       << "\nObject Index: " << i << "\n";
    gcs_client.InsertObject(item.bucket, hashed, std::move(os).str()).value();
    ++total_object_count;
  }
}

/// Create randomly named objects as prescribed by @p item
void process_one_item_gen(gcs::Client gcs_client, work_item const& item) {
  // Initialize a random bit source with some small amount of entropy.
  std::mt19937_64 generator(std::random_device{}());
  process_one_item(gcs_client, generator, item);
}

/// Create N parallel threads calling process_one_item().
std::vector<std::future<void>> launch_threads(po::variables_map const& vm) {
  auto const bucket = vm["bucket"].as<std::string>();
  auto const use_hash_prefix = vm["use-hash-prefix"].as<bool>();
  auto const thread_count = [&vm] {
    auto const tc = vm["thread-count"].as<unsigned int>();
    return tc == 0 ? 1U : tc;
  }();
  auto const object_count = vm["object-count"].as<long>();

  std::cout << "Creating " << object_count << " randomly named objects in "
            << bucket << std::endl;

  if (object_count == 0) return {};

  int thread_id = 0;
  auto remainder = object_count % thread_count;
  std::vector<std::future<void>> threads(thread_count);
  std::generate_n(threads.begin(), thread_count, [&] {
    auto task_object_count = object_count / thread_count;
    if (remainder > 0) {
      --remainder;
      ++task_object_count;
    }
    auto const task_id = "local_thread-" + std::to_string(++thread_id);
    work_item item{"local-job", task_id, bucket, task_object_count,
                   use_hash_prefix};
    gcs::Client gcs_client = gcs::Client::CreateDefaultClient().value();
    return std::async(std::launch::async, process_one_item_gen,
                      std::move(gcs_client), std::move(item));
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
  if (!ddl) throw std::runtime_error(ddl.status().message());
  std::cout << " DONE" << std::endl;
}

/// Insert rows in the `generate_object_jobs` table describing a job.
void schedule_job(po::variables_map const& vm) {
  auto const bucket = vm["bucket"].as<std::string>();
  auto const object_count = vm["object-count"].as<long>();
  auto const use_hash_prefix = vm["use-hash-prefix"].as<bool>();
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

  std::vector<spanner::SqlStatement> statements;
  // Cloud Spanner supports at most 20'000 rows per transaction.
  auto const max_rows = 20'000L;
  auto const task_size = 1'000L;
  auto flush = [&client, &statements] {
    client
        .Commit([&](auto txn) -> google::cloud::StatusOr<spanner::Mutations> {
          auto result = client.ExecuteBatchDml(txn, statements);
          if (!result) return std::move(result).status();
          return spanner::Mutations{};
        })
        .value();
    std::cout << "Committed " << statements.size() << " work items\n";
    statements.clear();
  };
  for (long offset = 0; offset < object_count; offset += task_size) {
    std::string task_id = "task-" + std::to_string(offset);
    std::int64_t objects_in_task = std::min(task_size, object_count - offset);
    statements.push_back(spanner::SqlStatement(
        insert_statement,
        {{"job_id", spanner::Value(job_id)},
         {"task_id", spanner::Value(task_id)},
         {"bucket", spanner::Value(bucket)},
         {"object_count", spanner::Value(objects_in_task)},
         {"use_hash_prefix", spanner::Value(use_hash_prefix)}}));
    if (statements.size() >= max_rows) flush();
  }
  if (not statements.empty()) flush();
  std::cout << "DONE\n";
}

/// Pick the next task out of the job queue.
std::optional<work_item> pick_next_work_item(spanner::Client spanner_client,
                                             std::string const& job_id,
                                             std::string const& worker_id,
                                             std::mt19937_64& generator) {
  auto const select_statement = R"sql(
SELECT task_id
     , bucket
     , object_count
     , use_hash_prefix
  FROM generate_object_jobs
 WHERE job_id = @job_id
   AND (status IS NULL
    OR  (status = 'WORKING'
   AND   TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), updated, MINUTE) > 10
        )
       )
 LIMIT 1000
)sql";
  auto const update_statement = R"sql(
UPDATE generate_object_jobs
   SET status = 'WORKING',
       owner = @worker_id,
       updated = PENDING_COMMIT_TIMESTAMP()
 WHERE job_id = @job_id
   AND task_id = @task_id)sql";

  std::optional<work_item> item;
  spanner_client
      .Commit([&](spanner::Transaction const& txn)
                  -> google::cloud::StatusOr<spanner::Mutations> {
        item = {};
        std::vector<spanner::Row> results;
        for (auto& r : spanner_client.ExecuteQuery(
                 txn,
                 spanner::SqlStatement(select_statement,
                                       {{"job_id", spanner::Value(job_id)}}))) {
          if (!r) return std::move(r).status();  // TODO(coryan) - cleanup
          results.push_back(std::move(r).value());
        }
        // There is no more work to do, exit the commit loop and have this
        // function return an empty optional.
        if (results.empty()) return spanner::Mutations{};

        auto row_idx = std::uniform_int_distribution<std::size_t>(
            0, results.size() - 1)(generator);

        auto values = std::move(results[row_idx]).values();

        // TODO(coryan) - cleanup
        auto update_result = spanner_client.ExecuteDml(
            txn,
            spanner::SqlStatement(update_statement,
                                  {{"job_id", spanner::Value(job_id)},
                                   {"task_id", spanner::Value(values[0])},
                                   {"worker_id", spanner::Value(worker_id)}}));
        if (!update_result) return std::move(update_result).status();
        if (update_result->RowsModified() != 1) {
          // This is unexpected, have the Commit() loop try again.
          return google::cloud::Status(google::cloud::StatusCode::kAborted,
                                       "please try again");
        }

        item = work_item{job_id, values[0].get<std::string>().value(),
                         values[1].get<std::string>().value(),
                         values[2].get<std::int64_t>().value(),
                         values[3].get<bool>().value()};
        return spanner::Mutations{};
      })
      .value();

  return item;
}

/// Mark a work item as completed.
void mark_done(spanner::Client spanner_client, work_item const& item) {
  // TODO(coryan) - cleanup
  spanner_client.Commit(
      [&](auto txn) -> google::cloud::StatusOr<spanner::Mutations> {
        auto const statement = R"sql(
UPDATE generate_object_jobs
   SET status = 'DONE',
       updated = PENDING_COMMIT_TIMESTAMP()
 WHERE job_id = @job_id
   AND task_id = @task_id)sql";
        auto result = spanner_client.ExecuteDml(
            txn, spanner::SqlStatement(
                     statement, {{"job_id", spanner::Value(item.job_id)},
                                 {"task_id", spanner::Value(item.task_id)}}));
        if (!result) return std::move(result).status();
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
  std::mt19937_64 generator(std::random_device{}());
  std::string worker_id = "worker-id-";
  static char const alphabet[] = "abcdefghijklmnopqrstuvwxyz0123456789";
  std::generate_n(std::back_inserter(worker_id), 16, [&generator] {
    return alphabet[std::uniform_int_distribution<int>(
        0, sizeof(alphabet) - 1)(generator)];
  });

  std::cout << "worker_id[" << worker_id << "]: processing work queue ["
            << job_id << "]" << std::endl;
  auto spanner_client = spanner::Client(spanner::MakeConnection(database));
  auto gcs_client = gcs::Client::CreateDefaultClient().value();
  auto next_item = [&] {
    return pick_next_work_item(spanner_client, job_id, worker_id, generator);
  };
  for (auto item = next_item(); item; item = next_item()) {
    std::cout << "  working on task " << item->task_id << "\n";
    process_one_item(gcs_client, generator, *item);
    mark_done(spanner_client, *item);
  }

  std::cout << "worker_id[" << worker_id << "]: waiting for stale jobs ["
            << job_id << "]" << std::endl;
  while (has_working_tasks(spanner_client, job_id)) {
    std::cout << "  waiting for slow tasks\n";
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(60s);
    if (auto item = next_item()) {
      std::cout << "  working on [stale] task " << item->task_id << "\n";
      process_one_item(gcs_client, generator, *item);
      mark_done(spanner_client, *item);
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
  long const default_object_count = 1'000'000;

  po::positional_options_description positional;
  positional.add("action", 1);
  po::options_description desc(
      "Populate a GCS Bucket with randomly named objects");
  desc.add_options()("help", "produce help message")
      //
      ("action", po::value<std::string>()->default_value("help"),
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
       "set the source bucket name")
      //
      ("object-count", po::value<long>()->default_value(default_object_count))
      //
      ("use-hash-prefix", po::value<bool>()->default_value(true))
      //
      ("thread-count",
       po::value<unsigned int>()->default_value(default_thread_count))
      //
      ("job-id", po::value<std::string>(),
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
