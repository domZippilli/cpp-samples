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

#include "gcs_indexer_constants.h"
#include <google/cloud/spanner/client.h>
#include <google/cloud/spanner/database_admin_client.h>
#include <boost/program_options.hpp>
#include <algorithm>
#include <deque>
#include <iostream>
#include <vector>

namespace po = boost::program_options;
namespace spanner = google::cloud::spanner;

int main(int argc, char* argv[]) try {
  auto get_env = [](std::string_view name) -> std::string {
    auto value = std::getenv(name.data());
    return value == nullptr ? std::string{} : value;
  };

  po::positional_options_description positional;
  po::options_description options("Create a GCS indexing database");
  options.add_options()("help", "produce help message")
      //
      ("project",
       po::value<std::string>()->default_value(get_env("GOOGLE_CLOUD_PROJECT")),
       "set the Google Cloud Platform project id")
      //
      ("instance", po::value<std::string>()->required(),
       "set the Cloud Spanner instance id")
      //
      ("database", po::value<std::string>()->required(),
       "set the Cloud Spanner database id");

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
  spanner::Database const database(vm["project"].as<std::string>(),
                                   vm["instance"].as<std::string>(),
                                   vm["database"].as<std::string>());

  spanner::DatabaseAdminClient client;

  std::string gcs_objects_table_ddl = R"sql(
CREATE TABLE gcs_objects (
  bucket STRING(128),
  object STRING(1024),
  generation STRING(128),
  meta_generation STRING(128),
  is_archived BOOL,
  size INT64,
  content_type STRING(256),
  time_created TIMESTAMP,
  updated TIMESTAMP,
  storage_class STRING(256),
  time_storage_class_updated TIMESTAMP,
  md5_hash STRING(256),
  crc32c STRING(256),
  event_timestamp TIMESTAMP
) PRIMARY KEY (bucket, object, generation)
)sql";
  std::string gcs_indexer_table_ddl = R"sql(
CREATE TABLE gcs_indexing_jobs (
  job_id STRING(128),
  bucket STRING(128),
  prefix STRING(1024),
  status STRING(32),
  owner STRING(32),
  updated TIMESTAMP OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (job_id, bucket, prefix)
)sql";
  auto created = client.CreateDatabase(
      database, {gcs_objects_table_ddl, gcs_indexer_table_ddl});
  std::cout << "Waiting for database creation to complete " << std::flush;
  for (;;) {
    using namespace std::chrono_literals;
    auto status = created.wait_for(1s);
    if (status == std::future_status::ready) break;
    std::cout << '.' << std::flush;
  }
  auto db = created.get().value();  // Throw on error.
  std::cout << " DONE\n" << db.DebugString() << "\n";

  return 0;
} catch (std::exception const& ex) {
  std::cerr << "Standard exception caught " << ex.what() << '\n';
  return 1;
}
