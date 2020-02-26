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
#include "indexer_utils.h"
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
  positional.add("bucket", -1);
  po::options_description options("Create a GCS indexing database");
  options.add_options()("help", "produce help message")
      //
      ("bucket", po::value<std::vector<std::string>>()->required(),
       "the bucket to refresh, use [BUCKET_NAME]/[PREFIX] to upload only a"
       " prefix")
      //
      ("job-id", po::value<std::string>()->required(),
       "use this as the job id")
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

  auto client = spanner::Client(spanner::MakeConnection(database));

  auto const insert_statement = R"sql(
INSERT INTO gcs_indexing_jobs (job_id, bucket, prefix)
VALUES (@job_id, @bucket, @prefix)
)sql";

  std::vector<spanner::SqlStatement> statements;
  auto const job_id = vm["job-id"].as<std::string>();
  for (auto const& bucket : vm["buckets"].as<std::vector<std::string>>()) {
    auto const wi = gcs_indexer::make_work_item(bucket);
    statements.push_back(spanner::SqlStatement(
        insert_statement, {{"job_id", spanner::Value(job_id)},
                           {"bucket", spanner::Value(wi.bucket)},
                           {"prefix", spanner::Value(wi.prefix)}}));
  }
  client.Commit([&](auto txn) -> google::cloud::StatusOr<spanner::Mutations> {
    auto result = client.ExecuteBatchDml(txn, statements);
    if (!result) return std::move(result).status();
    return spanner::Mutations{};
  });

  return 0;
} catch (std::exception const& ex) {
  std::cerr << "Standard exception caught " << ex.what() << '\n';
  return 1;
}
