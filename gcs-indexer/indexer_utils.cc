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
