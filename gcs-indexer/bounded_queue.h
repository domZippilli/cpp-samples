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

#include <condition_variable>
#include <deque>
#include <iostream>
#include <mutex>
#include <optional>
#include <vector>

template <typename T>
class bounded_queue {
 public:
  bounded_queue() : bounded_queue(512, 1024) {}
  explicit bounded_queue(std::size_t lwm, std::size_t hwm)
      : lwm_(lwm), hwm_(hwm) {}

  void shutdown() {
    std::unique_lock<std::mutex> lk(mu_);
    is_shutdown_ = true;
    lk.unlock();
    cv_read_.notify_all();
    cv_write_.notify_all();
  }

  std::optional<T> pop() {
    std::unique_lock<std::mutex> lk(mu_);
    reader_wait(lk, [this] { return is_shutdown_ or not empty(); });
    // Even if `is_shutdown_` is true we need to drain any remaining
    // items.
    if (empty()) return {};
    auto next = std::move(buffer_.back());
    buffer_.pop_back();
    ++pop_count_;
    if (below_lwm() and has_writers()) {
      cv_write_.notify_all();
    }
    if (not empty() and has_readers()) {
      cv_read_.notify_one();
    }
    lk.unlock();
    return next;
  }

  void push(T data) {
    std::unique_lock<std::mutex> lk(mu_);
    writer_wait(lk, [this] { return is_shutdown_ or below_hwm(); });
    if (is_shutdown_) return;
    buffer_.push_back(std::move(data));
    ++push_count_;
    max_depth_ = (std::max)(max_depth_, buffer_.size());
    if (has_readers()) {
      cv_read_.notify_one();
    }
  }

  void print_stats(std::ostream& os) {
    std::lock_guard<std::mutex> lk(mu_);
    os << "push_count=" << push_count_ << ", pop_count=" << pop_count_
       << ", max_depth=" << max_depth_ << ", current_depth=" << buffer_.size()
       << ", reader_count=" << reader_count_ << ", max_readers=" << max_readers_
       << ", writer_count=" << writer_count_ << ", max_writers=" << max_writers_
       << ", hwm_=" << hwm_ << ", lwm_=" << lwm_
       << ", is_shutdown=" << is_shutdown_;
  }

 private:
  [[nodiscard]] bool empty() const { return buffer_.empty(); }

  [[nodiscard]] bool below_hwm() const { return buffer_.size() <= hwm_; }

  [[nodiscard]] bool below_lwm() const { return buffer_.size() <= lwm_; }

  [[nodiscard]] bool has_readers() const { return reader_count_ > 0; }

  [[nodiscard]] bool has_writers() const { return writer_count_ > 0; }

  template <typename Predicate>
  void writer_wait(std::unique_lock<std::mutex>& lk, Predicate&& p) {
    ++writer_count_;
    max_writers_ = (std::max)(max_writers_, writer_count_);
    cv_write_.wait(lk, std::forward<Predicate>(p));
    --writer_count_;
  }

  template <typename Predicate>
  void reader_wait(std::unique_lock<std::mutex>& lk, Predicate&& p) {
    ++reader_count_;
    max_readers_ = (std::max)(max_readers_, reader_count_);
    cv_read_.wait(lk, std::forward<Predicate>(p));
    --reader_count_;
  }

 private:
  std::size_t const lwm_;
  std::size_t const hwm_;

  std::mutex mu_;
  std::condition_variable cv_read_;
  std::condition_variable cv_write_;
  std::vector<T> buffer_;
  bool is_shutdown_ = false;
  int reader_count_ = 0;
  int writer_count_ = 0;
  int max_readers_ = 0;
  int max_writers_ = 0;
  std::size_t max_depth_ = 0;
  std::size_t push_count_ = 0;
  std::size_t pop_count_ = 0;
};
