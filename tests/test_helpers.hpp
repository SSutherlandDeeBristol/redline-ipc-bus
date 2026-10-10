#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <functional>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "redline/bus.hpp"

namespace redline::test {

// A bus name unique to this test and process, whose segment is removed before and after the test
class ScopedBusName {
public:
  ScopedBusName() {
    static std::atomic_int counter{0};
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    name_ = std::string("test.") + (info ? info->name() : "anon") + "." + std::to_string(getpid()) + "." +
            std::to_string(counter++);
    for (auto& c : name_) {
      if (c == '/') c = '_';
    }
    destroy_bus(name_);
  }
  ~ScopedBusName() { destroy_bus(name_); }

  ScopedBusName(const ScopedBusName&) = delete;
  ScopedBusName& operator=(const ScopedBusName&) = delete;

  const std::string& str() const { return name_; }
  std::string shm_path() const { return "/dev/shm/redline." + name_; }
  operator const std::string&() const { return name_; }

private:
  std::string name_;
};

// Polls `pred` until it holds or the timeout expires
inline bool wait_until(const std::function<bool()>& pred,
                       std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::microseconds(200));
  }
  return pred();
}

inline Config test_config(BackPressureMode mode = BackPressureMode::RELIABLE, uint32_t bus_size = 64,
                          uint32_t max_message_size = 256) {
  Config config;
  config.backpressure_mode = mode;
  config.bus_size = bus_size;
  config.max_message_size = max_message_size;
  config.consumer_busy_spin_duration_us = 10;
  return config;
}

inline std::span<const uint8_t> as_bytes(const std::string& s) {
  return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}

// Self-validating payload: [producer:u32][counter:u64][len:u32] followed by bytes derived from all three,
// so any torn or mixed-up message fails is_valid_payload
inline std::vector<uint8_t> make_payload(uint32_t producer, uint64_t counter, uint32_t len) {
  len = std::max<uint32_t>(len, 16);
  std::vector<uint8_t> buf(len);
  std::memcpy(buf.data(), &producer, 4);
  std::memcpy(buf.data() + 4, &counter, 8);
  std::memcpy(buf.data() + 12, &len, 4);
  for (uint32_t i = 16; i < len; ++i) {
    buf[i] = static_cast<uint8_t>(producer * 31 + counter * 7 + i);
  }
  return buf;
}

struct PayloadInfo {
  bool valid;
  uint32_t producer;
  uint64_t counter;
};

inline PayloadInfo check_payload(std::span<const uint8_t> data) {
  PayloadInfo info{false, 0, 0};
  if (data.size() < 16) return info;
  uint32_t len;
  std::memcpy(&info.producer, data.data(), 4);
  std::memcpy(&info.counter, data.data() + 4, 8);
  std::memcpy(&len, data.data() + 12, 4);
  if (len != data.size()) return info;
  for (uint32_t i = 16; i < len; ++i) {
    if (data[i] != static_cast<uint8_t>(info.producer * 31 + info.counter * 7 + i)) return info;
  }
  info.valid = true;
  return info;
}

// Blocks subscriber callbacks until released
class Gate {
public:
  void wait() {
    std::unique_lock lk(mutex_);
    ++waiting_;
    cv_.notify_all();
    cv_.wait(lk, [&] { return open_; });
  }
  void open() {
    std::lock_guard lk(mutex_);
    open_ = true;
    cv_.notify_all();
  }
  bool has_waiter() {
    std::lock_guard lk(mutex_);
    return waiting_ > 0;
  }

private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool open_ = false;
  int waiting_ = 0;
};

// Values recorded by a subscriber callback, safe to inspect from the test thread
class Received {
public:
  void add(uint64_t v) {
    std::lock_guard lk(mutex_);
    values_.push_back(v);
  }
  std::vector<uint64_t> values() {
    std::lock_guard lk(mutex_);
    return values_;
  }
  size_t size() { return values().size(); }

private:
  std::mutex mutex_;
  std::vector<uint64_t> values_;
};

}
