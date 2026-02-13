#pragma once

#include <functional>
#include <memory>
#include <span>

#include "errors.hpp"
#include "subscriber.hpp"
#include "redline/config.hpp"

namespace redline {
struct SubscriberMetricsSnapshot {
  uint64_t messages_consumed = 0;
  uint64_t dropped_messages = 0;
  uint64_t current_lag = 0;
  uint64_t max_lag = 0;
  int64_t last_latency_ns = 0;

  // Percentiles
  int64_t p50_latency_ns = 0;
  int64_t p90_latency_ns = 0;
  int64_t p99_latency_ns = 0;
  int64_t p999_latency_ns = 0;
};

class Bus {
public:
  // timeout_us = -1 means no timeout (i.e. block indefinitely until message is published successfully)
  // timeout_us has no effect if backpressure_mode is not BackPressureMode::RELIABLE
  //
  // thread-safe: multiple threads can call publish concurrently
  virtual ErrorCode publish(const std::span<const uint8_t>& data, int64_t timeout_us = -1) = 0;

  // Note that the subscriber function must be thread-safe and return quickly.
  // If the subscriber function throws an exception, the exception will be caught and logged, and the subscriber will continue to receive messages as normal.
  // create_subscriber is thread-safe but may wait on a mutex if another thread is currently creating a subscriber.
  virtual ErrorCode add_subscriber(const std::function<void(const Message&)>& callback) = 0;
  virtual ErrorCode add_subscriber(std::shared_ptr<Subscriber> subscriber) = 0;

  virtual SubscriberMetricsSnapshot get_subscriber_metrics() const = 0;

  virtual ~Bus() = default;
};

std::unique_ptr<Bus> create_bus(const std::string& name, const Config& config);
}