#pragma once

#include <functional>
#include <memory>
#include <span>

#include "errors.hpp"
#include "message.hpp"
#include "redline/config.hpp"

namespace redline {

// TODO: upgrade to class with extra observability/metrics systems?
using Subscriber = std::function<void(const Message& message)>;

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
  virtual ErrorCode create_subscriber(const Subscriber& subscriber) = 0;

  virtual ~Bus() = default;
};

std::unique_ptr<Bus> create_bus(const std::string& name, const Config& config);
}