#pragma once

#include <functional>
#include <memory>
#include <span>

#include "redline/config.hpp"

namespace redline {

struct Message {
  uint64_t sequence;
  std::span<const uint8_t> data;
  int64_t publish_timestamp; // real timestamp, affected by NTP
};

using Subscriber = std::function<void(const Message& message)>;

// TODO: enforce Bus is singleton per name
class Bus {
public:
  virtual bool publish(const std::span<const uint8_t>& data) = 0;

  // Note that the subscriber function must be thread-safe and return quickly.
  virtual void create_subscriber(const Subscriber& subscriber) = 0;

  virtual ~Bus() = default;
};

std::unique_ptr<Bus> create_bus(const std::string& name, const Config& config);
}