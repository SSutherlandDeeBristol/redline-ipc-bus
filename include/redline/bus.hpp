#pragma once

#include <functional>
#include <memory>
#include <span>

#include "redline/config.hpp"

namespace redline {

using Subscriber = std::function<void(const std::span<const uint8_t>& data)>;

// TODO: enforce Bus is singleton per name
class Bus {
public:
  virtual bool publish(const std::span<const uint8_t>& data) = 0;

  // Note that the subscriber function must be thread-safe and return quickly.
  virtual void create_subscriber(const Subscriber& subscriber) = 0;

  virtual ~Bus() = default;
};

std::unique_ptr<Bus> createBus(const std::string& name, const Config& config);
}