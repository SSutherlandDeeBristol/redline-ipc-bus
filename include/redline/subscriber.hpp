#pragma once

#include <functional>
#include <memory>

#include "message.hpp"

namespace redline {
class Subscriber {
public:
  virtual void consume(const Message& message) = 0;

  virtual ~Subscriber() = default;
};

std::shared_ptr<Subscriber> create_subscriber(const std::function<void(const Message&)>& callback);
}