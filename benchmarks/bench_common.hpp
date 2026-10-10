#pragma once

#include <string>
#include <unistd.h>

#include "redline/bus.hpp"

namespace redline::bench {
// A bus name unique to this process and benchmark, removed before and after use
class ScopedBus {
public:
  explicit ScopedBus(const std::string& label) : name_("bench." + label + "." + std::to_string(getpid())) {
    destroy_bus(name_);
  }
  ~ScopedBus() { destroy_bus(name_); }
  const std::string& name() const { return name_; }

private:
  std::string name_;
};
}
