#pragma once

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <unistd.h>

#include "redline/bus.hpp"

namespace redline::bench {
// A bus name unique to this process and benchmark, removed before and after use
class ScopedBus {
public:
  explicit ScopedBus(const std::string& label) : name_("bench." + label + "." + std::to_string(getpid())) {
    (void)destroy_bus(name_);
  }
  ~ScopedBus() { (void)destroy_bus(name_); }
  const std::string& name() const { return name_; }

private:
  std::string name_;
};

inline std::unique_ptr<Bus> make_bus(const std::string& name, const BusLayout& layout, const BusOptions& options) {
  auto bus = create_bus(name, layout, options);
  if (!bus) {
    std::fprintf(stderr, "create_bus failed: %s\n", to_string(bus.error()).data());
    std::abort();
  }
  return std::move(*bus);
}
}
