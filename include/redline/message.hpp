#pragma once

#include <span>
#include <string_view>

namespace redline {
struct Message {
  uint64_t sequence; // 0 = first message ever published on bus, incremented by 1 for each subsequent message
  int64_t publish_timestamp_ns; // real timestamp in nanoseconds, affected by NTP
  std::span<const uint8_t> data;

  [[nodiscard]] std::string_view data_as_string() const {
    return {reinterpret_cast<const char*>(data.data()), data.size()};
  }
};
}