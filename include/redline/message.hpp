#pragma once

#include <cstdint>
#include <span>
#include <string_view>

namespace redline {
struct Message {
  uint64_t sequence; // 0 = first message ever published on bus, incremented by 1 for each subsequent message
  int64_t publish_timestamp_ns; // CLOCK_MONOTONIC timestamp in nanoseconds, comparable with redline::now_ns() in any process on the host
  std::span<const uint8_t> data; // only valid for the duration of the subscriber callback, copy it if you need it afterwards

  [[nodiscard]] std::string_view data_as_string() const {
    return {reinterpret_cast<const char*>(data.data()), data.size()};
  }
};
}
