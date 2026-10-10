#pragma once

#include <cstdint>
#include <ctime>

namespace redline {
// CLOCK_MONOTONIC in nanoseconds: the clock used for Message::publish_timestamp_ns. It is shared by every process on
// the host and never jumps with NTP, so it can be used to compute latencies across processes.
inline int64_t now_ns() noexcept {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1000000000LL + ts.tv_nsec;
}
}
