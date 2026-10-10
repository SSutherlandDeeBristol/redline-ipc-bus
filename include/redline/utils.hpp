#pragma once

#include <cstdint>
#include <ctime>

namespace redline {
// CLOCK_MONOTONIC is shared by every process on the host and never jumps with NTP, so it is the clock used for
// Message::publish_timestamp_ns. Use now_ns() to compute latencies against it.
inline int64_t now_us() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

inline int64_t now_ns() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

// CLOCK_MONOTONIC_RAW is not slewed by NTP at all, which suits measuring short local intervals
inline int64_t now_benchmark_us() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

inline int64_t now_benchmark_ns() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return ts.tv_sec * 1000000000LL + ts.tv_nsec;
}
}
