#pragma once

#include <cstdint>
#include <ctime>

namespace redline {
// CLOCK_MONOTONIC_RAW is not slewed by NTP at all, which suits measuring short local intervals such as timeouts
inline int64_t raw_now_ns() noexcept {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

// CLOCK_MONOTONIC at the resolution of the kernel's tick (a few milliseconds), but much cheaper to read
inline int64_t coarse_now_ns() noexcept {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
  return ts.tv_sec * 1000000000LL + ts.tv_nsec;
}
}
