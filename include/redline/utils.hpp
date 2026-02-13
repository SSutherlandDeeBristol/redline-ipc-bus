#pragma once

namespace redline {
inline int64_t now_us() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

inline int64_t now_benchmark_us() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

inline int64_t now_ns() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

inline int64_t now_benchmark_ns() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return ts.tv_sec * 1000000000LL + ts.tv_nsec;
}
}