#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <optional>

namespace redline::shm {
enum class FutexResult {
  OK,             // Wait/wake succeeded
  TIMEOUT,        // Timed wait expired
  WOULD_BLOCK,    // futex value did not match expected
  INTERRUPTED,    // Signal interrupted the wait (EINTR)
  ERROR           // Other errors
};

FutexResult futex_wait(std::atomic<uint32_t>* addr, uint32_t expected, const std::optional<std::chrono::microseconds>& timeout = std::nullopt);
FutexResult futex_wake(std::atomic<uint32_t>* addr, int count = 1);
}