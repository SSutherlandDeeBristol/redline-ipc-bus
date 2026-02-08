#include "futex.hpp"

#include <cerrno>
#include <linux/futex.h>
#include <unistd.h>

namespace redline::shm {
FutexResult futex_wait(std::atomic<uint32_t>* addr, uint32_t expected, const std::optional<std::chrono::microseconds>& timeout) {
  timespec ts;
  timespec* ts_ptr = nullptr;

  if (timeout.has_value()) {
    ts.tv_sec = std::chrono::duration_cast<std::chrono::seconds>(*timeout).count();
    ts.tv_nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(*timeout).count() % 1'000'000'000;
    ts_ptr = &ts;
  }

  int64_t ret = syscall(SYS_futex,
                    reinterpret_cast<int*>(addr),
                    FUTEX_WAIT,
                    expected,
                    ts_ptr,
                    nullptr,
                    0);

  if (ret == 0) return FutexResult::OK;

  switch (errno) {
    case EAGAIN:    return FutexResult::WOULD_BLOCK;
    case EINTR:     return FutexResult::INTERRUPTED;
    case ETIMEDOUT: return FutexResult::TIMEOUT;
    default:        return FutexResult::ERROR;
  }
}

FutexResult futex_wake(std::atomic<uint32_t>* addr, int count) {
  int64_t ret = syscall(SYS_futex,
                    reinterpret_cast<int*>(addr),
                    FUTEX_WAKE,
                    count,
                    nullptr,
                    nullptr,
                    0);

  if (ret >= 0) return FutexResult::OK;
  return FutexResult::ERROR;
}
}