#pragma once

#include <atomic>
#include <limits>

#include "shm/futex.hpp"
#include "shm/layout.hpp"

// Idle subscribers register in SHMSegment::waiters and sleep on commit_futex (see SubscriptionImpl::sleep_until_ready).
// Publishers only make the wake syscall when someone is registered.
namespace redline {
inline void wake_sleepers(shm::SHMSegment* segment) noexcept {
  segment->commit_futex.fetch_add(1, std::memory_order_seq_cst);
  shm::futex_wake(&segment->commit_futex, std::numeric_limits<int>::max());
}

// Called after committing. The fence pairs with the one in sleep_until_ready: either we see its waiter count, or it
// sees our commit.
inline void wake_if_sleeping(shm::SHMSegment* segment) noexcept {
  std::atomic_thread_fence(std::memory_order_seq_cst);
  if (segment->waiters.load(std::memory_order_relaxed) != 0) {
    wake_sleepers(segment);
  }
}
}
