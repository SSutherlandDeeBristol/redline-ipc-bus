#include "subscriber_registry.hpp"

#include <algorithm>
#include <unistd.h>

#include "log.hpp"

#include "layout.hpp"
#include "lease.hpp"
#include "redline/utils.hpp"

namespace redline::shm {
SubscriberLease::SubscriberLease(std::shared_ptr<SegmentMapping> segment, const uint32_t index, const int fd)
    : segment_(std::move(segment)), index_(index), fd_(fd) {}

SubscriberLease::~SubscriberLease() {
  if (fd_ < 0) {
    return; // abandoned
  }
  auto& subscriber = entry();
  subscriber.pid.store(0, std::memory_order_relaxed);
  subscriber.flags.store(0, std::memory_order_relaxed);
  // EMPTY before releasing the lock, so a prober that gets the lock never mistakes us for a crashed owner
  subscriber.state.store(uint32_t(SubscriberState::EMPTY), std::memory_order_seq_cst);
  close(fd_);
}

void SubscriberLease::abandon() noexcept {
  close(fd_);
  fd_ = -1;
}

SubscriberRegistry::SubscriberRegistry(std::shared_ptr<SegmentMapping> segment)
    : segment_(std::move(segment)), shm_segment_(segment_->segment()) {}

Result<std::unique_ptr<SubscriberLease>> SubscriberRegistry::register_subscriber(const uint32_t flags) {
  // A description of our own, so the lease conflicts with every other one, including other leases in this process
  const int fd = open_lease_description(*segment_);
  if (fd < 0) {
    return std::unexpected(Error::SYSTEM_ERROR);
  }

  auto index = try_claim(fd);
  if (!index && reap(now_ns(), 0) > 0) {
    index = try_claim(fd);
  }
  if (!index) {
    close(fd);
    return std::unexpected(Error::SUBSCRIBER_REGISTRY_FULL);
  }
  auto lease = std::unique_ptr<SubscriberLease>(new SubscriberLease(segment_, *index, fd));

  auto& subscriber = lease->entry();
  subscriber.pid.store(getpid(), std::memory_order_relaxed);
  subscriber.flags.store(flags, std::memory_order_relaxed);
  subscriber.heartbeat_ns.store(now_ns(), std::memory_order_relaxed);
  subscriber.dropped_count.store(0, std::memory_order_relaxed);
  subscriber.messages_consumed.store(0, std::memory_order_relaxed);
  subscriber.max_lag.store(0, std::memory_order_relaxed);
  subscriber.last_latency_ns.store(0, std::memory_order_relaxed);

  // Publishers may read read_seq as soon as the state is OK, so give it a sensible value first, then move it up to
  // the head once we are visible. Anything reserved in between is simply not delivered to us.
  subscriber.read_seq.store(shm_segment_->write_seq.load(), std::memory_order_seq_cst);
  subscriber.state.store(uint32_t(SubscriberState::OK), std::memory_order_seq_cst);
  subscriber.read_seq.store(shm_segment_->write_seq.load(), std::memory_order_seq_cst);

  return lease;
}

std::optional<uint32_t> SubscriberRegistry::try_claim(const int fd) noexcept {
  for (uint32_t i = 0; i < segment_->subscriber_count(); ++i) {
    auto& state = segment_->subscriber(i).state;
    if (state.load(std::memory_order_relaxed) != uint32_t(SubscriberState::EMPTY)) {
      continue;
    }
    // Lease first, so an entry that isn't EMPTY always has a holder unless its owner has gone
    if (!try_lock_byte(fd, subscriber_lock_offset(i))) {
      continue;
    }
    auto expected = uint32_t(SubscriberState::EMPTY);
    if (state.compare_exchange_strong(expected, uint32_t(SubscriberState::INITIALISING))) {
      // Raise the high water mark before the entry can become OK, so scans bounded by it will see us
      auto& high_water = shm_segment_->subscriber_high_water;
      uint32_t current = high_water.load(std::memory_order_seq_cst);
      while (current < i + 1 && !high_water.compare_exchange_weak(current, i + 1, std::memory_order_seq_cst)) {
      }
      return i;
    }
    unlock_byte(fd, subscriber_lock_offset(i));
  }
  return std::nullopt;
}

uint32_t SubscriberRegistry::scan_limit() const noexcept {
  return std::min(shm_segment_->subscriber_high_water.load(std::memory_order_seq_cst), segment_->subscriber_count());
}

std::optional<uint64_t> SubscriberRegistry::min_read_seq() const noexcept {
  std::optional<uint64_t> min;
  const uint32_t limit = scan_limit();
  for (uint32_t i = 0; i < limit; ++i) {
    const auto& subscriber = segment_->subscriber(i);
    // flags are written before the entry becomes OK, so they're settled by the time it counts
    if (subscriber.state.load(std::memory_order_seq_cst) == uint32_t(SubscriberState::OK) &&
        (subscriber.flags.load(std::memory_order_relaxed) & SUBSCRIBER_RELIABLE) != 0) {
      const uint64_t read_seq = subscriber.read_seq.load(std::memory_order_acquire);
      min = min ? std::min(*min, read_seq) : read_seq;
    }
  }
  return min;
}

bool SubscriberRegistry::lease_held(const uint32_t index) const noexcept {
  std::lock_guard lk(segment_->probe_mutex());
  return held_elsewhere(*segment_, subscriber_lock_offset(index));
}

uint32_t SubscriberRegistry::reap(const int64_t now_ns, const int64_t subscriber_timeout_ns) noexcept {
  uint32_t changed = 0;
  const uint64_t write_seq = shm_segment_->write_seq.load(std::memory_order_acquire);
  const uint32_t limit = scan_limit();

  for (uint32_t i = 0; i < limit; ++i) {
    auto& subscriber = segment_->subscriber(i);
    uint32_t state = subscriber.state.load(std::memory_order_acquire);
    if (state == uint32_t(SubscriberState::EMPTY)) {
      continue;
    }

    {
      // Holding the lock ourselves stops anyone claiming the entry while we decide. If we can't get it, its owner is
      // alive (or someone else is mid-claim or mid-probe), so leave it be.
      std::lock_guard lk(segment_->probe_mutex());
      if (try_lock_byte(segment_->fd(), subscriber_lock_offset(i))) {
        // Re-read: a clean deregistration stores EMPTY before releasing its lock
        state = subscriber.state.load(std::memory_order_acquire);
        if (state != uint32_t(SubscriberState::EMPTY) &&
            subscriber.state.compare_exchange_strong(state, uint32_t(SubscriberState::EMPTY))) {
          REDLINE_LOG(WARNING) << "Reclaimed subscriber " << i << " on " << segment_->name() << " left behind by exited process "
                       << subscriber.pid.load(std::memory_order_relaxed);
          ++changed;
        }
        unlock_byte(segment_->fd(), subscriber_lock_offset(i));
        continue;
      }
    }

    // Only RELIABLE subscribers can block publishers, so only they are worth evicting
    const uint32_t flags = subscriber.flags.load(std::memory_order_relaxed);
    const bool evictable = (flags & SUBSCRIBER_RELIABLE) != 0 && (flags & SUBSCRIBER_NO_EVICT) == 0;
    if (subscriber_timeout_ns > 0 && evictable && state == uint32_t(SubscriberState::OK)) {
      const uint64_t read_seq = subscriber.read_seq.load(std::memory_order_acquire);
      const int64_t heartbeat_ns = subscriber.heartbeat_ns.load(std::memory_order_relaxed);
      if (write_seq - read_seq >= shm_segment_->slot_count && now_ns - heartbeat_ns > subscriber_timeout_ns &&
          subscriber.state.compare_exchange_strong(state, uint32_t(SubscriberState::DEAD))) {
        REDLINE_LOG(WARNING) << "Evicted subscriber " << i << " on " << segment_->name() << " (pid "
                     << subscriber.pid.load(std::memory_order_relaxed) << ") for blocking publishers";
        ++changed;
      }
    }
  }
  return changed;
}
} // namespace redline::shm
