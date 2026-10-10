#include "subscriber_registry.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <unistd.h>

#include "log.hpp"

#include "layout.hpp"
#include "redline/utils.hpp"

namespace redline::shm {
namespace {
// Write-locks byte `index` of the segment through `fd`. Fails if another open file description holds it. Locks taken
// through the same description never conflict with each other.
bool try_lock_entry(const int fd, const uint32_t index) noexcept {
  struct flock lock{};
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  lock.l_start = off_t(index);
  lock.l_len = 1;
  return fcntl(fd, F_OFD_SETLK, &lock) == 0;
}

void unlock_entry(const int fd, const uint32_t index) noexcept {
  struct flock lock{};
  lock.l_type = F_UNLCK;
  lock.l_whence = SEEK_SET;
  lock.l_start = off_t(index);
  lock.l_len = 1;
  fcntl(fd, F_OFD_SETLK, &lock);
}
}

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

std::unique_ptr<SubscriberLease> SubscriberRegistry::register_subscriber(const bool evictable) {
  // A new description of the same segment, so the lease conflicts with every other description, including the ones
  // other leases in this process use. Reopening through /proc also works after the segment has been unlinked.
  const std::string path = "/proc/self/fd/" + std::to_string(segment_->fd());
  const int fd = open(path.c_str(), O_RDWR | O_CLOEXEC);
  if (fd < 0) {
    throw std::runtime_error("Failed to open a subscriber lease on " + segment_->name() + ": " + std::strerror(errno));
  }

  auto index = try_claim(fd);
  if (!index && reap(now_ns(), 0) > 0) {
    index = try_claim(fd);
  }
  if (!index) {
    close(fd);
    return nullptr;
  }
  auto lease = std::unique_ptr<SubscriberLease>(new SubscriberLease(segment_, *index, fd));

  auto& subscriber = lease->entry();
  subscriber.pid.store(getpid(), std::memory_order_relaxed);
  subscriber.flags.store(evictable ? 0 : SUBSCRIBER_NO_EVICT, std::memory_order_relaxed);
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
    if (!try_lock_entry(fd, i)) {
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
    unlock_entry(fd, i);
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
    if (subscriber.state.load(std::memory_order_seq_cst) == uint32_t(SubscriberState::OK)) {
      const uint64_t read_seq = subscriber.read_seq.load(std::memory_order_acquire);
      min = min ? std::min(*min, read_seq) : read_seq;
    }
  }
  return min;
}

bool SubscriberRegistry::lease_held(const uint32_t index) const noexcept {
  std::lock_guard lk(segment_->probe_mutex());
  if (!try_lock_entry(segment_->fd(), index)) {
    return true;
  }
  unlock_entry(segment_->fd(), index);
  return false;
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
      if (try_lock_entry(segment_->fd(), i)) {
        // Re-read: a clean deregistration stores EMPTY before releasing its lock
        state = subscriber.state.load(std::memory_order_acquire);
        if (state != uint32_t(SubscriberState::EMPTY) &&
            subscriber.state.compare_exchange_strong(state, uint32_t(SubscriberState::EMPTY))) {
          REDLINE_LOG(WARNING) << "Reclaimed subscriber " << i << " on " << segment_->name() << " left behind by exited process "
                       << subscriber.pid.load(std::memory_order_relaxed);
          ++changed;
        }
        unlock_entry(segment_->fd(), i);
        continue;
      }
    }

    const bool evictable = (subscriber.flags.load(std::memory_order_relaxed) & SUBSCRIBER_NO_EVICT) == 0;
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
