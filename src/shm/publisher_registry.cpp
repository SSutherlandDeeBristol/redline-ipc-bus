#include "publisher_registry.hpp"

#include <unistd.h>

#include "layout.hpp"
#include "lease.hpp"
#include "log.hpp"
#include "redline/utils.hpp"

// Why recovery is safe: a publisher records [from, from + count) in its entry before the compare-and-swap that
// reserves those sequences, and clears the record only after committing them. So for any sequence below write_seq
// that isn't committed, its owner's record covers it. If no live publisher's record covers it, its owner must be dead,
// and since it is below write_seq nobody can reserve it again: finishing it can't interfere with anyone.
//
// Recoverers exclude each other with an OFD lock on RECOVERY_LOCK_OFFSET (other processes) and the probe mutex (other
// threads here). That matters because finishing a slot the dead publisher had claimed is a plain store sequence, not
// a single compare-and-swap.
namespace redline::shm {

PublisherLease::PublisherLease(std::shared_ptr<SegmentMapping> segment, const uint32_t index, const int fd)
    : segment_(std::move(segment)), index_(index), fd_(fd) {}

PublisherLease::~PublisherLease() {
  if (fd_ < 0) {
    return; // abandoned
  }
  auto& publisher = entry();
  publisher.reserving_from.store(NO_RESERVATION, std::memory_order_relaxed);
  publisher.pid.store(0, std::memory_order_relaxed);
  // EMPTY before releasing the lock, so a prober that gets the lock never mistakes us for a crashed owner
  publisher.state.store(uint32_t(PublisherState::EMPTY), std::memory_order_seq_cst);
  close(fd_);
}

void PublisherLease::abandon() noexcept {
  close(fd_);
  fd_ = -1;
}

PublisherRegistry::PublisherRegistry(std::shared_ptr<SegmentMapping> segment)
    : segment_(std::move(segment)), shm_segment_(segment_->segment()), ring_buffer_(segment_) {}

std::unique_ptr<PublisherLease> PublisherRegistry::register_publisher() {
  const int fd = open_lease_description(*segment_);

  auto index = try_claim(fd);
  if (!index) {
    recover();
    index = try_claim(fd);
  }
  if (!index) {
    close(fd);
    return nullptr;
  }

  auto lease = std::unique_ptr<PublisherLease>(new PublisherLease(segment_, *index, fd));
  auto& publisher = lease->entry();
  publisher.pid.store(getpid(), std::memory_order_relaxed);
  publisher.reserving_from.store(NO_RESERVATION, std::memory_order_relaxed);
  publisher.state.store(uint32_t(PublisherState::OK), std::memory_order_seq_cst);
  return lease;
}

std::optional<uint32_t> PublisherRegistry::try_claim(const int fd) noexcept {
  for (uint32_t i = 0; i < segment_->publisher_count(); ++i) {
    auto& state = segment_->publisher(i).state;
    if (state.load(std::memory_order_relaxed) != uint32_t(PublisherState::EMPTY)) {
      continue;
    }
    // Lease first, so an entry that isn't EMPTY always has a holder unless its owner has gone
    if (!try_lock_byte(fd, publisher_lock_offset(i))) {
      continue;
    }
    auto expected = uint32_t(PublisherState::EMPTY);
    if (state.compare_exchange_strong(expected, uint32_t(PublisherState::CLAIMING))) {
      auto& high_water = shm_segment_->publisher_high_water;
      uint32_t current = high_water.load(std::memory_order_seq_cst);
      while (current < i + 1 && !high_water.compare_exchange_weak(current, i + 1, std::memory_order_seq_cst)) {
      }
      return i;
    }
    unlock_byte(fd, publisher_lock_offset(i));
  }
  return std::nullopt;
}

uint64_t PublisherRegistry::recover() noexcept {
  std::lock_guard lk(segment_->probe_mutex());
  if (!try_lock_byte(segment_->fd(), RECOVERY_LOCK_OFFSET)) {
    return 0; // another process is recovering
  }

  uint64_t finished = 0;
  const uint32_t limit = std::min(shm_segment_->publisher_high_water.load(std::memory_order_seq_cst), segment_->publisher_count());
  for (uint32_t i = 0; i < limit; ++i) {
    auto& publisher = segment_->publisher(i);
    uint32_t state = publisher.state.load(std::memory_order_seq_cst);
    if (state == uint32_t(PublisherState::EMPTY) || held_elsewhere(*segment_, publisher_lock_offset(i))) {
      continue;
    }

    // Its owner is dead. Finish whatever it left unfinished, then free the entry, unless some of it can't be finished
    // yet, in which case leave the entry for a later attempt.
    bool resolved = true;
    uint64_t finished_here = 0;
    const uint64_t from = publisher.reserving_from.load(std::memory_order_seq_cst);
    if (from != NO_RESERVATION) {
      const uint64_t count = publisher.reserving_count.load(std::memory_order_relaxed);
      for (uint64_t seq = from; seq < from + count; ++seq) {
        resolved &= finish_if_orphaned(seq, limit, finished_here);
      }
    }
    finished += finished_here;
    if (finished_here > 0) {
      REDLINE_LOG(WARNING) << "Recovered " << finished_here << " message(s) on " << segment_->name()
                           << " left unfinished by publisher " << i << " (pid " << publisher.pid.load(std::memory_order_relaxed)
                           << "), which exited mid-publish; subscribers will count them as dropped";
    }
    if (resolved) {
      publisher.reserving_from.store(NO_RESERVATION, std::memory_order_relaxed);
      publisher.state.compare_exchange_strong(state, uint32_t(PublisherState::EMPTY), std::memory_order_seq_cst);
    }
  }

  unlock_byte(segment_->fd(), RECOVERY_LOCK_OFFSET);
  if (finished > 0) {
    shm_segment_->recovered_sequences.fetch_add(finished, std::memory_order_relaxed);
  }
  return finished;
}

bool PublisherRegistry::live_record_covers(const uint64_t seq, const uint32_t limit) const noexcept {
  for (uint32_t i = 0; i < limit; ++i) {
    const auto& publisher = segment_->publisher(i);
    if (publisher.state.load(std::memory_order_seq_cst) == uint32_t(PublisherState::EMPTY)) {
      continue;
    }
    const uint64_t from = publisher.reserving_from.load(std::memory_order_seq_cst);
    const uint64_t count = publisher.reserving_count.load(std::memory_order_relaxed);
    if (from != NO_RESERVATION && seq >= from && seq - from < count && held_elsewhere(*segment_, publisher_lock_offset(i))) {
      return true;
    }
  }
  return false;
}

bool PublisherRegistry::finish_if_orphaned(const uint64_t seq, const uint32_t limit, uint64_t& finished) noexcept {
  // Above write_seq it was never reserved: the dead publisher's compare-and-swap failed. Checked first, so the
  // record scan below sees every record that could cover it.
  if (seq >= shm_segment_->write_seq.load(std::memory_order_seq_cst)) {
    return true;
  }
  if (live_record_covers(seq, limit)) {
    return false; // a live publisher owns it, or might
  }

  uint64_t target = seq;
  while (true) {
    uint64_t older = 0;
    switch (ring_buffer_.finish_lost(target, now_ns(), older)) {
      case RingBuffer::FinishResult::FINISHED:
        ++finished;
        if (target == seq) return true;
        target = seq; // the older lap is out of the way, now ours
        continue;
      case RingBuffer::FinishResult::NOTHING_TO_DO:
        if (target == seq) return true;
        target = seq;
        continue;
      case RingBuffer::FinishResult::OLDER_LAP_BUSY:
        // Stuck behind an older lap's publisher. If that one is dead too, finish it first.
        if (live_record_covers(older, limit)) return false;
        target = older;
        continue;
    }
  }
}
}
