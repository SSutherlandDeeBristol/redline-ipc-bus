#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include "ring_buffer.hpp"
#include "segment.hpp"

namespace redline::shm {
// How long a reserved sequence can stay unfinished before a waiting subscriber or publisher checks whether its
// publisher has died. Recovery never touches a live publisher's sequence, so this only bounds how often we look.
constexpr int64_t STALL_RECOVERY_NS = 1'000'000;


// Ownership of one publisher registry entry, held as an OFD lock (see lease.hpp). A lease may have at most one
// reservation outstanding at a time. Destroying the lease deregisters the entry.
class PublisherLease {
public:
  ~PublisherLease();

  PublisherLease(const PublisherLease&) = delete;
  PublisherLease& operator=(const PublisherLease&) = delete;

  uint32_t index() const noexcept { return index_; }
  PublisherEntry& entry() const noexcept { return segment_->publisher(index_); }

  // Records that this publisher is about to try reserving [from, from + count). Must be called before every attempt
  // at the compare-and-swap on write_seq, and the record kept until everything reserved has been committed.
  // Relaxed is enough: the compare-and-swap that follows releases the record to anyone who sees its write_seq.
  void reserving(const uint64_t from, const uint64_t count) noexcept {
    entry().reserving_count.store(count, std::memory_order_relaxed);
    entry().reserving_from.store(from, std::memory_order_relaxed);
  }

  // Everything this publisher reserved has been committed (or it reserved nothing)
  void done() noexcept { entry().reserving_from.store(NO_RESERVATION, std::memory_order_release); }

  // Drops the lock without deregistering, exactly as if this process had crashed. For tests.
  void abandon() noexcept;

private:
  friend class PublisherRegistry;
  PublisherLease(std::shared_ptr<SegmentMapping> segment, uint32_t index, int fd);

  std::shared_ptr<SegmentMapping> segment_;
  uint32_t index_;
  int fd_;
};

class PublisherRegistry {
public:
  explicit PublisherRegistry(std::shared_ptr<SegmentMapping> segment);

  // Claims an entry, recovering entries left by dead publishers if the registry is full. Fails with
  // PUBLISHER_REGISTRY_FULL if it is still full, and SYSTEM_ERROR if the lease's file description can't be opened.
  Result<std::unique_ptr<PublisherLease>> register_publisher();

  // Finishes, as lost, every sequence that a dead publisher reserved but never committed, so that subscribers and
  // later publishers can get past it, and frees the dead publishers' entries. A sequence is only ever finished when no
  // live publisher could own it, so slow publishers are never interfered with. Returns at once if another thread or
  // process is already recovering. Returns how many sequences were finished.
  uint64_t recover() noexcept;

private:
  std::shared_ptr<SegmentMapping> segment_;
  SHMSegment* shm_segment_;
  RingBuffer ring_buffer_;

  std::optional<uint32_t> try_claim(int fd) noexcept;

  // The rest require the probe mutex and the recovery lock
  bool live_record_covers(uint64_t seq, uint32_t limit) const noexcept;
  bool finish_if_orphaned(uint64_t seq, uint32_t limit, uint64_t& finished) noexcept;
};
}
