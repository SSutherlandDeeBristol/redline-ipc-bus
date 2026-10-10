#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include "segment.hpp"

namespace redline::shm {

// Exclusive ownership of one registry entry, held as an open file description (OFD) lock on byte `index` of the
// segment, taken through a file description of the lease's own. The kernel releases OFD locks when the description's
// last fd is closed, which happens however the owning process exits, so other processes can tell a live subscriber
// from one left behind by a crash just by testing the lock. Unlike checking a pid this works across pid namespaces,
// and unlike a robust mutex it isn't tied to the thread that took it.
//
// Destroying the lease deregisters the entry.
class SubscriberLease {
public:
  ~SubscriberLease();

  SubscriberLease(const SubscriberLease&) = delete;
  SubscriberLease& operator=(const SubscriberLease&) = delete;

  uint32_t index() const noexcept { return index_; }
  SubscriberEntry& entry() const noexcept { return segment_->subscriber(index_); }

  // Drops the lock without deregistering, exactly as if this process had crashed. For tests.
  void abandon() noexcept;

private:
  friend class SubscriberRegistry;
  SubscriberLease(std::shared_ptr<SegmentMapping> segment, uint32_t index, int fd);

  std::shared_ptr<SegmentMapping> segment_;
  uint32_t index_;
  int fd_;
};

class SubscriberRegistry {
public:
  explicit SubscriberRegistry(std::shared_ptr<SegmentMapping> segment);

  // Claims an entry for this process, starting at the current head of the bus with fresh metrics. Reclaims entries
  // left behind by exited processes if the registry is full. Returns nullptr if it is still full, and throws
  // std::runtime_error if the lease's file description can't be opened.
  // Entries registered with evictable = false are never evicted for being slow.
  std::unique_ptr<SubscriberLease> register_subscriber(bool evictable = true);

  // Lowest read_seq across live subscribers, or nullopt if there are none
  std::optional<uint64_t> min_read_seq() const noexcept;

  // Frees entries whose owner has exited. If subscriber_timeout_ns > 0, also evicts evictable live subscribers that
  // are at least a full ring behind and haven't heartbeated within the timeout. Returns how many entries were changed.
  uint32_t reap(int64_t now_ns, int64_t subscriber_timeout_ns) noexcept;

  // True if some file description other than the segment's own holds the lease on entry `index`
  bool lease_held(uint32_t index) const noexcept;

private:
  std::shared_ptr<SegmentMapping> segment_;
  SHMSegment* shm_segment_;

  std::optional<uint32_t> try_claim(int fd) noexcept;
  uint32_t scan_limit() const noexcept;
};
}
