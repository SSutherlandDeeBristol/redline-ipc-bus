#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "redline/bus.hpp"
#include "shm/layout.hpp"
#include "shm/publisher_registry.hpp"
#include "shm/ring_buffer.hpp"
#include "shm/segment.hpp"
#include "shm/subscriber_registry.hpp"

namespace redline {
// The publishing half of a Bus: reserving sequences against RELIABLE subscribers' backpressure, writing slots, and
// the publisher leases that let other processes recover a publish this process dies in the middle of.
class Producer final : private detail::ClaimOwner {
public:
  Producer(std::shared_ptr<shm::SegmentMapping> mapping, int64_t subscriber_timeout_us);
  ~Producer();

  Producer(const Producer&) = delete;
  Producer& operator=(const Producer&) = delete;

  Result<> publish_batch(std::span<const std::span<const uint8_t>> messages, int64_t timeout_us);
  Result<Claim> claim(int64_t timeout_us);

  // A publisher lease, and whether a publish or Claim in this process is using it. A lease records one reservation
  // at a time, so each thread publishing needs one of its own: it keeps the one it first gets (see ThreadSlots in
  // producer.cpp) until it exits or this Producer is destroyed. Claims take one for as long as they are open.
  struct alignas(shm::CACHE_LINE_SIZE) Slot {
    std::atomic_bool in_use{false};
    std::unique_ptr<shm::PublisherLease> lease; // set before the slot is counted, reset when the Producer is destroyed
  };

  // All the slots ever created for this Producer. Shared with the threads that have cached one of its slots, so their
  // cache entries stay valid (if useless) after the Producer has gone.
  struct SlotPool {
    static constexpr size_t SLOTS_PER_CHUNK = 64;
    static constexpr size_t MAX_CHUNKS = shm::MAX_REGISTRY_ENTRIES / SLOTS_PER_CHUNK;
    struct Chunk {
      std::array<Slot, SLOTS_PER_CHUNK> slots;
    };

    std::array<std::atomic<Chunk*>, MAX_CHUNKS> chunks{};
    std::atomic_size_t count = 0;
    std::mutex mutex;
    std::vector<std::unique_ptr<Chunk>> storage; // guarded by mutex

    Slot& at(size_t i) noexcept {
      return chunks[i / SLOTS_PER_CHUNK].load(std::memory_order_acquire)->slots[i % SLOTS_PER_CHUNK];
    }
  };

private:
  std::shared_ptr<shm::SegmentMapping> mapping_;
  shm::SHMSegment* segment_;
  std::string name_;
  uint64_t slot_count_;
  uint32_t max_message_size_;
  int64_t subscriber_timeout_ns_;

  shm::RingBuffer ring_buffer_;
  shm::SubscriberRegistry subscriber_registry_;
  shm::PublisherRegistry publisher_registry_;
  std::shared_ptr<SlotPool> pool_;

  // Lower bound on every live subscriber's read_seq, so publishers only scan the registry when the ring looks full.
  // Release/acquire so that a subscriber releasing a slot happens-before a publisher reusing it.
  std::atomic_uint64_t cached_min_read_seq_ = 0;
  std::atomic_bool scanning_ = false; // a producer in this process is refreshing cached_min_read_seq_
  std::atomic_int64_t last_reap_ns_ = 0;

  void commit_claim(uint64_t sequence, uint32_t size, void* context) noexcept override;
  void abort_claim(uint64_t sequence, void* context) noexcept override;
  void finish_claim(Slot* slot) noexcept;

  Result<Slot*> thread_slot();
  Result<Slot*> acquire_slot();
  static void release_slot(Slot* slot) noexcept { slot->in_use.store(false, std::memory_order_release); }

  bool reserve(uint64_t count, int64_t timeout_us, uint64_t& first_seq, shm::PublisherLease& lease);
  bool fits(uint64_t write_seq, uint64_t count, uint64_t min_read_seq) const noexcept;
  uint8_t* begin_slot(uint64_t seq) noexcept;
  void maybe_reap() noexcept;
};
}
