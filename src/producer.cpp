#include "producer.hpp"

#include <thread>

#include "redline/utils.hpp"
#include "clock.hpp"
#include "cpu.hpp"
#include "log.hpp"
#include "wake.hpp"

namespace redline {
namespace {
constexpr int64_t REAP_INTERVAL_NS = 100'000'000;
constexpr uint32_t SPINS_BEFORE_YIELD = 1024;
constexpr uint32_t SPINS_PER_SCAN = 16; // how often a producer waiting for space re-reads subscribers' positions

// The slot each thread publishes with, per Producer, so that publishing doesn't have to find a free slot and mark it
// in use every time. Entries hold their pool, so one whose Producer has been destroyed is never mistaken for
// another's. A thread that publishes on more Producers than this cycles through them, giving a slot back each time.
struct ThreadSlots {
  struct Entry {
    std::shared_ptr<Producer::SlotPool> pool;
    Producer::Slot* slot = nullptr;
  };
  std::array<Entry, 8> entries;
  size_t next_victim = 0;

  ~ThreadSlots() {
    for (auto& entry : entries) {
      if (entry.slot) {
        entry.slot->in_use.store(false, std::memory_order_release);
      }
    }
  }
};
thread_local ThreadSlots thread_slots;
}

Producer::Producer(std::shared_ptr<shm::SegmentMapping> mapping, const int64_t subscriber_timeout_us)
    : mapping_(std::move(mapping)),
      segment_(mapping_->segment()),
      name_(mapping_->name()),
      slot_count_(segment_->slot_count),
      max_message_size_(segment_->slot_size_bytes),
      subscriber_timeout_ns_(subscriber_timeout_us * 1000),
      ring_buffer_(mapping_),
      subscriber_registry_(mapping_),
      publisher_registry_(mapping_),
      pool_(std::make_shared<SlotPool>()) {}

Producer::~Producer() {
  // Deregisters every lease. Threads may still have slots cached, but they can tell this Producer has gone.
  std::lock_guard lk(pool_->mutex);
  const size_t count = pool_->count.load(std::memory_order_relaxed);
  for (size_t i = 0; i < count; ++i) {
    pool_->at(i).lease.reset();
  }
}

Result<> Producer::publish_batch(const std::span<const std::span<const uint8_t>> messages, const int64_t timeout_us) {
  for (const auto& data : messages) {
    if (data.size() > max_message_size_) {
      return std::unexpected(Error::MESSAGE_TOO_LARGE);
    }
  }
  const uint64_t count = messages.size();
  if (count == 0) {
    return {};
  }
  if (count > slot_count_) {
    return std::unexpected(Error::INVALID_ARGUMENT);
  }

  const auto slot = thread_slot();
  if (!slot) {
    return std::unexpected(slot.error());
  }
  shm::PublisherLease& lease = *(*slot)->lease;

  const int64_t timestamp_ns = now_ns();
  uint64_t first_seq;
  if (!reserve(count, timeout_us, first_seq, lease)) {
    return std::unexpected(Error::BACKPRESSURE_TIMEOUT);
  }

  // No payload means a later lap already took the slot, which subscribers will account for as a drop
  for (uint64_t i = 0; i < count; ++i) {
    if (uint8_t* payload = begin_slot(first_seq + i)) {
      ring_buffer_.fill_and_commit(first_seq + i, payload, messages[i], timestamp_ns);
    }
  }
  lease.done();
  wake_if_sleeping(segment_);
  return {};
}

Result<Claim> Producer::claim(const int64_t timeout_us) {
  // The claim keeps a lease of its own until it is committed or aborted, so its record covers the open sequence
  const auto slot = acquire_slot();
  if (!slot) {
    return std::unexpected(slot.error());
  }
  while (true) {
    uint64_t seq;
    if (!reserve(1, timeout_us, seq, *(*slot)->lease)) {
      release_slot(*slot);
      return std::unexpected(Error::BACKPRESSURE_TIMEOUT);
    }
    if (uint8_t* data = begin_slot(seq)) {
      return Claim(this, seq, std::span<uint8_t>(data, max_message_size_), *slot);
    }
    // Producers lapped each other and a later lap already took this slot, so this sequence is lost to subscribers
    // (they count it as dropped). Take the next one.
  }
}

void Producer::commit_claim(const uint64_t sequence, const uint32_t size, void* context) noexcept {
  ring_buffer_.commit(sequence, size, now_ns());
  finish_claim(static_cast<Slot*>(context));
}

void Producer::abort_claim(const uint64_t sequence, void* context) noexcept {
  ring_buffer_.abort(sequence, now_ns());
  finish_claim(static_cast<Slot*>(context)); // subscribers waiting on this sequence can move past it
}

void Producer::finish_claim(Slot* slot) noexcept {
  slot->lease->done();
  release_slot(slot);
  wake_if_sleeping(segment_);
}

Result<Producer::Slot*> Producer::thread_slot() {
  ThreadSlots& cache = thread_slots;
  for (const auto& entry : cache.entries) {
    if (entry.pool == pool_) {
      return entry.slot;
    }
  }

  const auto slot = acquire_slot();
  if (!slot) {
    return slot;
  }
  auto& victim = cache.entries[cache.next_victim];
  cache.next_victim = (cache.next_victim + 1) % cache.entries.size();
  if (victim.slot) {
    release_slot(victim.slot);
  }
  victim.pool = pool_;
  victim.slot = *slot;
  return *slot;
}

Result<Producer::Slot*> Producer::acquire_slot() {
  thread_local size_t hint = 0; // where this thread last found a free slot, so threads tend to keep to their own
  SlotPool& pool = *pool_;
  const size_t count = pool.count.load(std::memory_order_acquire);
  for (size_t k = 0; k < count; ++k) {
    const size_t i = (hint + k) % count;
    Slot& slot = pool.at(i);
    if (!slot.in_use.load(std::memory_order_relaxed) && !slot.in_use.exchange(true, std::memory_order_acquire)) {
      hint = i;
      return &slot;
    }
  }

  // All busy: register another lease
  std::lock_guard lk(pool.mutex);
  const size_t n = pool.count.load(std::memory_order_relaxed);
  if (n == SlotPool::MAX_CHUNKS * SlotPool::SLOTS_PER_CHUNK) {
    return std::unexpected(Error::PUBLISHER_REGISTRY_FULL);
  }
  auto lease = publisher_registry_.register_publisher();
  if (!lease) {
    return std::unexpected(lease.error());
  }
  if (n % SlotPool::SLOTS_PER_CHUNK == 0) {
    pool.storage.push_back(std::make_unique<SlotPool::Chunk>());
    pool.chunks[n / SlotPool::SLOTS_PER_CHUNK].store(pool.storage.back().get(), std::memory_order_release);
  }
  Slot& slot = pool.at(n);
  slot.lease = std::move(*lease);
  slot.in_use.store(true, std::memory_order_relaxed);
  pool.count.store(n + 1, std::memory_order_release);
  hint = n;
  return &slot;
}

// Claims seq's slot. If an older lap's publisher is still writing it, waits, and if that goes on for a while checks
// whether it has died. Returns null if a later lap already took the slot.
uint8_t* Producer::begin_slot(const uint64_t seq) noexcept {
  int64_t waiting_since_ns = 0;
  while (true) {
    const auto begin = ring_buffer_.try_begin_write(seq);
    if (begin.payload || begin.lapped) {
      return begin.payload;
    }
    const int64_t now = raw_now_ns();
    if (waiting_since_ns == 0) {
      waiting_since_ns = now;
    } else if (now - waiting_since_ns > shm::STALL_RECOVERY_NS) {
      publisher_registry_.recover();
      waiting_since_ns = now;
    }
    cpu_relax();
  }
}

// Reserves `count` consecutive sequences once every live RELIABLE subscriber has consumed the messages currently in
// their slots (immediately, if there are none). The compare-exchange makes the capacity check and the reservation one
// step, so concurrent producers can't overrun a subscriber between them.
// The lease records each attempt before it is made, so recovery can tell whose sequences are whose.
bool Producer::reserve(const uint64_t count, const int64_t timeout_us, uint64_t& first_seq, shm::PublisherLease& lease) {
  uint64_t write_seq = segment_->write_seq.load(std::memory_order_seq_cst);
  int64_t start_ns = -1;
  uint32_t spins = 0;

  while (true) {
    if (!fits(write_seq, count, cached_min_read_seq_.load(std::memory_order_acquire))) {
      // Rescanning reads every subscriber's read_seq, which they write for every message. If waiting producers
      // rescanned constantly they would slow down the subscribers they are waiting for, so one thread per process
      // scans, every SPINS_PER_SCAN spins, and the rest wait for its result.
      uint64_t min_read_seq = cached_min_read_seq_.load(std::memory_order_acquire);
      if (spins % SPINS_PER_SCAN == 0 && !scanning_.load(std::memory_order_relaxed) &&
          !scanning_.exchange(true, std::memory_order_acquire)) {
        min_read_seq = subscriber_registry_.min_read_seq().value_or(write_seq);
        cached_min_read_seq_.store(min_read_seq, std::memory_order_release);
        scanning_.store(false, std::memory_order_release);
      }

      if (!fits(write_seq, count, min_read_seq)) {
        maybe_reap();

        if (start_ns < 0) {
          start_ns = raw_now_ns();
        } else if (timeout_us >= 0 && raw_now_ns() - start_ns >= timeout_us * 1000) {
          lease.done();
          return false;
        }

        if (++spins % SPINS_BEFORE_YIELD == 0) {
          std::this_thread::yield();
        } else {
          cpu_relax();
        }
        write_seq = segment_->write_seq.load(std::memory_order_seq_cst);
        continue;
      }
    }

    lease.reserving(write_seq, count);
    if (segment_->write_seq.compare_exchange_weak(write_seq, write_seq + count, std::memory_order_seq_cst)) {
      first_seq = write_seq;
      return true;
    }
  }
}

// Whether sequences [write_seq, write_seq + count) can be written without overwriting anything at or after min_read_seq
bool Producer::fits(const uint64_t write_seq, const uint64_t count, const uint64_t min_read_seq) const noexcept {
  // min_read_seq can be ahead of a stale write_seq, which means there is plenty of room
  return write_seq < min_read_seq || write_seq + count - min_read_seq <= slot_count_;
}

// Checks for subscribers that have died or (if configured) stalled, and for publishers that died mid-publish leaving
// subscribers stuck, at most every REAP_INTERVAL_NS per Producer
void Producer::maybe_reap() noexcept {
  const int64_t now = now_ns();
  int64_t last = last_reap_ns_.load(std::memory_order_relaxed);
  if (now - last < REAP_INTERVAL_NS || !last_reap_ns_.compare_exchange_strong(last, now)) {
    return;
  }
  if (subscriber_registry_.reap(now, subscriber_timeout_ns_) > 0) {
    cached_min_read_seq_.store(0, std::memory_order_release); // force a rescan
  }
  publisher_registry_.recover();
}
}
