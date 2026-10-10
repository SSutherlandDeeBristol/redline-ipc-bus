#include "ring_buffer.hpp"

#include <algorithm>
#include <cstring>

#include "cpu.hpp"
#include "layout.hpp"

namespace redline::shm {
// Payload copies race with other processes by design: the seqlock version check detects any overlap and discards the
// result. They live in their own functions so ThreadSanitizer can be told to ignore exactly these races
// (see tests/tsan.supp).
__attribute__((noinline)) void seqlock_copy_to_slot(uint8_t* dst, const uint8_t* src, size_t size) noexcept {
  if (size == 0) return; // an empty span's data() may be null, which memcpy doesn't allow even for zero bytes
  std::memcpy(dst, src, size);
}

__attribute__((noinline)) void seqlock_copy_from_slot(uint8_t* dst, const uint8_t* src, size_t size) noexcept {
  std::memcpy(dst, src, size);
}

RingBuffer::RingBuffer(std::shared_ptr<SegmentMapping> segment)
    : segment_(std::move(segment)), slot_size_bytes_(segment_->segment()->slot_size_bytes) {}

bool RingBuffer::write(const uint64_t seq, const std::span<const uint8_t>& data, const int64_t timestamp_ns) noexcept {
  uint8_t* payload = begin_write(seq);
  if (!payload) {
    return false;
  }
  fill_and_commit(seq, payload, data, timestamp_ns);
  return true;
}

void RingBuffer::fill_and_commit(const uint64_t seq, uint8_t* payload, const std::span<const uint8_t>& data,
                                 const int64_t timestamp_ns) noexcept {
  seqlock_copy_to_slot(payload, data.data(), data.size());
  commit(seq, uint32_t(data.size()), timestamp_ns);
}

uint8_t* RingBuffer::begin_write(const uint64_t seq) noexcept {
  while (true) {
    const Begin begin = try_begin_write(seq);
    if (begin.payload || begin.lapped) {
      return begin.payload;
    }
    cpu_relax(); // an older lap is still writing this slot
  }
}

RingBuffer::Begin RingBuffer::try_begin_write(const uint64_t seq) noexcept {
  SlotHeader* header = segment_->header_at(seq);

  // Claim the slot by moving its version to odd. Only one producer can do this at a time, so two producers a lap
  // apart can never interleave their writes.
  uint64_t version = header->version.load(std::memory_order_relaxed);
  while (true) {
    if (version >= writing_version(seq)) {
      return {nullptr, true, 0}; // a later lap got here first
    }
    if (version & 1) {
      return {nullptr, false, (version - 1) / 2}; // an older lap is still writing this slot
    }
    if (header->version.compare_exchange_weak(version, writing_version(seq), std::memory_order_relaxed)) {
      break;
    }
  }
  // Keep the payload writes that follow from becoming visible before the odd version
  std::atomic_thread_fence(std::memory_order_release);
  return {segment_->payload_at(seq), false, 0};
}

RingBuffer::FinishResult RingBuffer::finish_lost(const uint64_t seq, const int64_t timestamp_ns, uint64_t& older) noexcept {
  SlotHeader* header = segment_->header_at(seq);
  uint64_t version = header->version.load(std::memory_order_acquire);
  while (true) {
    if (version >= committed_version(seq)) {
      return FinishResult::NOTHING_TO_DO;
    }
    if (version == writing_version(seq)) {
      // The dead publisher got as far as claiming the slot. Nobody else can write it while it is odd, and the
      // recovery lock keeps other recoverers out, so finish it as the publisher would have.
      break;
    }
    if (version & 1) {
      older = (version - 1) / 2;
      return FinishResult::OLDER_LAP_BUSY;
    }
    // Not claimed yet: claim it ourselves. A publisher lapping us may beat us to it, in which case the sequence is
    // already gone and subscribers count it as overwritten.
    if (header->version.compare_exchange_weak(version, writing_version(seq), std::memory_order_acquire)) {
      break;
    }
  }
  commit(seq, LOST_SIZE, timestamp_ns);
  return FinishResult::FINISHED;
}

void RingBuffer::commit(const uint64_t seq, const uint32_t size, const int64_t timestamp_ns) noexcept {
  SlotHeader* header = segment_->header_at(seq);
  header->size.store(size, std::memory_order_relaxed);
  header->publish_timestamp_ns.store(timestamp_ns, std::memory_order_relaxed);
  header->version.store(committed_version(seq), std::memory_order_release);
}

RingBuffer::ReadOutcome RingBuffer::read(const uint64_t seq, uint8_t* out) const noexcept {
  const SlotHeader* header = segment_->header_at(seq);

  const uint64_t version = header->version.load(std::memory_order_acquire);
  if (version < committed_version(seq)) {
    return {ReadResult::NOT_READY, 0, 0};
  }
  if (version > committed_version(seq)) {
    return {ReadResult::OVERWRITTEN, 0, 0};
  }

  // The size may be torn if a writer is overtaking us, so never let it run past the slot. The version check below
  // throws away anything read during an overlap.
  const uint32_t raw_size = header->size.load(std::memory_order_relaxed);
  const bool aborted = raw_size == ABORTED_SIZE;
  const bool lost = raw_size == LOST_SIZE;
  const uint32_t size = aborted || lost ? 0 : std::min(raw_size, slot_size_bytes_);
  const int64_t timestamp_ns = header->publish_timestamp_ns.load(std::memory_order_relaxed);
  seqlock_copy_from_slot(out, segment_->payload_at(seq), size);

  std::atomic_thread_fence(std::memory_order_acquire);
  if (header->version.load(std::memory_order_relaxed) != version) {
    return {ReadResult::OVERWRITTEN, 0, 0};
  }

  return {aborted ? ReadResult::SKIPPED : lost ? ReadResult::LOST : ReadResult::OK, size, timestamp_ns};
}

RingBuffer::ReadOutcome RingBuffer::peek(const uint64_t seq, const uint8_t*& data) const noexcept {
  const SlotHeader* header = segment_->header_at(seq);

  // Acquire pairs with the producer's committing store, so everything it wrote to the slot is visible
  const uint64_t version = header->version.load(std::memory_order_acquire);
  if (version < committed_version(seq)) {
    return {ReadResult::NOT_READY, 0, 0};
  }
  if (version > committed_version(seq)) {
    return {ReadResult::OVERWRITTEN, 0, 0};
  }

  const uint32_t size = header->size.load(std::memory_order_relaxed);
  if (size == ABORTED_SIZE || size == LOST_SIZE) {
    return {size == ABORTED_SIZE ? ReadResult::SKIPPED : ReadResult::LOST, 0, 0};
  }
  data = segment_->payload_at(seq);
  return {ReadResult::OK, std::min(size, slot_size_bytes_), header->publish_timestamp_ns.load(std::memory_order_relaxed)};
}

bool RingBuffer::is_ready(const uint64_t seq) const noexcept {
  return segment_->header_at(seq)->version.load(std::memory_order_seq_cst) >= committed_version(seq);
}
} // namespace redline::shm
