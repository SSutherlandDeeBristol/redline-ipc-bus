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
  seqlock_copy_to_slot(payload, data.data(), data.size());
  commit(seq, uint32_t(data.size()), timestamp_ns);
  return true;
}

uint8_t* RingBuffer::begin_write(const uint64_t seq) noexcept {
  SlotHeader* header = segment_->header_at(seq);

  // Claim the slot by moving its version to odd. Only one producer can do this at a time, so two producers a lap
  // apart can never interleave their writes.
  uint64_t version = header->version.load(std::memory_order_relaxed);
  while (true) {
    if (version >= writing_version(seq)) {
      return nullptr; // a later lap got here first
    }
    if (version & 1) {
      // An older lap is still writing this slot
      cpu_relax();
      version = header->version.load(std::memory_order_relaxed);
      continue;
    }
    if (header->version.compare_exchange_weak(version, writing_version(seq), std::memory_order_relaxed)) {
      break;
    }
  }
  // Keep the payload writes that follow from becoming visible before the odd version
  std::atomic_thread_fence(std::memory_order_release);
  return segment_->payload_at(seq);
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
  const uint32_t size = aborted ? 0 : std::min(raw_size, slot_size_bytes_);
  const int64_t timestamp_ns = header->publish_timestamp_ns.load(std::memory_order_relaxed);
  seqlock_copy_from_slot(out, segment_->payload_at(seq), size);

  std::atomic_thread_fence(std::memory_order_acquire);
  if (header->version.load(std::memory_order_relaxed) != version) {
    return {ReadResult::OVERWRITTEN, 0, 0};
  }

  return {aborted ? ReadResult::SKIPPED : ReadResult::OK, size, timestamp_ns};
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
  if (size == ABORTED_SIZE) {
    return {ReadResult::SKIPPED, 0, 0};
  }
  data = segment_->payload_at(seq);
  return {ReadResult::OK, std::min(size, slot_size_bytes_), header->publish_timestamp_ns.load(std::memory_order_relaxed)};
}

bool RingBuffer::is_ready(const uint64_t seq) const noexcept {
  return segment_->header_at(seq)->version.load(std::memory_order_seq_cst) >= committed_version(seq);
}
} // namespace redline::shm
