#include "ring_buffer.hpp"

#include <cstring>

#include "glog/logging.h"

#include "layout.hpp"

namespace redline::shm {
RingBuffer::RingBuffer(std::shared_ptr<SegmentMapping> segment)
    : segment_(std::move(segment)), shm_segment_(segment_->segment()) {}

RingBuffer::PushResult RingBuffer::try_push(const std::span<const uint8_t> &data,
                                            const int64_t timestamp_us) {
  PushResult push_result{};

  if (data.size() > shm_segment_->slot_size_bytes) {
    push_result.result = WriteResult::MESSAGE_TOO_LARGE;
    return push_result;
  }

  const uint64_t seq = shm_segment_->write_seq.fetch_add(1, std::memory_order_release);
  shm_segment_->write_seq_futex.fetch_add(1, std::memory_order_release);

  SlotHeader *header = segment_->header_at(seq);
  header->state.store(uint32_t(SlotState::WRITING));
  header->seq = seq;
  header->size = uint32_t(data.size());
  header->publish_timestamp_us = timestamp_us;

  uint8_t *payload = segment_->payload_at(seq);
  memcpy(payload, data.data(), data.size());
  header->state.store(uint32_t(SlotState::COMMITTED), std::memory_order_release);

  push_result.write_seq = seq;
  push_result.result = WriteResult::OK;
  return push_result;
}

RingBuffer::PopResult RingBuffer::try_pop(const uint64_t &seq) {
  PopResult pop_result{};

  const SlotHeader *header = segment_->header_at(seq);
  pop_result.slot_state = header->state.load(std::memory_order_acquire);

  const uint64_t write_seq = shm_segment_->write_seq.load(std::memory_order_acquire);
  pop_result.write_seq_futex = shm_segment_->write_seq_futex.load(std::memory_order_acquire);

  if (pop_result.slot_state == uint32_t(SlotState::EMPTY)) {
    pop_result.result = ReadResult::EMPTY;
    return pop_result;
  }

  if (pop_result.slot_state != uint32_t(SlotState::COMMITTED)) {
    pop_result.result = ReadResult::NOT_COMMITTED_YET;
    return pop_result;
  }

  if (header->seq != seq) {
    // Producer wrapped and overwrote unread data
    if (seq + shm_segment_->slot_count <= write_seq) {
      pop_result.result = ReadResult::OVERWRITTEN;
    } else {
      // Otherwise producer hasn't published this slot yet
      pop_result.result = ReadResult::TOO_FAR_AHEAD;
    }
    return pop_result;
  }

  pop_result.data = std::span(segment_->payload_at(seq), header->size);
  pop_result.publish_timestamp_us = header->publish_timestamp_us;
  pop_result.result = ReadResult::OK;

  return pop_result;
}
} // namespace redline