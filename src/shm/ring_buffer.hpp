#pragma once

#include <span>

#include "segment.hpp"

namespace redline::shm {
struct SlotHeader;

class RingBuffer {
public:
  enum class WriteResult : uint32_t { OK, MESSAGE_TOO_LARGE };
  enum class ReadResult : uint32_t { OK, EMPTY, NOT_COMMITTED_YET, OVERWRITTEN, TOO_FAR_AHEAD };

  explicit RingBuffer(std::shared_ptr<SegmentMapping> segment_);

  struct PushResult {
    WriteResult result;
    uint64_t write_seq;
  };

  PushResult try_push(const std::span<const uint8_t> & data);

  struct PopResult {
    ReadResult result;
    std::span<const uint8_t> data;
    uint32_t slot_state;
    uint32_t write_seq_futex;
  };
  PopResult try_pop(const uint64_t &seq);

private:
  std::shared_ptr<SegmentMapping> segment_;
  SHMSegment* shm_segment_;
};
}