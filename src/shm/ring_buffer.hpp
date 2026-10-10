#pragma once

#include <cstdint>
#include <memory>
#include <span>

#include "segment.hpp"

namespace redline::shm {
class RingBuffer {
public:
  enum class ReadResult : uint32_t {
    OK,          // message copied out intact
    NOT_READY,   // not published yet, or still being written
    OVERWRITTEN  // a later lap has (or is about to have) replaced this message
  };

  explicit RingBuffer(std::shared_ptr<SegmentMapping> segment);

  // Writes the message for a sequence the caller has reserved from write_seq. Waits for an older lap that is still
  // mid-write on the same slot. Returns false if a later lap has already claimed the slot, in which case the message
  // is lost (only possible when producers lap each other in BEST_EFFORT mode, or with no subscribers).
  bool write(uint64_t seq, const std::span<const uint8_t>& data, int64_t timestamp_ns) noexcept;

  struct ReadOutcome {
    ReadResult result;
    uint32_t size;
    int64_t publish_timestamp_ns;
  };

  // Copies the message for `seq` into `out`, which must hold at least slot_size_bytes. The copy is validated against
  // the slot's version afterwards, so a message overwritten mid-copy is reported as OVERWRITTEN rather than returned torn.
  ReadOutcome read(uint64_t seq, uint8_t* out) const noexcept;

  // Zero-copy alternative to read(): on OK, `data` points at the message inside the slot. Only stable for as long as
  // producers can't reuse the slot, i.e. in RELIABLE mode while this subscriber's read_seq is still <= seq.
  ReadOutcome peek(uint64_t seq, const uint8_t*& data) const noexcept;

  // True once `seq` has been committed, or overwritten by a later lap (sequentially consistent, for the sleep protocol)
  bool is_ready(uint64_t seq) const noexcept;

private:
  std::shared_ptr<SegmentMapping> segment_;
  uint32_t slot_size_bytes_;
};
}
