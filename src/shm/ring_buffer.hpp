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
    OVERWRITTEN, // a later lap has (or is about to have) replaced this message
    SKIPPED,     // the producer aborted this sequence, so there is no message to deliver
    LOST         // the producer died before committing this sequence, so its message is lost
  };

  explicit RingBuffer(std::shared_ptr<SegmentMapping> segment);

  // Writes the message for a sequence the caller has reserved from write_seq. Waits for an older lap that is still
  // mid-write on the same slot. Returns false if a later lap has already claimed the slot, in which case the message
  // is lost (only possible when producers lap each other, which RELIABLE subscribers prevent).
  bool write(uint64_t seq, const std::span<const uint8_t>& data, int64_t timestamp_ns) noexcept;

  // write() in two halves, for producers that build the message in place. begin_write claims the slot for a reserved
  // `seq` and returns where its payload goes (slot_size_bytes long), or nullptr if a later lap has already claimed it.
  // Subscribers see `seq` as not ready until it is committed or aborted, which must follow.
  uint8_t* begin_write(uint64_t seq) noexcept;

  // begin_write without waiting: `payload` is null if a later lap already has the slot (`lapped`) or an older lap is
  // still writing it (`busy_with` is that lap's sequence)
  struct Begin {
    uint8_t* payload;
    bool lapped;
    uint64_t busy_with;
  };
  Begin try_begin_write(uint64_t seq) noexcept;
  void commit(uint64_t seq, uint32_t size, int64_t timestamp_ns) noexcept;
  // Copies `data` into the payload begin_write returned for `seq`, and commits it
  void fill_and_commit(uint64_t seq, uint8_t* payload, const std::span<const uint8_t>& data, int64_t timestamp_ns) noexcept;
  // Commits `seq` as a gap that subscribers skip
  void abort(uint64_t seq, int64_t timestamp_ns) noexcept { commit(seq, ABORTED_SIZE, timestamp_ns); }

  // Recovery only: the caller holds the recovery lock and knows `seq`'s publisher is dead. Commits `seq` as lost so
  // subscribers and later laps can get past it, whether or not its publisher had started writing.
  enum class FinishResult {
    FINISHED,
    NOTHING_TO_DO, // already committed, or overwritten by a later lap
    OLDER_LAP_BUSY // an older lap's publisher is still in the slot (its sequence is put in `older`)
  };
  FinishResult finish_lost(uint64_t seq, int64_t timestamp_ns, uint64_t& older) noexcept;

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
