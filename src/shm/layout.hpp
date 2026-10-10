#pragma once

#include <atomic>
#include <cstdint>

namespace redline {
namespace shm {

static constexpr uint64_t MAGIC = 0x123ff123;
// Bump whenever the layout of anything in this file changes. Segments created by a build with a different version are
// rejected rather than misinterpreted.
static constexpr uint64_t VERSION = 4;
static constexpr uint64_t CACHE_LINE_SIZE = 64;

enum class SegmentInitState : uint32_t { UNINITIALISED, INITIALISING, INITIALISED };

// EMPTY -> INITIALISING -> OK, claimed via CAS from EMPTY by whoever holds the entry's lease (see SubscriberLease).
// OK -> DEAD when the subscriber is evicted for being too slow; the owner notices, rejoins at the head and goes back to OK.
// Any state -> EMPTY when the owner deregisters, or when nobody holds the lease any more because the owner has exited.
enum class SubscriberState : uint32_t { EMPTY, INITIALISING, OK, DEAD };

// SubscriberEntry::flags
static constexpr uint32_t SUBSCRIBER_NO_EVICT = 1; // reads slots in place, so must never be overwritten mid-read
static constexpr uint32_t SUBSCRIBER_RELIABLE = 2; // publishers never overwrite a message this subscriber hasn't read

// Pid is informational only. Whether an entry's owner is still alive is decided by its lease, which works across pid
// namespaces.
struct alignas(CACHE_LINE_SIZE) SubscriberEntry {
  // Line 1: read by publishers computing backpressure
  std::atomic_uint32_t state = uint32_t(SubscriberState::EMPTY);
  std::atomic_int32_t pid = 0;
  std::atomic_uint64_t read_seq = 0;      // next sequence this subscriber will read
  std::atomic_int64_t heartbeat_ns = 0;   // CLOCK_MONOTONIC, refreshed by the subscriber's poll loop
  std::atomic_uint32_t flags = 0;         // SUBSCRIBER_* flags, set before the entry becomes OK

  // Line 2: metrics, only written by the owning poll thread
  alignas(CACHE_LINE_SIZE) std::atomic_uint64_t dropped_count = 0;
  std::atomic_uint64_t messages_consumed = 0;
  std::atomic_uint64_t max_lag = 0;
  std::atomic_int64_t last_latency_ns = 0;
};
static_assert(sizeof(SubscriberEntry) % CACHE_LINE_SIZE == 0);

// Each slot is a SlotHeader immediately followed by its payload, so small messages share a cache line with their header.
//
// `version` is a per-slot seqlock: while the producer of sequence s is writing it holds 2s+1, once committed it holds 2s+2.
// A reader for sequence s therefore sees:
//   version <  2s+1  -> not written yet (slot still holds an older lap)
//   version == 2s+1  -> being written
//   version == 2s+2  -> committed, safe to copy (re-check version after copying)
//   version >  2s+2  -> overwritten by a later lap
// Only the producer that moved the version to odd may write the slot, so two producers can never write the same slot at once.
struct SlotHeader {
  std::atomic_uint64_t version = 0;
  std::atomic_uint32_t size = 0;                  // payload size in bytes
  std::atomic_int64_t publish_timestamp_ns = 0;   // CLOCK_MONOTONIC
};
static constexpr uint64_t SLOT_HEADER_SIZE = 32;
static_assert(sizeof(SlotHeader) <= SLOT_HEADER_SIZE);

// SlotHeader::size of a sequence whose producer gave it up (an aborted Claim): subscribers skip it
static constexpr uint32_t ABORTED_SIZE = UINT32_MAX;

constexpr uint64_t writing_version(uint64_t seq) { return 2 * seq + 1; }
constexpr uint64_t committed_version(uint64_t seq) { return 2 * seq + 2; }

struct alignas(CACHE_LINE_SIZE) SHMSegment {
  uint64_t magic = MAGIC;
  uint64_t version = VERSION;
  uint32_t segment_header_bytes = sizeof(SHMSegment); // sanity check against layout drift between builds
  uint32_t subscriber_entry_bytes = sizeof(SubscriberEntry);

  std::atomic_uint32_t init_state = 0;

  uint32_t slot_count = 0;           // always a power of two
  uint32_t slot_size_bytes = 0;      // max payload size
  uint32_t subscriber_count = 0;     // entries in the subscriber registry
  uint64_t subscribers_offset = 0;   // SubscriberEntry[subscriber_count] lives here, cache line aligned
  uint64_t slot_stride_bytes = 0;    // header + payload, cache line aligned
  uint64_t slot_mask = 0;
  uint64_t slots_offset = 0;
  uint64_t total_bytes = 0;

  // One past the highest registry index ever claimed, so scans of the registry can stop early
  std::atomic_uint32_t subscriber_high_water = 0;

  // Next sequence to be reserved by a producer. Slots below this may still be being written.
  alignas(CACHE_LINE_SIZE) std::atomic_uint64_t write_seq = 0;

  // Sleeping subscribers register in `waiters` then futex-wait on `commit_futex`. Producers only bump and wake
  // `commit_futex` when `waiters` is non-zero, so the publish fast path makes no syscalls.
  alignas(CACHE_LINE_SIZE) std::atomic_uint32_t waiters = 0;
  alignas(CACHE_LINE_SIZE) std::atomic_uint32_t commit_futex = 0;
};
static_assert(alignof(SHMSegment) >= CACHE_LINE_SIZE);
}
}
