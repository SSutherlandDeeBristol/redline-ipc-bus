#pragma once

#include <array>
#include <atomic>
#include <cstdint>

namespace redline {
namespace shm {

static constexpr uint64_t MAGIC = 0x123ff123;
static constexpr uint64_t VERSION = 0;
static constexpr uint64_t CACHE_LINE_SIZE = 64;

enum class SegmentInitState : uint32_t { UNINITIALISED, INITIALISING, INITIALISED };
enum class SubscriberState : uint32_t { EMPTY, INITIALISING, OK, DEAD };
enum class SlotState : uint32_t { EMPTY, WRITING, COMMITTED };

struct alignas(CACHE_LINE_SIZE) SubscriberEntry {
  std::atomic_uint32_t state = uint32_t(SlotState::EMPTY);
  uint64_t pid; // Process ID
  std::atomic_uint64_t read_seq = 0;
  int64_t last_read_timestamp = 0;
  alignas(CACHE_LINE_SIZE) std::atomic_uint64_t dropped_count = 0;
  std::atomic_uint64_t messages_consumed = 0;
  std::atomic_uint64_t max_lag = 0;
  std::atomic_int64_t last_latency_ns = 0;
};
static_assert(sizeof(SubscriberEntry) % CACHE_LINE_SIZE == 0);

struct alignas(CACHE_LINE_SIZE) SlotHeader {
  std::atomic_uint32_t state = uint32_t(SlotState::EMPTY);
  uint32_t size = 0;                   // actual message size in bytes
  uint64_t seq = 0;                    // written seq for this slot
  int64_t publish_timestamp_ns = 0;    // real timestamp when published
  uint8_t padding[64 - 4 - 4 - 8 - 8];
};
static_assert(alignof(SlotHeader) >= CACHE_LINE_SIZE);
static_assert(sizeof(SlotHeader) % CACHE_LINE_SIZE == 0);

struct alignas(CACHE_LINE_SIZE) SHMSegment {
  uint64_t magic = MAGIC;
  uint64_t version = VERSION;
  uint64_t reserved = 0;

  std::atomic_uint32_t init_state = 0;

  uint32_t slot_count = 1024; // TODO: make configurable
  uint32_t slot_size_bytes = 4096; // TODO: make configurable
  uint64_t slot_headers_offset = 0;
  uint64_t slot_data_offset = 0;
  uint64_t total_bytes = 0;

  alignas(CACHE_LINE_SIZE) std::atomic_uint64_t write_seq = 0;
  alignas(CACHE_LINE_SIZE) std::atomic_uint32_t write_seq_futex = 0;

  std::array<SubscriberEntry, 256> subscribers; // TODO: make configurable
};
static_assert(alignof(SHMSegment) >= CACHE_LINE_SIZE);
}
}