#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "layout.hpp"

namespace redline {
struct Config;
}

namespace redline::shm {
class SegmentMapping {
public:
  // Throws std::invalid_argument for an invalid config and std::runtime_error if the segment can't be created or opened
  static std::shared_ptr<SegmentMapping> open_or_create(const std::string& name, const Config& config);

  // Validates the config, throwing std::invalid_argument if it can't describe a bus
  static void validate_config(const Config& config);

  ~SegmentMapping();

  SegmentMapping(const SegmentMapping&) = delete;
  SegmentMapping& operator=(const SegmentMapping&) = delete;
  SegmentMapping(SegmentMapping&&) = delete;
  SegmentMapping& operator=(SegmentMapping&&) = delete;

  SHMSegment* segment() const noexcept { return segment_; }
  const std::string& name() const noexcept { return name_; }
  int fd() const noexcept { return shm_fd_; }

  // Serialises lease probes made through fd(): locks taken through one file description don't conflict with each
  // other, so two threads probing through it at once could both believe they hold the same entry's lease
  std::mutex& probe_mutex() noexcept { return probe_mutex_; }

  uint32_t subscriber_count() const noexcept { return subscriber_count_; }
  SubscriberEntry& subscriber(uint32_t index) const noexcept { return subscribers_[index]; }
  uint32_t publisher_count() const noexcept { return publisher_count_; }
  PublisherEntry& publisher(uint32_t index) const noexcept { return publishers_[index]; }

  SlotHeader* header_at(uint64_t seq) const noexcept {
    return reinterpret_cast<SlotHeader*>(slots_ + (seq & slot_mask_) * slot_stride_);
  }
  uint8_t* payload_at(uint64_t seq) const noexcept {
    return slots_ + (seq & slot_mask_) * slot_stride_ + SLOT_HEADER_SIZE;
  }

private:
  SegmentMapping(const std::string& name, const Config& config);

  void create(const Config& config);
  bool open_existing(const Config& config);
  void unmap() noexcept;

  std::string name_;
  int shm_fd_ = -1;
  void* base_ = nullptr;
  uint64_t mapped_bytes_ = 0;

  SHMSegment* segment_ = nullptr;
  SubscriberEntry* subscribers_ = nullptr;
  uint32_t subscriber_count_ = 0;
  PublisherEntry* publishers_ = nullptr;
  uint32_t publisher_count_ = 0;
  uint8_t* slots_ = nullptr;
  uint64_t slot_mask_ = 0;
  uint64_t slot_stride_ = 0;

  std::mutex probe_mutex_;
};
}
