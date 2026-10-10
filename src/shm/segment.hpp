#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "layout.hpp"
#include "redline/config.hpp"
#include "redline/errors.hpp"

namespace redline::shm {
class SegmentMapping {
public:
  // Creates the segment with `layout`, or opens it if it already exists, in which case its layout must match.
  // Failures are logged.
  static Result<std::shared_ptr<SegmentMapping>> open_or_create(const std::string& name, const BusLayout& layout,
                                                                const BusOptions& options);

  // Opens an existing segment, whatever its layout. Failures other than NOT_FOUND are logged.
  static Result<std::shared_ptr<SegmentMapping>> open(const std::string& name, const BusOptions& options);

  // INVALID_ARGUMENT, with the reason logged, if these can't describe a bus
  static Result<> validate(const BusLayout& layout);
  static Result<> validate(const BusOptions& options);

  ~SegmentMapping();

  SegmentMapping(const SegmentMapping&) = delete;
  SegmentMapping& operator=(const SegmentMapping&) = delete;
  SegmentMapping(SegmentMapping&&) = delete;
  SegmentMapping& operator=(SegmentMapping&&) = delete;

  SHMSegment* segment() const noexcept { return segment_; }
  const std::string& name() const noexcept { return name_; }
  const BusLayout& layout() const noexcept { return layout_; }
  int fd() const noexcept { return shm_fd_; }

  // Serialises lease probes made through fd(): locks taken through one file description don't conflict with each
  // other, so two threads probing through it at once could both believe they hold the same entry's lease
  std::mutex& probe_mutex() noexcept { return probe_mutex_; }

  uint32_t subscriber_count() const noexcept { return layout_.max_subscribers; }
  SubscriberEntry& subscriber(uint32_t index) const noexcept { return subscribers_[index]; }
  uint32_t publisher_count() const noexcept { return layout_.max_publishers; }
  PublisherEntry& publisher(uint32_t index) const noexcept { return publishers_[index]; }

  SlotHeader* header_at(uint64_t seq) const noexcept {
    return reinterpret_cast<SlotHeader*>(slots_ + (seq & slot_mask_) * slot_stride_);
  }
  uint8_t* payload_at(uint64_t seq) const noexcept {
    return slots_ + (seq & slot_mask_) * slot_stride_ + SLOT_HEADER_SIZE;
  }

private:
  explicit SegmentMapping(std::string name) : name_(std::move(name)) {}

  enum class Opened { YES, VANISHED };

  Result<> create(const BusLayout& layout, const BusOptions& options);
  // With no expected layout, takes it from the segment. VANISHED if the segment was removed before we opened it.
  Result<Opened> open_existing(const std::optional<BusLayout>& expected, const BusOptions& options);
  Result<> map_all(const BusOptions& options);
  void unmap() noexcept;

  std::string name_;
  BusLayout layout_;
  int shm_fd_ = -1;
  void* base_ = nullptr;
  uint64_t mapped_bytes_ = 0;

  SHMSegment* segment_ = nullptr;
  SubscriberEntry* subscribers_ = nullptr;
  PublisherEntry* publishers_ = nullptr;
  uint8_t* slots_ = nullptr;
  uint64_t slot_mask_ = 0;
  uint64_t slot_stride_ = 0;

  std::mutex probe_mutex_;
};
}
