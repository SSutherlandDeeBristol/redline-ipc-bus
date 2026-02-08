#pragma once

#include <memory>
#include <string>

namespace redline {
struct Config;
}

namespace redline::shm {
struct SHMSegment;
struct SlotHeader;

class SegmentMapping {
public:
  static std::shared_ptr<SegmentMapping> open_or_create(const std::string& name, const Config& config);

  ~SegmentMapping();

  SegmentMapping(const SegmentMapping&) = delete;
  SegmentMapping& operator=(const SegmentMapping&) = delete;
  SegmentMapping(SegmentMapping&&) = delete;
  SegmentMapping& operator=(SegmentMapping&&) = delete;

  SHMSegment* segment() noexcept { return segment_; }
  std::string name() noexcept { return name_; }

  SlotHeader* header_at(uint64_t seq) const;
  uint8_t*    payload_at(uint64_t seq) const;

private:
  SegmentMapping(const std::string& name, const Config& config);

  std::string name_;
  int shm_fd_ = -1;
  void* base_ = nullptr;

  SHMSegment* segment_ = nullptr;
};
}