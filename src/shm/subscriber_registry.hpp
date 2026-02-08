#pragma once

#include "segment.hpp"

namespace redline::shm {

enum class SubscribeResult : uint32_t { OK, REGISTRY_FULL };

class SubscriberRegistry {
public:
  SubscriberRegistry(std::shared_ptr<SegmentMapping> segment);

  SubscribeResult register_subscriber(uint64_t& out_index);
  void remove_subscriber(uint64_t index);

private:
  std::shared_ptr<SegmentMapping> segment_;
  SHMSegment* shm_segment_;
};
}