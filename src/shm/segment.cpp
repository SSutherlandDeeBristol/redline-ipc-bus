#include "segment.hpp"

#include <fcntl.h>
#include <iostream>
#include <sys/mman.h>

#include "glog/logging.h"

#include "layout.hpp"
#include "redline/config.hpp"

namespace redline::shm {
namespace {
constexpr std::size_t align_up(std::size_t n, std::size_t a) {
  return (n + a - 1) & ~(a - 1);
}
}

std::shared_ptr<SegmentMapping>
SegmentMapping::open_or_create(const std::string &name, const Config& config) {
  return std::shared_ptr<SegmentMapping>(new SegmentMapping(name, config));
}

SegmentMapping::~SegmentMapping() {
  if (base_ && segment_->total_bytes > 0) {
    munmap(base_, segment_->total_bytes);
  }
  if (shm_fd_ >= 0) {
    close(shm_fd_);
  }
}

SegmentMapping::SegmentMapping(const std::string &name, const Config& config) {
  name_ = name;

  const uint64_t slot_headers_offset = align_up(sizeof(SHMSegment), 64);
  const uint64_t slot_data_offset    = align_up(slot_headers_offset + config.bus_size * sizeof(SlotHeader), 64);
  const uint64_t total_bytes         = slot_data_offset + (config.bus_size * config.max_message_size);

  // Attempt to open the shared memory file, creating it if it doesn't exist
  shm_fd_ = shm_open(name_.c_str(), O_RDWR | O_CREAT | O_EXCL, 0666); // TODO: check file mode
  if (shm_fd_ >= 0) {
    // success in creating the segment so initialise it properly...
    int r = ftruncate(shm_fd_, static_cast<off_t>(total_bytes));
    if (r == -1) {
      close(shm_fd_);
      throw std::runtime_error("Failed to create shared memory segment at " + name_);
    }

    base_ = mmap(nullptr, total_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd_, 0);
    if (base_ == MAP_FAILED) {
      close(shm_fd_);
      throw std::runtime_error("Failed to map shared memory segment at " + name_);
    }

    LOG(INFO) << "Initialising new shared memory segment at " << name_;

    segment_ = reinterpret_cast<SHMSegment*>(base_);
    segment_->init_state.store(uint32_t(SegmentInitState::INITIALISING), std::memory_order_acquire);

    // Initialise the rest of the segment
    segment_->magic = MAGIC;
    segment_->version = VERSION;
    segment_->slot_count = config.bus_size;
    segment_->slot_size_bytes = config.max_message_size;
    segment_->slot_headers_offset = slot_headers_offset;
    segment_->slot_data_offset = slot_data_offset;
    segment_->total_bytes = total_bytes;
    segment_->write_seq.store(0);

    for (auto& subscriber : segment_->subscribers) {
      subscriber.state.store(uint32_t(SubscriberState::EMPTY));
      subscriber.pid = 0;
      subscriber.read_seq = 0;
      subscriber.last_read_timestamp = 0;
    }

    // TODO: initialise slots to be empty...

    // Mark the segment as initialised for other processes
    segment_->init_state.store(uint32_t(SegmentInitState::INITIALISED), std::memory_order_release);

    LOG(INFO) << "Created new shared memory segment at " << name_;
  } else if (errno == EEXIST) {
    LOG(INFO) << "Opening existing shared memory segment at " << name_;

    // already exists, so open it normally
    shm_fd_ = shm_open(name_.c_str(), O_RDWR, 0666);
    if (shm_fd_ < 0) {
      throw std::runtime_error("Failed to open existing shared memory segment at " + name_);
    }

    base_ = mmap(nullptr, total_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd_, 0);
    if (base_ == MAP_FAILED) {
      close(shm_fd_);
      throw std::runtime_error("Failed to map existing shared memory segment at " + name_);
    }

    segment_ = reinterpret_cast<SHMSegment*>(base_);

    // Check that the segment is initialised and potentially wait...
    while (segment_->init_state.load(std::memory_order_acquire) != uint32_t(SegmentInitState::INITIALISED)) {
      // Busy wait - could use futexes or similar for better efficiency
    }

    const uint64_t actual_version = segment_->version;
    const uint64_t actual_magic = segment_->magic;
    const uint32_t actual_slot_size = segment_->slot_size_bytes;
    const uint32_t actual_slot_count = segment_->slot_count;

    if (actual_version != VERSION) {
      munmap(base_, sizeof(SHMSegment));
      close(shm_fd_);

      throw std::runtime_error("Shared memory segment version mismatch at " + name_ + " (expected " +
                               std::to_string(VERSION) + ", got " +
                               std::to_string(actual_version) + ")");
    }

    if (actual_magic != MAGIC) {
      munmap(base_, sizeof(SHMSegment));
      close(shm_fd_);

      throw std::runtime_error("Shared memory segment magic mismatch at " + name_ + " (expected " +
                               std::to_string(MAGIC) + ", got " +
                               std::to_string(actual_magic) + ")");
    }

    if (actual_slot_size != config.max_message_size) {
      munmap(base_, sizeof(SHMSegment));
      close(shm_fd_);

      throw std::runtime_error("Shared memory segment slot size mismatch at " + name_ + " (expected " +
                               std::to_string(config.max_message_size) + ", got " +
                               std::to_string(actual_slot_size) + ")");
    }

    if (actual_slot_count != config.bus_size) {
      munmap(base_, sizeof(SHMSegment));
      close(shm_fd_);

      throw std::runtime_error("Shared memory segment slot count mismatch at " + name_ + " (expected " +
                               std::to_string(config.bus_size) + ", got " +
                               std::to_string(actual_slot_count) + ")");
    }

     // TODO: return a proper error in case of version or magic mismatch
    LOG(INFO) << "Opened existing shared memory segment at " << name_;
  } else {
    throw std::runtime_error("Failed to create or open shared memory segment at " + name_);
  }
}

SlotHeader* SegmentMapping::header_at(uint64_t seq) const {
  const uint64_t index = seq % segment_->slot_count; // TODO: use bitmask if slot count is a power of 2 for better performance
  return reinterpret_cast<SlotHeader*>(reinterpret_cast<uint8_t*>(segment_) +
                                        segment_->slot_headers_offset + (index * sizeof(SlotHeader)));
}

uint8_t* SegmentMapping::payload_at(uint64_t seq) const {
  const uint64_t index = seq % segment_->slot_count;
  return reinterpret_cast<uint8_t*>(segment_) + segment_->slot_data_offset + (index * segment_->slot_size_bytes);
}
}