#include "segment.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

#include "log.hpp"

#include "layout.hpp"
#include "redline/config.hpp"
#include "redline/utils.hpp"

namespace redline::shm {
namespace {
constexpr uint64_t align_up(uint64_t n, uint64_t a) {
  return (n + a - 1) & ~(a - 1);
}

constexpr uint32_t MAX_BUS_SIZE = 1u << 30;
constexpr uint32_t MAX_MESSAGE_SIZE = 1u << 30;
constexpr uint32_t MAX_SUBSCRIBERS = 1u << 16;

struct Geometry {
  uint64_t slot_stride_bytes;
  uint64_t subscribers_offset;
  uint64_t slots_offset;
  uint64_t total_bytes;
};

// All arithmetic in 64 bits so large buses can't silently overflow into a too-small segment
Geometry geometry_for(const Config& config) {
  Geometry g{};
  g.slot_stride_bytes = align_up(SLOT_HEADER_SIZE + uint64_t(config.max_message_size), CACHE_LINE_SIZE);
  g.subscribers_offset = align_up(sizeof(SHMSegment), CACHE_LINE_SIZE);
  g.slots_offset = g.subscribers_offset + sizeof(SubscriberEntry) * uint64_t(config.max_subscribers);
  g.total_bytes = g.slots_offset + g.slot_stride_bytes * uint64_t(config.bus_size);
  return g;
}

std::string errno_string() {
  return std::strerror(errno);
}
}

void SegmentMapping::validate_config(const Config& config) {
  if (config.bus_size < 2 || config.bus_size > MAX_BUS_SIZE || (config.bus_size & (config.bus_size - 1)) != 0) {
    throw std::invalid_argument("bus_size must be a power of two between 2 and 2^30, got " + std::to_string(config.bus_size));
  }
  if (config.max_message_size == 0 || config.max_message_size > MAX_MESSAGE_SIZE) {
    throw std::invalid_argument("max_message_size must be between 1 and 2^30, got " + std::to_string(config.max_message_size));
  }
  if (config.max_subscribers == 0 || config.max_subscribers > MAX_SUBSCRIBERS) {
    throw std::invalid_argument("max_subscribers must be between 1 and " + std::to_string(MAX_SUBSCRIBERS) + ", got " + std::to_string(config.max_subscribers));
  }
  if (config.backpressure_mode != BackPressureMode::RELIABLE && config.backpressure_mode != BackPressureMode::BEST_EFFORT) {
    throw std::invalid_argument("backpressure_mode is not a valid BackPressureMode");
  }
  if (config.zero_copy_reads && config.backpressure_mode != BackPressureMode::RELIABLE) {
    throw std::invalid_argument("zero_copy_reads requires RELIABLE mode, since BEST_EFFORT publishers may overwrite a message mid-read");
  }
  if (config.latency_histogram_significant_figures < 1 || config.latency_histogram_significant_figures > 5) {
    throw std::invalid_argument("latency_histogram_significant_figures must be between 1 and 5");
  }
  if (config.consumer_busy_spin_duration_us < 0 || config.subscriber_timeout_us < 0 || config.init_timeout_ms < 0) {
    throw std::invalid_argument("durations and timeouts must not be negative");
  }
}

std::shared_ptr<SegmentMapping>
SegmentMapping::open_or_create(const std::string &name, const Config& config) {
  validate_config(config);
  return std::shared_ptr<SegmentMapping>(new SegmentMapping(name, config));
}

SegmentMapping::~SegmentMapping() {
  unmap();
}

void SegmentMapping::unmap() noexcept {
  if (base_) {
    munmap(base_, mapped_bytes_);
    base_ = nullptr;
    segment_ = nullptr;
    mapped_bytes_ = 0;
  }
  if (shm_fd_ >= 0) {
    close(shm_fd_);
    shm_fd_ = -1;
  }
}

SegmentMapping::SegmentMapping(const std::string &name, const Config& config) : name_(name) {
  const int64_t deadline_ms = now_us() / 1000 + config.init_timeout_ms;

  while (true) {
    // Whoever manages to create the segment exclusively initialises it, everyone else opens it
    shm_fd_ = shm_open(name_.c_str(), O_RDWR | O_CREAT | O_EXCL, config.shm_permissions);
    if (shm_fd_ >= 0) {
      create(config);
      return;
    }
    if (errno != EEXIST) {
      throw std::runtime_error("Failed to create shared memory segment at " + name_ + ": " + errno_string());
    }

    if (open_existing(config)) {
      return;
    }

    // The segment vanished between our two shm_opens (e.g. its creator failed and removed it), so try again
    if (now_us() / 1000 > deadline_ms) {
      throw std::runtime_error("Timed out creating or opening shared memory segment at " + name_);
    }
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
}

void SegmentMapping::create(const Config& config) {
  const Geometry g = geometry_for(config);

  // Any failure from here on must remove the half-made segment, or every later open would wait on it forever
  const auto fail = [&](const std::string& what) {
    const std::string reason = errno_string();
    unmap();
    shm_unlink(name_.c_str());
    throw std::runtime_error(what + " " + name_ + ": " + reason);
  };

  // shm_open applies the umask, so set the requested permissions explicitly
  if (fchmod(shm_fd_, config.shm_permissions) == -1) {
    fail("Failed to set permissions on shared memory segment");
  }

  // ftruncate zero-fills, which leaves every slot at version 0 (empty) and every subscriber entry EMPTY
  if (ftruncate(shm_fd_, static_cast<off_t>(g.total_bytes)) == -1) {
    fail("Failed to size shared memory segment");
  }

  base_ = mmap(nullptr, g.total_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd_, 0);
  if (base_ == MAP_FAILED) {
    base_ = nullptr;
    fail("Failed to map shared memory segment");
  }
  mapped_bytes_ = g.total_bytes;

  segment_ = reinterpret_cast<SHMSegment*>(base_);
  segment_->init_state.store(uint32_t(SegmentInitState::INITIALISING), std::memory_order_relaxed);

  segment_->magic = MAGIC;
  segment_->version = VERSION;
  segment_->segment_header_bytes = sizeof(SHMSegment);
  segment_->subscriber_entry_bytes = sizeof(SubscriberEntry);
  segment_->slot_count = config.bus_size;
  segment_->slot_size_bytes = config.max_message_size;
  segment_->subscriber_count = config.max_subscribers;
  segment_->subscribers_offset = g.subscribers_offset;
  segment_->slot_stride_bytes = g.slot_stride_bytes;
  segment_->slot_mask = config.bus_size - 1;
  segment_->slots_offset = g.slots_offset;
  segment_->total_bytes = g.total_bytes;

  subscribers_ = reinterpret_cast<SubscriberEntry*>(static_cast<uint8_t*>(base_) + g.subscribers_offset);
  subscriber_count_ = config.max_subscribers;
  slots_ = static_cast<uint8_t*>(base_) + g.slots_offset;
  slot_mask_ = segment_->slot_mask;
  slot_stride_ = g.slot_stride_bytes;

  // Publishes all of the above to processes waiting in open_existing
  segment_->init_state.store(uint32_t(SegmentInitState::INITIALISED), std::memory_order_release);

  REDLINE_LOG(INFO) << "Created new shared memory segment at " << name_;
}

bool SegmentMapping::open_existing(const Config& config) {
  shm_fd_ = shm_open(name_.c_str(), O_RDWR, 0);
  if (shm_fd_ < 0) {
    if (errno == ENOENT) return false;
    throw std::runtime_error("Failed to open existing shared memory segment at " + name_ + ": " + errno_string());
  }

  const auto fail = [&](const std::string& what) {
    unmap();
    throw std::runtime_error(what);
  };

  const int64_t deadline_ms = now_us() / 1000 + config.init_timeout_ms;
  const auto wait_or_fail = [&](const std::string& what) {
    if (now_us() / 1000 > deadline_ms) {
      fail("Timed out waiting for shared memory segment at " + name_ + " to be initialised (" + what + "). "
           "Its creator may have crashed; remove it with redline::destroy_bus.");
    }
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  };

  // The creator may not have sized the segment yet, and touching an unsized mapping would SIGBUS
  struct stat st{};
  while (true) {
    if (fstat(shm_fd_, &st) == -1) fail("Failed to stat shared memory segment at " + name_ + ": " + errno_string());
    if (uint64_t(st.st_size) >= sizeof(SHMSegment)) break;
    wait_or_fail("segment has not been sized");
  }

  base_ = mmap(nullptr, sizeof(SHMSegment), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd_, 0);
  if (base_ == MAP_FAILED) {
    base_ = nullptr;
    fail("Failed to map existing shared memory segment at " + name_ + ": " + errno_string());
  }
  mapped_bytes_ = sizeof(SHMSegment);
  segment_ = reinterpret_cast<SHMSegment*>(base_);

  while (segment_->init_state.load(std::memory_order_acquire) != uint32_t(SegmentInitState::INITIALISED)) {
    wait_or_fail("segment header is not initialised");
  }

  const auto mismatch = [&](const std::string& field, uint64_t expected, uint64_t actual) {
    fail("Shared memory segment " + field + " mismatch at " + name_ + " (expected " + std::to_string(expected) +
         ", got " + std::to_string(actual) + ")");
  };

  if (segment_->magic != MAGIC) mismatch("magic", MAGIC, segment_->magic);
  if (segment_->version != VERSION) mismatch("version", VERSION, segment_->version);
  if (segment_->segment_header_bytes != sizeof(SHMSegment)) mismatch("header size", sizeof(SHMSegment), segment_->segment_header_bytes);
  if (segment_->subscriber_entry_bytes != sizeof(SubscriberEntry)) mismatch("subscriber entry size", sizeof(SubscriberEntry), segment_->subscriber_entry_bytes);
  if (segment_->slot_size_bytes != config.max_message_size) mismatch("slot size", config.max_message_size, segment_->slot_size_bytes);
  if (segment_->slot_count != config.bus_size) mismatch("slot count", config.bus_size, segment_->slot_count);
  if (segment_->subscriber_count != config.max_subscribers) mismatch("max subscribers", config.max_subscribers, segment_->subscriber_count);

  const Geometry g = geometry_for(config);
  if (segment_->total_bytes != g.total_bytes || segment_->slot_stride_bytes != g.slot_stride_bytes ||
      segment_->slots_offset != g.slots_offset ||
      segment_->subscribers_offset != g.subscribers_offset || uint64_t(st.st_size) < g.total_bytes) {
    fail("Shared memory segment at " + name_ + " has an unexpected size");
  }

  // Now the header has been validated, map the whole segment
  munmap(base_, mapped_bytes_);
  base_ = mmap(nullptr, g.total_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd_, 0);
  if (base_ == MAP_FAILED) {
    base_ = nullptr;
    fail("Failed to map existing shared memory segment at " + name_ + ": " + errno_string());
  }
  mapped_bytes_ = g.total_bytes;
  segment_ = reinterpret_cast<SHMSegment*>(base_);

  subscribers_ = reinterpret_cast<SubscriberEntry*>(static_cast<uint8_t*>(base_) + g.subscribers_offset);
  subscriber_count_ = config.max_subscribers;
  slots_ = static_cast<uint8_t*>(base_) + g.slots_offset;
  slot_mask_ = segment_->slot_mask;
  slot_stride_ = g.slot_stride_bytes;

  REDLINE_LOG(INFO) << "Opened existing shared memory segment at " << name_;
  return true;
}
}
