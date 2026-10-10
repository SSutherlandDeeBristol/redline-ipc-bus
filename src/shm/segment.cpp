#include "segment.hpp"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

#include "log.hpp"

#include "layout.hpp"
#include "redline/utils.hpp"

namespace redline::shm {
namespace {
constexpr uint64_t align_up(uint64_t n, uint64_t a) {
  return (n + a - 1) & ~(a - 1);
}

constexpr uint32_t MAX_BUS_SIZE = 1u << 30;
constexpr uint32_t MAX_MESSAGE_SIZE = 1u << 30;

struct Geometry {
  uint64_t slot_stride_bytes;
  uint64_t subscribers_offset;
  uint64_t publishers_offset;
  uint64_t slots_offset;
  uint64_t total_bytes;
};

// All arithmetic in 64 bits so large buses can't silently overflow into a too-small segment
Geometry geometry_for(const BusLayout& layout) {
  Geometry g{};
  g.slot_stride_bytes = align_up(SLOT_HEADER_SIZE + uint64_t(layout.max_message_size), CACHE_LINE_SIZE);
  g.subscribers_offset = align_up(sizeof(SHMSegment), CACHE_LINE_SIZE);
  g.publishers_offset = g.subscribers_offset + sizeof(SubscriberEntry) * uint64_t(layout.max_subscribers);
  g.slots_offset = g.publishers_offset + sizeof(PublisherEntry) * uint64_t(layout.max_publishers);
  g.total_bytes = g.slots_offset + g.slot_stride_bytes * uint64_t(layout.bus_size);
  return g;
}

std::string errno_string() {
  return std::strerror(errno);
}

std::unexpected<Error> fail(const Error error, const std::string& message) {
  REDLINE_LOG(ERROR) << message;
  return std::unexpected(error);
}

// Why `layout` can't describe a bus, or empty if it can
std::string layout_problem(const BusLayout& layout) {
  if (layout.bus_size < 2 || layout.bus_size > MAX_BUS_SIZE || (layout.bus_size & (layout.bus_size - 1)) != 0) {
    return "bus_size must be a power of two between 2 and 2^30, got " + std::to_string(layout.bus_size);
  }
  if (layout.max_message_size == 0 || layout.max_message_size > MAX_MESSAGE_SIZE) {
    return "max_message_size must be between 1 and 2^30, got " + std::to_string(layout.max_message_size);
  }
  if (layout.max_subscribers == 0 || layout.max_subscribers > MAX_REGISTRY_ENTRIES) {
    return "max_subscribers must be between 1 and " + std::to_string(MAX_REGISTRY_ENTRIES) + ", got " +
           std::to_string(layout.max_subscribers);
  }
  if (layout.max_publishers == 0 || layout.max_publishers > MAX_REGISTRY_ENTRIES) {
    return "max_publishers must be between 1 and " + std::to_string(MAX_REGISTRY_ENTRIES) + ", got " +
           std::to_string(layout.max_publishers);
  }
  return {};
}

std::string describe(const BusLayout& layout) {
  return "max_message_size=" + std::to_string(layout.max_message_size) + " bus_size=" + std::to_string(layout.bus_size) +
         " max_subscribers=" + std::to_string(layout.max_subscribers) +
         " max_publishers=" + std::to_string(layout.max_publishers);
}
}

Result<> SegmentMapping::validate(const BusLayout& layout) {
  if (const auto problem = layout_problem(layout); !problem.empty()) {
    return fail(Error::INVALID_ARGUMENT, problem);
  }
  return {};
}

Result<> SegmentMapping::validate(const BusOptions& options) {
  if (options.backpressure_mode != BackPressureMode::RELIABLE && options.backpressure_mode != BackPressureMode::BEST_EFFORT) {
    return fail(Error::INVALID_ARGUMENT, "backpressure_mode is not a valid BackPressureMode");
  }
  if (options.on_lapped != OnLapped::RESUME_AT_OLDEST && options.on_lapped != OnLapped::RESUME_AT_NEWEST) {
    return fail(Error::INVALID_ARGUMENT, "on_lapped is not a valid OnLapped");
  }
  if (options.zero_copy_reads && options.backpressure_mode != BackPressureMode::RELIABLE) {
    return fail(Error::INVALID_ARGUMENT, "zero_copy_reads requires RELIABLE mode, since BEST_EFFORT publishers may "
                                         "overwrite a message mid-read");
  }
  if (options.latency_histogram_significant_figures < 1 || options.latency_histogram_significant_figures > 5) {
    return fail(Error::INVALID_ARGUMENT, "latency_histogram_significant_figures must be between 1 and 5");
  }
  if (options.consumer_busy_spin_duration_us < 0 || options.subscriber_timeout_us < 0 || options.init_timeout_ms < 0) {
    return fail(Error::INVALID_ARGUMENT, "durations and timeouts must not be negative");
  }
  if (options.poll_thread_cpu && (*options.poll_thread_cpu < 0 || *options.poll_thread_cpu >= CPU_SETSIZE)) {
    return fail(Error::INVALID_ARGUMENT, "poll_thread_cpu is not a valid CPU number");
  }
  return {};
}

Result<std::shared_ptr<SegmentMapping>> SegmentMapping::open_or_create(const std::string& name, const BusLayout& layout,
                                                                       const BusOptions& options) {
  if (auto valid = validate(layout); !valid) return std::unexpected(valid.error());
  if (auto valid = validate(options); !valid) return std::unexpected(valid.error());

  auto mapping = std::shared_ptr<SegmentMapping>(new SegmentMapping(name));
  const int64_t deadline_ns = now_ns() + options.init_timeout_ms * 1'000'000;
  while (true) {
    // Whoever manages to create the segment exclusively initialises it, everyone else opens it
    mapping->shm_fd_ = shm_open(name.c_str(), O_RDWR | O_CREAT | O_EXCL, options.shm_permissions);
    if (mapping->shm_fd_ >= 0) {
      if (auto created = mapping->create(layout, options); !created) return std::unexpected(created.error());
      return mapping;
    }
    if (errno != EEXIST) {
      return fail(Error::SYSTEM_ERROR, "Failed to create shared memory segment at " + name + ": " + errno_string());
    }

    const auto opened = mapping->open_existing(layout, options);
    if (!opened) return std::unexpected(opened.error());
    if (*opened == Opened::YES) return mapping;

    // The segment vanished between our two shm_opens (e.g. its creator failed and removed it), so try again
    if (now_ns() > deadline_ns) {
      return fail(Error::SYSTEM_ERROR, "Timed out creating or opening shared memory segment at " + name);
    }
    std::this_thread::sleep_for(std::chrono::microseconds(100));
  }
}

Result<std::shared_ptr<SegmentMapping>> SegmentMapping::open(const std::string& name, const BusOptions& options) {
  if (auto valid = validate(options); !valid) return std::unexpected(valid.error());

  auto mapping = std::shared_ptr<SegmentMapping>(new SegmentMapping(name));
  const auto opened = mapping->open_existing(std::nullopt, options);
  if (!opened) return std::unexpected(opened.error());
  if (*opened == Opened::VANISHED) return std::unexpected(Error::NOT_FOUND);
  return mapping;
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

Result<> SegmentMapping::create(const BusLayout& layout, const BusOptions& options) {
  layout_ = layout;
  const Geometry g = geometry_for(layout);

  // Any failure from here on must remove the half-made segment, or every later open would wait on it forever
  const auto fail_and_remove = [&](const std::string& what) {
    const std::string reason = errno_string();
    unmap();
    shm_unlink(name_.c_str());
    return fail(Error::SYSTEM_ERROR, what + " " + name_ + ": " + reason);
  };

  // shm_open applies the umask, so set the requested permissions explicitly
  if (fchmod(shm_fd_, options.shm_permissions) == -1) {
    return fail_and_remove("Failed to set permissions on shared memory segment");
  }

  // ftruncate zero-fills, which leaves every slot at version 0 (empty) and every registry entry EMPTY
  if (ftruncate(shm_fd_, static_cast<off_t>(g.total_bytes)) == -1) {
    return fail_and_remove("Failed to size shared memory segment");
  }

  if (!map_all(options)) {
    shm_unlink(name_.c_str());
    unmap();
    return std::unexpected(Error::SYSTEM_ERROR);
  }

  segment_->init_state.store(uint32_t(SegmentInitState::INITIALISING), std::memory_order_relaxed);
  segment_->magic = MAGIC;
  segment_->version = VERSION;
  segment_->segment_header_bytes = sizeof(SHMSegment);
  segment_->subscriber_entry_bytes = sizeof(SubscriberEntry);
  segment_->publisher_entry_bytes = sizeof(PublisherEntry);
  segment_->slot_count = layout.bus_size;
  segment_->slot_size_bytes = layout.max_message_size;
  segment_->subscriber_count = layout.max_subscribers;
  segment_->subscribers_offset = g.subscribers_offset;
  segment_->publisher_count = layout.max_publishers;
  segment_->publishers_offset = g.publishers_offset;
  segment_->slot_stride_bytes = g.slot_stride_bytes;
  segment_->slot_mask = layout.bus_size - 1;
  segment_->slots_offset = g.slots_offset;
  segment_->total_bytes = g.total_bytes;

  // Publishes all of the above to processes waiting in open_existing
  segment_->init_state.store(uint32_t(SegmentInitState::INITIALISED), std::memory_order_release);

  REDLINE_LOG(INFO) << "Created new shared memory segment at " << name_;
  return {};
}

Result<SegmentMapping::Opened> SegmentMapping::open_existing(const std::optional<BusLayout>& expected,
                                                             const BusOptions& options) {
  shm_fd_ = shm_open(name_.c_str(), O_RDWR, 0);
  if (shm_fd_ < 0) {
    if (errno == ENOENT) return Opened::VANISHED;
    return fail(Error::SYSTEM_ERROR, "Failed to open existing shared memory segment at " + name_ + ": " + errno_string());
  }

  const auto give_up = [&](const Error error, const std::string& what) {
    unmap();
    return fail(error, what);
  };

  const int64_t deadline_ns = now_ns() + options.init_timeout_ms * 1'000'000;
  const auto timed_out = [&] {
    if (now_ns() > deadline_ns) return true;
    std::this_thread::sleep_for(std::chrono::microseconds(100));
    return false;
  };
  const auto timeout_message = [&](const std::string& what) {
    return "Timed out waiting for shared memory segment at " + name_ + " to be initialised (" + what + "). "
           "Its creator may have crashed; remove it with redline::destroy_bus.";
  };

  // The creator may not have sized the segment yet, and touching an unsized mapping would SIGBUS
  struct stat st{};
  while (true) {
    if (fstat(shm_fd_, &st) == -1) {
      return give_up(Error::SYSTEM_ERROR, "Failed to stat shared memory segment at " + name_ + ": " + errno_string());
    }
    if (uint64_t(st.st_size) >= sizeof(SHMSegment)) break;
    if (timed_out()) return give_up(Error::SYSTEM_ERROR, timeout_message("segment has not been sized"));
  }

  base_ = mmap(nullptr, sizeof(SHMSegment), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd_, 0);
  if (base_ == MAP_FAILED) {
    base_ = nullptr;
    return give_up(Error::SYSTEM_ERROR, "Failed to map existing shared memory segment at " + name_ + ": " + errno_string());
  }
  mapped_bytes_ = sizeof(SHMSegment);
  segment_ = reinterpret_cast<SHMSegment*>(base_);

  while (segment_->init_state.load(std::memory_order_acquire) != uint32_t(SegmentInitState::INITIALISED)) {
    if (timed_out()) return give_up(Error::SYSTEM_ERROR, timeout_message("segment header is not initialised"));
  }

  const auto mismatch = [&](const std::string& field, uint64_t wanted, uint64_t actual) {
    return give_up(Error::INCOMPATIBLE, "Shared memory segment " + field + " mismatch at " + name_ + " (expected " +
                                            std::to_string(wanted) + ", got " + std::to_string(actual) + ")");
  };
  if (segment_->magic != MAGIC) return mismatch("magic", MAGIC, segment_->magic);
  if (segment_->version != VERSION) return mismatch("version", VERSION, segment_->version);
  if (segment_->segment_header_bytes != sizeof(SHMSegment)) {
    return mismatch("header size", sizeof(SHMSegment), segment_->segment_header_bytes);
  }
  if (segment_->subscriber_entry_bytes != sizeof(SubscriberEntry)) {
    return mismatch("subscriber entry size", sizeof(SubscriberEntry), segment_->subscriber_entry_bytes);
  }
  if (segment_->publisher_entry_bytes != sizeof(PublisherEntry)) {
    return mismatch("publisher entry size", sizeof(PublisherEntry), segment_->publisher_entry_bytes);
  }

  layout_ = BusLayout{
    .max_message_size = segment_->slot_size_bytes,
    .bus_size = segment_->slot_count,
    .max_subscribers = segment_->subscriber_count,
    .max_publishers = segment_->publisher_count,
  };
  if (expected && *expected != layout_) {
    return give_up(Error::INCOMPATIBLE, "Bus " + name_ + " already exists with a different layout (" +
                                            describe(layout_) + ", asked for " + describe(*expected) + ")");
  }
  const Geometry g = geometry_for(layout_);
  if (!layout_problem(layout_).empty() || segment_->total_bytes != g.total_bytes ||
      segment_->slot_stride_bytes != g.slot_stride_bytes || segment_->slots_offset != g.slots_offset ||
      segment_->subscribers_offset != g.subscribers_offset || segment_->publishers_offset != g.publishers_offset ||
      uint64_t(st.st_size) < g.total_bytes) {
    return give_up(Error::INCOMPATIBLE, "Shared memory segment at " + name_ + " has an unexpected size or layout");
  }

  // Now the header has been validated, map the whole segment
  munmap(base_, mapped_bytes_);
  base_ = nullptr;
  if (!map_all(options)) {
    unmap();
    return std::unexpected(Error::SYSTEM_ERROR);
  }

  REDLINE_LOG(INFO) << "Opened existing shared memory segment at " << name_;
  return Opened::YES;
}

Result<> SegmentMapping::map_all(const BusOptions& options) {
  const Geometry g = geometry_for(layout_);
  base_ = mmap(nullptr, g.total_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd_, 0);
  if (base_ == MAP_FAILED) {
    base_ = nullptr;
    return fail(Error::SYSTEM_ERROR, "Failed to map shared memory segment at " + name_ + ": " + errno_string());
  }
  mapped_bytes_ = g.total_bytes;

  auto* bytes = static_cast<uint8_t*>(base_);
  segment_ = reinterpret_cast<SHMSegment*>(base_);
  subscribers_ = reinterpret_cast<SubscriberEntry*>(bytes + g.subscribers_offset);
  publishers_ = reinterpret_cast<PublisherEntry*>(bytes + g.publishers_offset);
  slots_ = bytes + g.slots_offset;
  slot_mask_ = layout_.bus_size - 1;
  slot_stride_ = g.slot_stride_bytes;

  // Before prefaulting, so the pages are allocated huge
  if (options.huge_pages && madvise(base_, mapped_bytes_, MADV_HUGEPAGE) != 0) {
    REDLINE_LOG(WARNING) << "Failed to request huge pages for " << name_ << ": " << errno_string();
  }
  if (options.prefault) {
    // Write-faults every page without changing it. Falls back to reading each page on kernels before 5.14, which
    // also allocates them, since the mapping is shared.
    if (madvise(base_, mapped_bytes_, MADV_POPULATE_WRITE) != 0) {
      const auto page = uint64_t(sysconf(_SC_PAGESIZE));
      for (uint64_t offset = 0; offset < mapped_bytes_; offset += page) {
        (void)*static_cast<volatile uint8_t*>(bytes + offset);
      }
    }
  }
  if (options.lock_memory && mlock(base_, mapped_bytes_) != 0) {
    return fail(Error::SYSTEM_ERROR, "Failed to lock shared memory segment at " + name_ + " in memory (" +
                                         std::to_string(mapped_bytes_) + " bytes; see RLIMIT_MEMLOCK): " + errno_string());
  }
  return {};
}
}
