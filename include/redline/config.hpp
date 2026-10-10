#pragma once

#include <cstdint>
#include <optional>
#include <sys/types.h>

namespace redline {

// How a subscriber copes with publishers that are faster than it. Each subscriber chooses for itself, and subscribers
// of both kinds can share a bus.
enum class BackPressureMode : uint32_t {
  RELIABLE = 0,   // Publishers wait (up to their timeout) rather than overwrite a message this subscriber hasn't read yet:
                  // nothing is lost, but a slow subscriber slows every publisher
  BEST_EFFORT = 1 // Publishers never wait for this subscriber: if it falls a full ring behind it skips ahead and counts
                  // what it missed as dropped
};

// Where a BEST_EFFORT subscriber resumes once publishers have lapped it
enum class OnLapped : uint32_t {
  RESUME_AT_OLDEST = 0, // the oldest message still in the ring, so as few messages as possible are dropped
  RESUME_AT_NEWEST = 1  // the newest message, for consumers that only need the latest value: under sustained overload
                        // they keep up with the head instead of always reading the oldest message
};

// The shape of a bus' shared memory segment, fixed when the bus is created. Every create_bus call for a bus must
// pass the same layout; open_bus reads it from the segment instead.
struct BusLayout {
  uint32_t max_message_size = 4096; // largest message in bytes, between 1 and 2^30
  uint32_t bus_size = 1024;         // message slots, a power of two between 2 and 2^30. Absorbs bursts.

  // Subscriber registry entries (between 1 and 65536), shared by every process on the bus. Each Bus with shared-thread
  // callbacks, each own_thread callback and each Subscription takes one. Costs 128 bytes of shared memory per entry.
  uint32_t max_subscribers = 256;

  // Publisher registry entries (between 1 and 65536), shared by every process on the bus. Each thread that has
  // published through a Bus holds one until it exits or the Bus is destroyed, and each open Claim holds one. Costs 64
  // bytes of shared memory per entry.
  uint32_t max_publishers = 256;

  bool operator==(const BusLayout&) const = default;
};

// Settings for one Bus object. They only affect the process using it, so processes on the same bus can differ.
struct BusOptions {
  // Defaults for this Bus' subscribers. own_thread callbacks and subscribe() can override them with SubscriberOptions.
  BackPressureMode backpressure_mode = BackPressureMode::RELIABLE;
  OnLapped on_lapped = OnLapped::RESUME_AT_OLDEST; // BEST_EFFORT only

  // Read messages in place in shared memory instead of copying them out first, saving a copy per message. RELIABLE
  // only. The slot stays reserved until the callback returns, so slow callbacks hold up publishers sooner, and these
  // subscribers are never evicted by subscriber_timeout_us.
  bool zero_copy_reads = false;

  // Measure publish-to-delivery latency (last_latency_ns and the percentiles). Costs a clock read and a histogram
  // update per message.
  bool latency_metrics = true;
  int latency_histogram_significant_figures = 3; // 1 to 5: 3 means percentiles are accurate to within 0.1%

  // How long an idle subscriber busy-spins before sleeping until a publisher wakes it
  int64_t consumer_busy_spin_duration_us = 100;

  // When this Bus' publishers are blocked by a live RELIABLE subscriber that has not made progress for this long,
  // they evict it: it skips ahead to the newest message and counts what it missed as dropped. 0 disables eviction.
  // Subscribers whose process has exited are always reclaimed, regardless of this setting.
  int64_t subscriber_timeout_us = 0;

  // Pin the thread running this Bus' shared-thread callbacks to this CPU
  std::optional<int> poll_thread_cpu = std::nullopt;

  // Memory: fault the whole segment in when mapping it, rather than on first touch; lock it in RAM (subject to
  // RLIMIT_MEMLOCK); ask for transparent huge pages, which only takes effect if
  // /sys/kernel/mm/transparent_hugepage/shmem_enabled allows it.
  bool prefault = false;
  bool lock_memory = false;
  bool huge_pages = false;

  mode_t shm_permissions = 0600; // permissions of the shared memory segment, if this process creates it
  int64_t init_timeout_ms = 1000; // how long to wait for another process to finish creating the segment
};
}
