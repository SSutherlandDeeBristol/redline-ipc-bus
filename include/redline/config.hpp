#pragma once

#include <cstdint>
#include <sys/types.h>

namespace redline {

enum class BackPressureMode : uint32_t {
  RELIABLE = 0, // Producers will wait (up to some timeout) for backpressure to clear before publishing messages, ensuring reliable delivery but potentially higher latency under load
  BEST_EFFORT = 1 // Producers will ignore backpressure and overwrite consumers that are too slow, ensuring low latency but potentially dropping messages under load
};

// All processes attaching to the same bus must use the same max_message_size, bus_size, max_subscribers and backpressure_mode,
// otherwise create_bus throws. The remaining fields are local to each process.
struct Config {
  uint32_t max_message_size                 = 4096; // in bytes, must be > 0
  uint32_t bus_size                         = 1024; // number of message slots in the bus (must be a power of two >= 2), tune this to absorb bursts of messages

  // Subscriber registry entries (between 1 and 65536), shared by every process on the bus. Each Bus with shared-thread
  // callbacks, each own_thread callback and each Subscription takes one. Costs 128 bytes of shared memory per entry.
  uint32_t max_subscribers                  = 256;

  BackPressureMode backpressure_mode        = BackPressureMode::RELIABLE; // see BackPressureMode enum

  int64_t consumer_busy_spin_duration_us    = 100; // how long to busy spin for when waiting for messages before futex waiting, in microseconds

  // RELIABLE mode only: a live subscriber that is blocking publishers and has not made progress for this long is evicted.
  // An evicted subscriber skips ahead to the newest message and counts what it missed as dropped. 0 disables eviction.
  // Subscribers whose process has exited are always reclaimed, regardless of this setting.
  int64_t subscriber_timeout_us             = 0;

  // RELIABLE mode only: subscribers read messages in place in shared memory instead of copying them out first, which
  // saves a copy per message (worthwhile for large messages). The slot stays reserved until every callback for it has
  // returned, so slow callbacks hold up publishers sooner, and these subscribers are never evicted by
  // subscriber_timeout_us: a stalled one blocks publishers until it resumes or its process exits.
  bool zero_copy_reads                      = false;

  int latency_histogram_significant_figures = 3; // (between 1 and 5) precision of the latency histogram, e.g. 3 means percentiles are accurate to within 0.1%

  mode_t shm_permissions                    = 0600; // permissions of the shared memory segment if this process creates it
  int64_t init_timeout_ms                   = 1000; // how long to wait for another process to finish creating the segment
};
}
