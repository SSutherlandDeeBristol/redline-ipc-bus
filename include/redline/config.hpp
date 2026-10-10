#pragma once

#include <cstdint>
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

// All processes attaching to the same bus must use the same max_message_size, bus_size, max_subscribers and
// max_publishers, otherwise create_bus throws. The remaining fields are local to each process (and are defaults for
// each Bus' subscribers).
struct Config {
  uint32_t max_message_size                 = 4096; // in bytes, must be > 0
  uint32_t bus_size                         = 1024; // number of message slots in the bus (must be a power of two >= 2), tune this to absorb bursts of messages

  // Subscriber registry entries (between 1 and 65536), shared by every process on the bus. Each Bus with shared-thread
  // callbacks, each own_thread callback and each Subscription takes one. Costs 128 bytes of shared memory per entry.
  uint32_t max_subscribers                  = 256;

  // Publisher registry entries (between 1 and 65536), shared by every process on the bus. Each thread publishing
  // through a Bus, and each open Claim, holds one while it publishes. Costs 64 bytes of shared memory per entry.
  uint32_t max_publishers                   = 256;

  // Default for this Bus' subscribers; own_thread callbacks and subscribe() can override it via SubscriberOptions
  BackPressureMode backpressure_mode        = BackPressureMode::RELIABLE;

  int64_t consumer_busy_spin_duration_us    = 100; // how long to busy spin for when waiting for messages before futex waiting, in microseconds

  // When this process' publishers are blocked by a live RELIABLE subscriber that has not made progress for this long,
  // they evict it: it skips ahead to the newest message and counts what it missed as dropped. 0 disables eviction.
  // Subscribers whose process has exited are always reclaimed, regardless of this setting.
  int64_t subscriber_timeout_us             = 0;

  // Default for this Bus' subscribers (RELIABLE ones only): read messages in place in shared memory instead of copying
  // them out first, which saves a copy per message (worthwhile for large messages). The slot stays reserved until
  // every callback for it has returned, so slow callbacks hold up publishers sooner, and these subscribers are never
  // evicted by subscriber_timeout_us: a stalled one blocks publishers until it resumes or its process exits.
  bool zero_copy_reads                      = false;

  int latency_histogram_significant_figures = 3; // (between 1 and 5) precision of the latency histogram, e.g. 3 means percentiles are accurate to within 0.1%

  mode_t shm_permissions                    = 0600; // permissions of the shared memory segment if this process creates it
  int64_t init_timeout_ms                   = 1000; // how long to wait for another process to finish creating the segment
};
}
