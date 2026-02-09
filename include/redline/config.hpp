#pragma once

namespace redline {
struct Config {
  uint32_t max_message_size = 4096; // in bytes TODO: enforce that this is a multiple of cache line size?
  uint32_t max_messages     = 1024; // total number of messages in the bus TODO: enforce that this is a power of 2 for efficient modulo in the ring buffer?
  int64_t consumer_busy_spin_duration_us = 100; // how long to busy spin for when waiting for messages before sleeping, in microseconds
};
}