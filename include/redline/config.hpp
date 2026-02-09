#pragma once

namespace redline {

enum class BackPressureMode {
  RELIABLE = 0, // Producers will wait (up to some timeout) for backpressure to clear before publishing messages, ensuring reliable delivery but potentially higher latency under load
  BEST_EFFORT = 1 // Producers will ignore backpressure and overwrite consumers that are too slow, ensuring low latency but potentially dropping messages under load
};

struct Config {
  uint32_t max_message_size              = 4096; // in bytes (must be multiple of 64)
  uint32_t max_messages                  = 1024; // total number of messages in the bus
  int64_t consumer_busy_spin_duration_us = 100; // how long to busy spin for when waiting for messages before sleeping, in microseconds
  BackPressureMode backpressure_mode     = BackPressureMode::RELIABLE;
};
}