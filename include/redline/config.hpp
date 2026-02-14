#pragma once

namespace redline {

enum class BackPressureMode {
  RELIABLE = 0, // Producers will wait (up to some timeout) for backpressure to clear before publishing messages, ensuring reliable delivery but potentially higher latency under load
  BEST_EFFORT = 1 // Producers will ignore backpressure and overwrite consumers that are too slow, ensuring low latency but potentially dropping messages under load
};

struct Config {
  uint32_t max_message_size                 = 4096; // in bytes (must be multiple of 64)
  uint32_t bus_size                         = 1024; // total number of messages in the bus, tune this to absorb bursts of messages

  BackPressureMode backpressure_mode        = BackPressureMode::RELIABLE; // see BackPressureMode enum

  int64_t consumer_busy_spin_duration_us    = 100; // how long to busy spin for when waiting for messages before futex waiting, in microseconds

  int latency_histogram_num_buckets         = 2048; // number of buckets for the latency histogram#
  int latency_histogram_significant_figures = 3; // (between 1 and 5) number of significant figures for latency histogram (e.g. 3 means 1ns precision up to 1us, then 10ns precision up to 10us, etc.)
};
}