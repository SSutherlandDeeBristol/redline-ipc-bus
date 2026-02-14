#include "redline/bus.hpp"

#include <chrono>
#include <iostream>
#include <thread>

#include "glog/logging.h"

#include "redline/utils.hpp"

int main() {
  using namespace redline;

  Config config;
  config.bus_size = 8192;
  config.consumer_busy_spin_duration_us = 10;
  config.backpressure_mode = BackPressureMode::BEST_EFFORT;

  const auto bus = create_bus("testing", config);

  auto publish_thread_1 = std::thread([bus = bus.get(), config]() {
    uint64_t counter = 0;
    while (true) {
      std::string message = "Hello, world! " + std::to_string(counter++);
      auto ec = bus->publish(std::span((const uint8_t*) message.data(), message.size()), -1);
      if (ec != ErrorCode::OK) {
        LOG(ERROR) << "Failed to publish message: " << static_cast<int>(ec);
      }
    }
  });

  int64_t last_time_ns = 0;
  int64_t messageCount = 0;
  double total_latency_ns = 0.0;

  bus->add_subscriber([&](const Message& message) {
    const int64_t current_time_ns = now_ns();

    if (current_time_ns - last_time_ns > 1'000'000'000) {
      LOG(INFO) << "Message: " << message.data_as_string();
      LOG(INFO) << "Received " << messageCount << " messages in the last second, average latency: " << (messageCount > 0 ? (total_latency_ns) / double(messageCount) : 0.0) << " ns";
      last_time_ns = current_time_ns;
      messageCount = 0;
      total_latency_ns = 0.0;
    }

    total_latency_ns += double(current_time_ns - message.publish_timestamp_ns);
    messageCount++;
  });

  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    const auto metrics = bus->get_subscriber_metrics();
    LOG(INFO) << "Subscriber metrics:\nmessages_consumed=" << metrics.messages_consumed
              << "\ndropped_messages=" << metrics.dropped_messages
              << "\ncurrent_lag=" << metrics.current_lag
              << "\nmax_lag=" << metrics.max_lag
              << "\nlast_latency_ns=" << metrics.last_latency_ns
              << "\np50_latency_ns=" << metrics.p50_latency_ns
              << "\np90_latency_ns=" << metrics.p90_latency_ns
              << "\np99_latency_ns=" << metrics.p99_latency_ns
              << "\np999_latency_ns=" << metrics.p999_latency_ns;
  }

  return 0;
}