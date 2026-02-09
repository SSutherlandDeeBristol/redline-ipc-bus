#include "redline/bus.hpp"

#include <chrono>
#include <iostream>
#include <thread>

#include "glog/logging.h"

#include "redline/utils.hpp"

int main() {
  using namespace redline;

  Config config;
  config.consumer_busy_spin_duration_us = 10;

  const auto bus = create_bus("testing", config);

  auto publish_thread_1 = std::thread([bus = bus.get(), config]() {
    uint64_t counter = 0;
    while (true) {
      std::string message = "Hello, world! " + std::to_string(counter++);
      while (!bus->publish(std::span((const uint8_t*) message.data(), message.size()))) {
        // busy spin
      }
      // std::this_thread::sleep_for(std::chrono::nanoseconds(10));
    }
  });

  int64_t last_time_us = 0;
  int64_t messageCount = 0;
  double total_latency_ms = 0.0;

  bus->create_subscriber([&](const Message& message) {
    const int64_t current_time_us = now_us();

    if (current_time_us - last_time_us > 1'000'000) {
      LOG(INFO) << "Received " << messageCount << " messages in the last second, average latency: " << (messageCount > 0 ? total_latency_ms / messageCount : 0.0) << " ms";
      last_time_us = current_time_us;
      messageCount = 0;
      total_latency_ms = 0.0;
    }

    total_latency_ms += double(current_time_us - message.publish_timestamp) / 1000;
    messageCount++;
  });

  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(60));
  }

  return 0;
}