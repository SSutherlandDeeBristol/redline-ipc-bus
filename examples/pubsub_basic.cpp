#include "redline/bus.hpp"

#include <chrono>
#include <iostream>
#include <thread>

#include "glog/logging.h"

#include <sys/mman.h>

int main() {
  using namespace redline;

  Config config;
  config.consumer_busy_spin_duration_us = 10;

  const auto bus = createBus("testing", config);

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

  bus->create_subscriber([](const std::span<const uint8_t>& data) {
    LOG_EVERY_T(INFO, 1.) << "Message received (sub 1): " << std::string(data.begin(), data.end());
  });

  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(60));
  }

  return 0;
}