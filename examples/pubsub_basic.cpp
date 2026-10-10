#include "redline/bus.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>

#include "redline/utils.hpp"

namespace {
std::atomic_bool stop{false};
}

int main() {
  using namespace redline;

  // Stop cleanly on Ctrl-C so the subscriber deregisters from the bus
  std::signal(SIGINT, [](int) { stop = true; });
  std::signal(SIGTERM, [](int) { stop = true; });

  Config config;
  config.bus_size = 8192;
  config.consumer_busy_spin_duration_us = 10;
  config.backpressure_mode = BackPressureMode::RELIABLE;

  const auto bus = create_bus("testing", config);

  int64_t last_time_ns = 0;
  int64_t messageCount = 0;
  double total_latency_ns = 0.0;

  bus->add_subscriber([&](const Message& message) {
    const int64_t current_time_ns = now_ns();

    if (current_time_ns - last_time_ns > 1'000'000'000) {
      std::cout << "Message: " << message.data_as_string() << "\n"
                << "Received " << messageCount << " messages in the last second, average latency: "
                << (messageCount > 0 ? (total_latency_ns) / double(messageCount) : 0.0) << " ns" << std::endl;
      last_time_ns = current_time_ns;
      messageCount = 0;
      total_latency_ns = 0.0;
    }

    total_latency_ns += double(current_time_ns - message.publish_timestamp_ns);
    messageCount++;
  });

  auto publish_thread_1 = std::thread([bus = bus.get()]() {
    uint64_t counter = 0;
    std::string message;
    while (!stop) {
      message = "Hello, world! " + std::to_string(counter++);
      auto ec = bus->publish(std::span((const uint8_t*) message.data(), message.size()), -1);
      if (ec != ErrorCode::OK) {
        std::cerr << "Failed to publish message: " << static_cast<int>(ec) << std::endl;
      }
    }
  });

  while (!stop) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    const auto metrics = bus->get_subscriber_metrics();
    // Report percentiles per interval rather than since startup
    bus->reset_subscriber_metrics();
    std::cout << "Subscriber metrics:\nmessages_consumed=" << metrics.messages_consumed
              << "\ndropped_messages=" << metrics.dropped_messages
              << "\ncurrent_lag=" << metrics.current_lag
              << "\nmax_lag=" << metrics.max_lag
              << "\nlast_latency_ns=" << metrics.last_latency_ns
              << "\np50_latency_ns=" << metrics.p50_latency_ns
              << "\np90_latency_ns=" << metrics.p90_latency_ns
              << "\np99_latency_ns=" << metrics.p99_latency_ns
              << "\np999_latency_ns=" << metrics.p999_latency_ns << std::endl;
  }

  publish_thread_1.join();
  return 0;
}
