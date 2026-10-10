// Prints every message published on the "example" bus. Run alongside redline_publisher.
#include <chrono>
#include <cstdio>
#include <thread>

#include "redline/bus.hpp"
#include "redline/utils.hpp"

int main() {
  auto bus = redline::create_bus("example", redline::BusLayout{});
  if (!bus) {
    std::fprintf(stderr, "create_bus failed: %s\n", redline::to_string(bus.error()).data());
    return 1;
  }

  auto subscribed = (*bus)->add_subscriber([](const redline::Message& m) {
    const double latency_us = double(redline::now_ns() - m.publish_timestamp_ns) / 1000.0;
    const std::string_view text = m.data_as_string();
    std::printf("#%llu %.*s (%.1f us)\n", static_cast<unsigned long long>(m.sequence), static_cast<int>(text.size()),
                text.data(), latency_us);
    std::fflush(stdout);
  });
  if (!subscribed) {
    std::fprintf(stderr, "add_subscriber failed: %s\n", redline::to_string(subscribed.error()).data());
    return 1;
  }

  // Messages arrive on a background thread
  std::this_thread::sleep_for(std::chrono::hours(24));
}
