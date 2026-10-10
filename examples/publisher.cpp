// Publishes a numbered message every 100 ms. Run alongside redline_subscriber.
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

#include "redline/bus.hpp"

int main() {
  auto bus = redline::create_bus("example", redline::Config{});

  for (int i = 0;; ++i) {
    const std::string message = "hello " + std::to_string(i);
    if (bus->publish(message) != redline::ErrorCode::OK) {
      std::fprintf(stderr, "publish failed\n");
      return 1;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}
