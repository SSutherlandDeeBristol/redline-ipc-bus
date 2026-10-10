// Publishes a numbered message every 100 ms. Run alongside redline_subscriber.
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

#include "redline/bus.hpp"

int main() {
  auto bus = redline::create_bus("example", redline::BusLayout{});
  if (!bus) {
    std::fprintf(stderr, "create_bus failed: %s\n", redline::to_string(bus.error()).data());
    return 1;
  }

  for (int i = 0;; ++i) {
    const std::string message = "hello " + std::to_string(i);
    if (auto published = (*bus)->publish(message); !published) {
      std::fprintf(stderr, "publish failed: %s\n", redline::to_string(published.error()).data());
      return 1;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}
