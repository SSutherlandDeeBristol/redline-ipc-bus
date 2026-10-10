// One publisher thread and one subscriber in the same process. The publisher sends at a fixed rate (default 100,000
// messages a second; pass another as the first argument), and every second the subscriber's metrics are printed.
//
// The latency printed is publish-to-callback time at that rate, with the subscriber spinning rather than sleeping
// between messages. Raise the rate past what the subscriber can sustain and it measures queueing instead: messages
// wait in the ring for longer and longer until the publisher is held back, and the latency becomes roughly
// bus_size / rate.
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

#include "redline/bus.hpp"
#include "redline/utils.hpp"

namespace {
std::atomic_bool stop{false};
}

int main(int argc, char** argv) {
  using namespace redline;

  const double rate = argc > 1 ? std::atof(argv[1]) : 100'000.0;
  if (rate <= 0) {
    std::fprintf(stderr, "usage: %s [messages per second]\n", argv[0]);
    return 2;
  }

  // Stop cleanly on Ctrl-C so the subscriber deregisters from the bus
  std::signal(SIGINT, [](int) { stop = true; });
  std::signal(SIGTERM, [](int) { stop = true; });

  // A bus of our own, so a leftover segment from an older build or another run can't get in the way
  const std::string name = "pubsub_basic." + std::to_string(getpid());
  BusLayout layout;
  layout.max_message_size = 64;
  layout.bus_size = 8192;
  BusOptions options;
  options.consumer_busy_spin_duration_us = 1000; // longer than the gap between messages, so the subscriber never sleeps
  auto created = create_bus(name, layout, options);
  if (!created) {
    std::fprintf(stderr, "create_bus failed: %s\n", to_string(created.error()).data());
    return 1;
  }
  auto& bus = *created;

  std::atomic_uint64_t received{0};
  if (!bus->add_subscriber([&](const Message&) { received.fetch_add(1, std::memory_order_relaxed); })) {
    return 1;
  }

  std::thread publisher([&] {
    const auto interval = std::chrono::duration<double>(1.0 / rate);
    auto next = std::chrono::steady_clock::now();
    uint64_t counter = 0;
    std::string message;
    while (!stop) {
      // Busy-wait for the next send time: sleeping is far too coarse at these rates
      while (std::chrono::steady_clock::now() < next) {
      }
      next += std::chrono::duration_cast<std::chrono::steady_clock::duration>(interval);
      message = "message " + std::to_string(counter++);
      if (auto published = bus->publish(message); !published) {
        std::fprintf(stderr, "publish failed: %s\n", to_string(published.error()).data());
      }
    }
  });

  uint64_t last_received = 0;
  while (!stop) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    const auto metrics = bus->get_subscriber_metrics();
    // Report percentiles per interval rather than since startup
    bus->reset_subscriber_metrics();
    const uint64_t total = received.load();
    std::printf("received %llu/s  latency p50 %lld ns  p99 %lld ns  p99.9 %lld ns  max lag %llu  dropped %llu\n",
                static_cast<unsigned long long>(total - last_received), static_cast<long long>(metrics.p50_latency_ns),
                static_cast<long long>(metrics.p99_latency_ns), static_cast<long long>(metrics.p999_latency_ns),
                static_cast<unsigned long long>(metrics.max_lag),
                static_cast<unsigned long long>(metrics.dropped_messages));
    std::fflush(stdout);
    last_received = total;
  }

  publisher.join();
  bus.reset();
  (void)destroy_bus(name);
  return 0;
}
