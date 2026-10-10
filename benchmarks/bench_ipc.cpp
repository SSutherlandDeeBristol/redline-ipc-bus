// Latency and throughput between two processes. The parent publishes on a "ping" bus and an echo process, started
// from this same binary, answers on a "pong" bus.
//
//   redline_bench_ipc [--count N] [--cpus A,B] [--latency-metrics]
//
// --count sets the number of round trips per message size (default 200000). --cpus pins the parent to CPU A and the
// echo process to CPU B; use two different physical cores for stable numbers. --latency-metrics turns the subscribers'
// latency metrics on, to see what they cost.
//
// Latency: one message in flight at a time, both sides busy-polling, so this is the transport's round trip without
// queueing or wake-ups. Throughput: the parent publishes as fast as the echo process (a RELIABLE subscriber) can take
// messages, and the clock stops when the echo process reports it has read them all.
#include <algorithm>
#include <cerrno>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sched.h>
#include <spawn.h>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include "redline/bus.hpp"
#include "redline/utils.hpp"

extern char** environ;

using namespace redline;

namespace {
bool latency_metrics = false;

// First byte of each message on the ping bus. An empty message tells the echo process to exit.
constexpr uint8_t PING = 'P';        // echo it back
constexpr uint8_t COUNT = 'C';       // just count it
constexpr uint8_t REPORT = 'R';      // reply with how many COUNT messages arrived

constexpr uint32_t MAX_MESSAGE_SIZE = 4096;
const uint32_t SIZES[] = {16, 256, 4096};

[[noreturn]] void die(const char* what, const Error error) {
  std::fprintf(stderr, "%s failed: %s\n", what, to_string(error).data());
  std::exit(1);
}

template <typename T>
T check(Result<T> result, const char* what) {
  if (!result) die(what, result.error());
  return std::move(*result);
}

void check(const Result<> result, const char* what) {
  if (!result) die(what, result.error());
}

void pin_to(const int cpu) {
  if (cpu < 0) return;
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(static_cast<size_t>(cpu), &set);
  if (sched_setaffinity(0, sizeof(set), &set) != 0) {
    std::fprintf(stderr, "failed to pin to CPU %d: %s\n", cpu, std::strerror(errno));
    std::exit(1);
  }
}

BusOptions spinning() {
  BusOptions options;
  options.consumer_busy_spin_duration_us = 10'000'000; // never sleep: measure the transport, not futex wake-ups
  options.latency_metrics = latency_metrics;
  return options;
}

int run_echo(const std::string& ping_name, const std::string& pong_name, const int cpu) {
  pin_to(cpu);
  auto ping = check(open_bus(ping_name, spinning()), "open_bus");
  auto pong = check(open_bus(pong_name, spinning()), "open_bus");
  auto subscription = check(ping->subscribe(), "subscribe");
  check(pong->publish("ready"), "publish");

  uint64_t counted = 0;
  bool done = false;
  while (!done) {
    subscription->poll([&](const Message& m) {
      if (m.data.empty()) {
        done = true;
      } else if (m.data[0] == PING) {
        check(pong->publish(m.data), "publish");
      } else if (m.data[0] == COUNT) {
        ++counted;
      } else if (m.data[0] == REPORT) {
        check(pong->publish(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(&counted), sizeof(counted))), "publish");
        counted = 0;
      }
    });
  }
  return 0;
}

// Busy-polls until one message arrives, and returns it
std::vector<uint8_t> receive(Subscription& subscription) {
  std::vector<uint8_t> out;
  while (subscription.poll([&](const Message& m) { out.assign(m.data.begin(), m.data.end()); }, 1) == 0) {
  }
  return out;
}

int64_t percentile(const std::vector<int64_t>& sorted, const double p) {
  return sorted[std::min(sorted.size() - 1, size_t(p / 100.0 * double(sorted.size())))];
}

void measure_latency(Bus& ping, Subscription& pong, const uint64_t count) {
  std::printf("Round trip (ns)    p50      p90      p99    p99.9      max\n");
  for (const uint32_t size : SIZES) {
    std::vector<uint8_t> payload(size, 0x42);
    payload[0] = PING;
    std::vector<int64_t> samples;
    samples.reserve(count);
    for (uint64_t i = 0; i < count + count / 10; ++i) { // the first tenth warms up and isn't recorded
      const int64_t start = now_ns();
      check(ping.publish(payload), "publish");
      receive(pong);
      if (i >= count / 10) samples.push_back(now_ns() - start);
    }
    std::sort(samples.begin(), samples.end());
    std::printf("  %4u B     %8" PRId64 " %8" PRId64 " %8" PRId64 " %8" PRId64 " %8" PRId64 "\n", size,
                percentile(samples, 50), percentile(samples, 90), percentile(samples, 99), percentile(samples, 99.9),
                samples.back());
  }
}

void measure_throughput(Bus& ping, Subscription& pong, const uint64_t count) {
  std::printf("\nThroughput        msg/s       MB/s\n");
  const std::vector<uint8_t> report{REPORT};
  for (const uint32_t size : SIZES) {
    std::vector<uint8_t> payload(size, 0x42);
    payload[0] = COUNT;
    const uint64_t messages = count * 20;
    const int64_t start = now_ns();
    for (uint64_t i = 0; i < messages; ++i) {
      check(ping.publish(payload), "publish");
    }
    check(ping.publish(report), "publish");
    const auto reply = receive(pong);
    const double seconds = double(now_ns() - start) / 1e9;

    uint64_t counted = 0;
    std::memcpy(&counted, reply.data(), std::min(reply.size(), sizeof(counted)));
    if (counted != messages) {
      std::fprintf(stderr, "echo process counted %" PRIu64 " of %" PRIu64 " messages\n", counted, messages);
      std::exit(1);
    }
    std::printf("  %4u B   %10.0f %10.1f\n", size, double(messages) / seconds,
                double(messages) * size / seconds / 1e6);
  }
}
}

int main(int argc, char** argv) {
  if (argc == 6 && std::string(argv[1]) == "--echo") {
    latency_metrics = std::string(argv[5]) == "1";
    return run_echo(argv[2], argv[3], std::atoi(argv[4]));
  }

  uint64_t count = 200'000;
  int parent_cpu = -1, echo_cpu = -1;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--count" && i + 1 < argc) {
      count = std::strtoull(argv[++i], nullptr, 10);
    } else if (arg == "--cpus" && i + 1 < argc && std::sscanf(argv[++i], "%d,%d", &parent_cpu, &echo_cpu) == 2) {
    } else if (arg == "--latency-metrics") {
      latency_metrics = true;
    } else {
      std::fprintf(stderr, "usage: %s [--count N] [--cpus A,B] [--latency-metrics]\n", argv[0]);
      return 2;
    }
  }
  if (count < 10) count = 10;
  pin_to(parent_cpu);

  const std::string ping_name = "bench_ipc.ping." + std::to_string(getpid());
  const std::string pong_name = "bench_ipc.pong." + std::to_string(getpid());
  BusLayout layout;
  layout.max_message_size = MAX_MESSAGE_SIZE;
  layout.bus_size = 1024;
  int status = 0;
  {
    auto ping = check(create_bus(ping_name, layout, spinning()), "create_bus");
    auto pong = check(create_bus(pong_name, layout, spinning()), "create_bus");
    auto replies = check(pong->subscribe(), "subscribe");

    std::string self = "/proc/self/exe", echo_flag = "--echo", ping_arg = ping_name, pong_arg = pong_name,
                cpu_arg = std::to_string(echo_cpu), metrics_arg = latency_metrics ? "1" : "0";
    char* child_argv[] = {self.data(), echo_flag.data(), ping_arg.data(), pong_arg.data(), cpu_arg.data(),
                          metrics_arg.data(), nullptr};
    pid_t child = -1;
    if (const int error = posix_spawn(&child, self.c_str(), nullptr, nullptr, child_argv, environ); error != 0) {
      std::fprintf(stderr, "failed to start the echo process: %s\n", std::strerror(error));
      return 1;
    }
    receive(*replies); // "ready": it has subscribed

    measure_latency(*ping, *replies, count);
    measure_throughput(*ping, *replies, count);

    check(ping->publish(std::span<const uint8_t>{}), "publish");
    waitpid(child, &status, 0);
  }
  (void)destroy_bus(ping_name);
  (void)destroy_bus(pong_name);
  return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}
