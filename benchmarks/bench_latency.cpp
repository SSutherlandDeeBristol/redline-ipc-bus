// End-to-end latency without queueing: a ping-pong between two buses, one message in flight at a time.
// Reported time per iteration is the round trip; one-way latency is roughly half of it.
#include <atomic>
#include <thread>
#include <vector>

#include <benchmark/benchmark.h>

#include "bench_common.hpp"
#include "redline/bus.hpp"

using namespace redline;

namespace {
void BM_PingPong(benchmark::State& state) {
  const auto size = static_cast<uint32_t>(state.range(0));
  const bool zero_copy = state.range(1) != 0;

  bench::ScopedBus ping_name("ping"), pong_name("pong");
  BusLayout layout;
  layout.bus_size = 1024;
  layout.max_message_size = 4096;
  BusOptions options;
  options.zero_copy_reads = zero_copy;
  options.consumer_busy_spin_duration_us = 1'000'000; // spin, don't sleep: measure the transport, not futex wakeups

  auto ping = bench::make_bus(ping_name.name(), layout, options);
  auto pong = bench::make_bus(pong_name.name(), layout, options);

  // The echo side would normally live in another process: see bench_ipc for that
  auto echo_in = bench::make_bus(ping_name.name(), layout, options);
  auto echo_out = bench::make_bus(pong_name.name(), layout, options);
  echo_in->add_subscriber([&](const Message& m) { echo_out->publish(m.data); });

  std::atomic_uint64_t pongs{0};
  pong->add_subscriber([&](const Message&) { pongs.fetch_add(1, std::memory_order_release); });

  const std::vector<uint8_t> payload(size, 0x42);
  uint64_t expected = 0;
  for (auto _ : state) {
    ping->publish(payload);
    ++expected;
    while (pongs.load(std::memory_order_acquire) != expected) {
    }
  }
  state.SetItemsProcessed(int64_t(state.iterations()));
}
}

BENCHMARK(BM_PingPong)
    ->ArgNames({"size", "zero_copy"})
    ->ArgsProduct({{16, 256, 4096}, {0, 1}})
    ->UseRealTime()
    ->MinTime(2.0);

BENCHMARK_MAIN();
