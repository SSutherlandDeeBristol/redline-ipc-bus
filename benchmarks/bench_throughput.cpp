// Publish throughput for one producer, with and without a subscriber
#include <atomic>
#include <cstring>
#include <vector>

#include <benchmark/benchmark.h>

#include "bench_common.hpp"
#include "redline/bus.hpp"

using namespace redline;

namespace {
// Args: message size, subscriber count (0 or 1), backpressure mode, zero-copy reads, publish via claim, latency metrics
void BM_Publish(benchmark::State& state) {
  const auto size = static_cast<uint32_t>(state.range(0));
  const bool subscribe = state.range(1) != 0;
  const auto mode = static_cast<BackPressureMode>(state.range(2));
  const bool zero_copy = state.range(3) != 0;
  const bool use_claim = state.range(4) != 0;
  const bool latency_metrics = state.range(5) != 0;

  bench::ScopedBus scoped("throughput");
  BusLayout layout;
  layout.bus_size = 8192;
  layout.max_message_size = 4096;
  BusOptions options;
  options.backpressure_mode = mode;
  options.zero_copy_reads = zero_copy;
  options.latency_metrics = latency_metrics;
  auto bus = bench::make_bus(scoped.name(), layout, options);

  std::atomic_uint64_t received{0};
  if (subscribe) {
    bus->add_subscriber([&](const Message&) { received.fetch_add(1, std::memory_order_relaxed); });
  }

  const std::vector<uint8_t> payload(size, 0x42);
  for (auto _ : state) {
    if (use_claim) {
      // Stands in for serializing straight into shared memory: the message is written once, in place
      if (auto claim = bus->claim()) {
        std::memset(claim->data().data(), 0x42, size);
        benchmark::DoNotOptimize(claim->commit(size));
      }
    } else {
      benchmark::DoNotOptimize(bus->publish(payload));
    }
  }

  state.SetItemsProcessed(int64_t(state.iterations()));
  state.SetBytesProcessed(int64_t(state.iterations()) * size);
  if (subscribe) {
    const auto metrics = bus->get_subscriber_metrics();
    state.counters["dropped"] = double(metrics.dropped_messages);
    state.counters["p50_ns"] = double(metrics.p50_latency_ns);
    state.counters["p99_ns"] = double(metrics.p99_latency_ns);
  }
}
}

BENCHMARK(BM_Publish)
    ->ArgNames({"size", "subs", "mode", "zero_copy", "claim", "metrics"})
    // No subscriber: raw cost of publishing
    ->Args({16, 0, int(BackPressureMode::BEST_EFFORT), 0, 0, 1})
    ->Args({256, 0, int(BackPressureMode::BEST_EFFORT), 0, 0, 1})
    ->Args({4096, 0, int(BackPressureMode::BEST_EFFORT), 0, 0, 1})
    // One subscriber, publisher never waits
    ->Args({16, 1, int(BackPressureMode::BEST_EFFORT), 0, 0, 1})
    ->Args({4096, 1, int(BackPressureMode::BEST_EFFORT), 0, 0, 1})
    // One subscriber, publisher throttled to the subscriber's pace
    ->Args({16, 1, int(BackPressureMode::RELIABLE), 0, 0, 1})
    ->Args({256, 1, int(BackPressureMode::RELIABLE), 0, 0, 1})
    ->Args({4096, 1, int(BackPressureMode::RELIABLE), 0, 0, 1})
    // One subscriber reading in place
    ->Args({16, 1, int(BackPressureMode::RELIABLE), 1, 0, 1})
    ->Args({4096, 1, int(BackPressureMode::RELIABLE), 1, 0, 1})
    // Built in place by the publisher too: zero-copy end to end
    ->Args({4096, 0, int(BackPressureMode::BEST_EFFORT), 0, 1, 1})
    ->Args({4096, 1, int(BackPressureMode::RELIABLE), 1, 1, 1})
    // Without latency metrics: no clock read or histogram update per message
    ->Args({16, 1, int(BackPressureMode::RELIABLE), 0, 0, 0})
    ->Args({16, 1, int(BackPressureMode::RELIABLE), 1, 0, 0})
    ->UseRealTime();

BENCHMARK_MAIN();
