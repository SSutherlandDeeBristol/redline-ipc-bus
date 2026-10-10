// Multiple producers publishing to one bus, with or without a subscriber, one message at a time or in batches
#include <memory>
#include <vector>

#include <benchmark/benchmark.h>

#include "bench_common.hpp"
#include "redline/bus.hpp"

using namespace redline;

namespace {
std::unique_ptr<bench::ScopedBus> scoped;
std::unique_ptr<Bus> bus;

// Args: backpressure mode, subscriber count (0 or 1), batch size
void setup(const benchmark::State& state) {
  scoped = std::make_unique<bench::ScopedBus>("mp");
  Config config;
  config.bus_size = 8192;
  config.max_message_size = 256;
  config.backpressure_mode = static_cast<BackPressureMode>(state.range(0));
  bus = create_bus(scoped->name(), config);
  if (state.range(1) != 0) {
    bus->add_subscriber([](const Message&) {});
  }
}

void teardown(const benchmark::State&) {
  bus.reset();
  scoped.reset();
}

void BM_PublishMultiProducer(benchmark::State& state) {
  const auto batch_size = static_cast<size_t>(state.range(2));
  const std::vector<uint8_t> payload(64, 0x42);
  const std::vector<std::span<const uint8_t>> batch(batch_size, payload);

  for (auto _ : state) {
    if (batch_size == 1) {
      benchmark::DoNotOptimize(bus->publish(payload));
    } else {
      benchmark::DoNotOptimize(bus->publish_batch(batch));
    }
  }
  state.SetItemsProcessed(state.iterations() * int64_t(batch_size));
}
}

BENCHMARK(BM_PublishMultiProducer)
    ->ArgNames({"mode", "subs", "batch"})
    ->Args({int(BackPressureMode::BEST_EFFORT), 0, 1})
    ->Args({int(BackPressureMode::RELIABLE), 0, 1})
    ->Args({int(BackPressureMode::RELIABLE), 1, 1})
    ->Args({int(BackPressureMode::RELIABLE), 1, 16})
    ->Args({int(BackPressureMode::BEST_EFFORT), 1, 1})
    ->Setup(setup)
    ->Teardown(teardown)
    ->ThreadRange(1, 8)
    ->UseRealTime();

BENCHMARK_MAIN();
