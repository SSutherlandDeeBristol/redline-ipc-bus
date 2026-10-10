// Bus::publish_batch: one reservation for several messages
#include <set>
#include <thread>

#include <gtest/gtest.h>

#include "redline/bus.hpp"
#include "test_helpers.hpp"

using namespace redline;
using namespace redline::test;
using namespace std::chrono_literals;

namespace {
struct Batch {
  std::vector<std::vector<uint8_t>> storage;
  std::vector<std::span<const uint8_t>> spans;

  Batch(uint32_t producer, uint64_t first_counter, size_t count, uint32_t size = 32) {
    for (size_t i = 0; i < count; ++i) storage.push_back(make_payload(producer, first_counter + i, size));
    for (const auto& s : storage) spans.emplace_back(s);
  }
};

class BatchAnyMode : public ::testing::TestWithParam<BackPressureMode> {};

std::string mode_name(const ::testing::TestParamInfo<BackPressureMode>& info) {
  return info.param == BackPressureMode::RELIABLE ? "Reliable" : "BestEffort";
}
}

TEST_P(BatchAnyMode, DeliversAsConsecutiveSequences) {
  ScopedBusName name;
  auto bus = create_bus(name, test_config(GetParam(), 64));
  auto subscription = bus->subscribe();

  ASSERT_EQ(bus->publish(as_bytes("single")), ErrorCode::OK);
  const Batch batch(3, 0, 10);
  ASSERT_EQ(bus->publish_batch(batch.spans), ErrorCode::OK);
  ASSERT_EQ(bus->publish_batch({}), ErrorCode::OK);

  std::vector<uint64_t> sequences, counters;
  std::vector<int64_t> timestamps;
  subscription->poll([&](const Message& m) {
    sequences.push_back(m.sequence);
    if (m.sequence > 0) {
      const auto info = check_payload(m.data);
      EXPECT_TRUE(info.valid);
      counters.push_back(info.counter);
      timestamps.push_back(m.publish_timestamp_ns);
    }
  });
  EXPECT_EQ(sequences, (std::vector<uint64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
  EXPECT_EQ(counters, (std::vector<uint64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
  EXPECT_EQ(std::set<int64_t>(timestamps.begin(), timestamps.end()).size(), 1u) << "a batch shares one timestamp";
}

TEST_P(BatchAnyMode, OneOversizedMessageRejectsTheWholeBatch) {
  ScopedBusName name;
  const Config config = test_config(GetParam(), 64, 64);
  auto bus = create_bus(name, config);
  auto subscription = bus->subscribe();

  Batch batch(0, 0, 3);
  const std::vector<uint8_t> too_big(65);
  batch.spans[1] = too_big;
  EXPECT_EQ(bus->publish_batch(batch.spans), ErrorCode::MESSAGE_TOO_LARGE);
  EXPECT_EQ(subscription->poll([](const Message&) {}), 0u);
  ASSERT_EQ(bus->publish(as_bytes("next")), ErrorCode::OK);
  subscription->poll([](const Message& m) { EXPECT_EQ(m.sequence, 0u) << "no sequences were used up"; });
}

INSTANTIATE_TEST_SUITE_P(Modes, BatchAnyMode,
                         ::testing::Values(BackPressureMode::RELIABLE, BackPressureMode::BEST_EFFORT), mode_name);

TEST(Batch, ReliableBatchLargerThanTheBusIsInvalid) {
  ScopedBusName name;
  auto bus = create_bus(name, test_config(BackPressureMode::RELIABLE, 8));
  EXPECT_EQ(bus->publish_batch(Batch(0, 0, 9).spans), ErrorCode::INVALID_ARGUMENT);
  EXPECT_EQ(bus->publish_batch(Batch(0, 0, 8).spans, 0), ErrorCode::OK);
}

TEST(Batch, ReliableBatchWaitsForRoomForAllOfIt) {
  ScopedBusName name;
  auto bus = create_bus(name, test_config(BackPressureMode::RELIABLE, 8));
  auto subscription = bus->subscribe();
  ASSERT_EQ(bus->publish_batch(Batch(0, 0, 5).spans, 0), ErrorCode::OK);

  // 3 slots free: a batch of 4 doesn't fit and publishes nothing
  EXPECT_EQ(bus->publish_batch(Batch(0, 5, 4).spans, 1000), ErrorCode::BACKPRESSURE_TIMEOUT);
  EXPECT_EQ(subscription->get_metrics().current_lag, 5u);

  std::thread consumer([&] {
    std::this_thread::sleep_for(20ms);
    subscription->poll([](const Message&) {}, 1);
  });
  EXPECT_EQ(bus->publish_batch(Batch(0, 5, 4).spans, 2'000'000), ErrorCode::OK);
  consumer.join();

  std::vector<uint64_t> counters;
  subscription->poll([&](const Message& m) { counters.push_back(check_payload(m.data).counter); });
  EXPECT_EQ(counters, (std::vector<uint64_t>{1, 2, 3, 4, 5, 6, 7, 8}));
}

TEST(Batch, ConcurrentBatchesStayContiguousAndComplete) {
  ScopedBusName name;
  constexpr uint32_t producers = 4;
  constexpr uint64_t batches = 2'000;
  constexpr size_t batch_size = 7;
  std::vector<uint64_t> next_expected(producers, 0);
  std::atomic_uint64_t received{0}, errors{0};
  uint32_t current_producer = producers;
  size_t run_length = 0;
  auto bus = create_bus(name, test_config(BackPressureMode::RELIABLE, 64, 128));
  ASSERT_EQ(bus->add_subscriber([&](const Message& m) {
    const auto info = check_payload(m.data);
    if (!info.valid || info.producer >= producers || info.counter != next_expected[info.producer]) {
      ++errors;
    } else {
      ++next_expected[info.producer];
    }
    // A batch is never interleaved with another producer's messages
    if (info.producer != current_producer) {
      if (current_producer < producers && run_length % batch_size != 0) ++errors;
      current_producer = info.producer;
      run_length = 0;
    }
    ++run_length;
    ++received;
  }), ErrorCode::OK);

  std::vector<std::thread> threads;
  for (uint32_t p = 0; p < producers; ++p) {
    threads.emplace_back([&, p] {
      for (uint64_t b = 0; b < batches; ++b) {
        const Batch batch(p, b * batch_size, batch_size, 16 + uint32_t(b % 100));
        ASSERT_EQ(bus->publish_batch(batch.spans, 5'000'000), ErrorCode::OK);
      }
    });
  }
  for (auto& t : threads) t.join();
  ASSERT_TRUE(wait_until([&] { return received == producers * batches * batch_size; }, 10s));
  EXPECT_EQ(errors.load(), 0u);
  EXPECT_EQ(bus->get_subscriber_metrics().dropped_messages, 0u);
}
