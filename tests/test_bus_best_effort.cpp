// BEST_EFFORT mode: publishers never wait; slow subscribers lose messages but never receive corrupted ones
#include <thread>

#include <gtest/gtest.h>

#include "redline/bus.hpp"
#include "test_helpers.hpp"

using namespace redline;
using namespace redline::test;
using namespace std::chrono_literals;

TEST(BestEffort, StalledSubscriberNeverBlocksPublisher) {
  ScopedBusName name;
  std::atomic_bool release{false};
  auto bus = create_bus(name, test_config(BackPressureMode::BEST_EFFORT, 8));
  ASSERT_EQ(bus->add_subscriber([&](const Message&) { while (!release) std::this_thread::sleep_for(1ms); }), ErrorCode::OK);

  const auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < 10'000; ++i) ASSERT_EQ(bus->publish(as_bytes("fire and forget"), 0), ErrorCode::OK);
  EXPECT_LT(std::chrono::steady_clock::now() - start, 2s);
  release = true;
}

namespace {
struct OverrunResult {
  uint64_t published = 0;
  uint64_t delivered = 0;
  uint64_t corrupted = 0;
  uint64_t out_of_order = 0;
  uint64_t dropped = 0;
};

// Producers flood a small ring while a deliberately slow subscriber falls behind and gets lapped. This is the
// scenario that used to hand callbacks torn messages (61% of them in the original audit).
OverrunResult run_overrun(uint32_t producers, uint32_t slots, uint32_t message_size, std::chrono::milliseconds duration) {
  ScopedBusName name;
  const Config config = test_config(BackPressureMode::BEST_EFFORT, slots, message_size);
  OverrunResult result;
  std::atomic_uint64_t delivered{0};
  uint64_t last_sequence = 0;
  bool first = true;
  auto bus = create_bus(name, config);
  EXPECT_EQ(bus->add_subscriber([&](const Message& m) {
    for (int spin = 0; spin < 2000; ++spin) std::atomic_signal_fence(std::memory_order_seq_cst); // slow consumer
    if (!check_payload(m.data).valid) ++result.corrupted;
    if (!first && m.sequence <= last_sequence) ++result.out_of_order;
    first = false;
    last_sequence = m.sequence;
    ++delivered;
  }), ErrorCode::OK);

  std::atomic_bool stop{false};
  std::atomic_uint64_t published{0};
  std::vector<std::thread> threads;
  for (uint32_t p = 0; p < producers; ++p) {
    threads.emplace_back([&, p] {
      uint64_t i = 0;
      while (!stop) {
        if (bus->publish(make_payload(p, i, 16 + uint32_t(i % (message_size - 16)))) == ErrorCode::OK) ++published;
        ++i;
      }
    });
  }
  std::this_thread::sleep_for(duration);
  stop = true;
  for (auto& t : threads) t.join();

  // Let the subscriber drain what's left (messages_consumed counts a message before its callback runs, `delivered`
  // after), then every sequence must be accounted for
  EXPECT_TRUE(wait_until([&] {
    const auto m = bus->get_subscriber_metrics();
    return m.current_lag == 0 && m.messages_consumed == delivered;
  }));
  const auto metrics = bus->get_subscriber_metrics();
  result.published = published;
  result.delivered = delivered;
  result.dropped = metrics.dropped_messages;
  EXPECT_EQ(metrics.messages_consumed, result.delivered);
  return result;
}
}

TEST(BestEffort, LappedSubscriberNeverSeesCorruptMessages) {
  const auto r = run_overrun(1, 8, 1024, 1000ms);
  EXPECT_EQ(r.corrupted, 0u);
  EXPECT_EQ(r.out_of_order, 0u);
  EXPECT_GT(r.dropped, 0u) << "test should have overrun the subscriber";
  EXPECT_GT(r.delivered, 0u);
  EXPECT_EQ(r.delivered + r.dropped, r.published) << "every message is either delivered or counted as dropped";
}

TEST(BestEffort, MultipleProducersLappingEachOtherNeverCorrupt) {
  const auto r = run_overrun(4, 2, 512, 1000ms);
  EXPECT_EQ(r.corrupted, 0u);
  EXPECT_EQ(r.out_of_order, 0u);
  EXPECT_GT(r.dropped, 0u);
  EXPECT_EQ(r.delivered + r.dropped, r.published);
}

TEST(BestEffort, FastSubscriberDropsNothing) {
  ScopedBusName name;
  std::atomic_uint64_t received{0};
  auto bus = create_bus(name, test_config(BackPressureMode::BEST_EFFORT, 1024, 64));
  ASSERT_EQ(bus->add_subscriber([&](const Message&) { ++received; }), ErrorCode::OK);
  for (uint64_t i = 0; i < 10'000; ++i) {
    ASSERT_EQ(bus->publish(make_payload(0, i, 32)), ErrorCode::OK);
    if (i % 256 == 255) {
      ASSERT_TRUE(wait_until([&] { return received == i + 1; }));
    }
  }
  ASSERT_TRUE(wait_until([&] { return received == 10'000; }));
  EXPECT_EQ(bus->get_subscriber_metrics().dropped_messages, 0u);
}
