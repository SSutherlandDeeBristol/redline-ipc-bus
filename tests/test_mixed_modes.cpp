// Delivery mode is each subscriber's choice: RELIABLE and BEST_EFFORT subscribers share a bus
#include <thread>

#include <gtest/gtest.h>

#include "redline/bus.hpp"
#include "test_helpers.hpp"

using namespace redline;
using namespace redline::test;
using namespace std::chrono_literals;

TEST(MixedModes, LossyObserverNeverThrottlesAReliableBus) {
  ScopedBusName name;
  constexpr uint32_t slots = 16;
  constexpr uint64_t messages = 2'000;
  Gate gate;
  Received reliable_received;
  std::atomic_uint64_t observed{0};
  auto publisher = make_bus(name, test_config(BackPressureMode::RELIABLE, slots));
  auto reliable = make_bus(name, test_config(BackPressureMode::RELIABLE, slots));
  auto observer = make_bus(name, test_config(BackPressureMode::BEST_EFFORT, slots));

  ASSERT_TRUE(reliable->add_subscriber([&](const Message& m) { reliable_received.add(check_payload(m.data).counter); }));
  ASSERT_TRUE(observer->add_subscriber([&](const Message& m) {
    if (m.sequence == 0) gate.wait();
    EXPECT_TRUE(check_payload(m.data).valid);
    ++observed;
  }));

  ASSERT_TRUE(publisher->publish(make_payload(0, 0, 32)));
  ASSERT_TRUE(wait_until([&] { return gate.has_waiter(); }));

  // The observer is stuck, yet only the reliable subscriber's pace matters
  for (uint64_t i = 1; i < messages; ++i) {
    ASSERT_TRUE(publisher->publish(make_payload(0, i, 32), 2'000'000)) << i;
  }
  ASSERT_TRUE(wait_until([&] { return reliable_received.size() == messages; }));
  const auto values = reliable_received.values();
  for (uint64_t i = 0; i < messages; ++i) ASSERT_EQ(values[i], i);
  EXPECT_EQ(reliable->get_subscriber_metrics().dropped_messages, 0u);

  // The observer was lapped: it gets what is left and counts the rest as dropped
  gate.open();
  ASSERT_TRUE(wait_until([&] { return observer->get_subscriber_metrics().current_lag == 0; }));
  const auto metrics = observer->get_subscriber_metrics();
  EXPECT_GT(metrics.dropped_messages, 0u);
  EXPECT_EQ(metrics.messages_consumed + metrics.dropped_messages, messages);
}

TEST(MixedModes, SlowReliableSubscriberStillHoldsPublishersBack) {
  ScopedBusName name;
  Config config = test_config(BackPressureMode::BEST_EFFORT, 8);
  auto bus = make_bus(name, config);
  auto lossy = subscribe(*bus);
  auto reliable = subscribe(*bus, {.backpressure_mode = BackPressureMode::RELIABLE});

  for (int i = 0; i < 8; ++i) ASSERT_TRUE(bus->publish(as_bytes("m"), 0));
  EXPECT_EQ(bus->publish(as_bytes("m"), 0), std::unexpected(Error::BACKPRESSURE_TIMEOUT));
  EXPECT_EQ(reliable->poll([](const Message&) {}, 1), 1u);
  EXPECT_TRUE(bus->publish(as_bytes("m"), 0));

  // Dropping the reliable subscription leaves nothing to wait for
  reliable.reset();
  for (int i = 0; i < 100; ++i) ASSERT_TRUE(bus->publish(as_bytes("m"), 0));
  lossy->poll([](const Message&) {});
  EXPECT_EQ(lossy->get_metrics().messages_consumed + lossy->get_metrics().dropped_messages, 109u);
}

TEST(MixedModes, OptionsOverrideTheBusConfig) {
  ScopedBusName name;
  std::atomic_int own_thread_received{0};
  Gate gate;
  auto bus = make_bus(name, test_config(BackPressureMode::RELIABLE, 4));
  auto lossy = subscribe(*bus, {.backpressure_mode = BackPressureMode::BEST_EFFORT});
  for (int i = 0; i < 50; ++i) ASSERT_TRUE(bus->publish(as_bytes("m"), 0));

  ASSERT_TRUE(bus->add_subscriber([&](const Message&) {
    gate.wait();
    ++own_thread_received;
  }, {.own_thread = true, .backpressure_mode = BackPressureMode::BEST_EFFORT}));
  for (int i = 0; i < 50; ++i) ASSERT_TRUE(bus->publish(as_bytes("m"), 0)) << "own_thread observer is lossy too";
  gate.open();
}

TEST(MixedModes, InvalidOptionsAreRejected) {
  ScopedBusName name;
  auto bus = make_bus(name, test_config(BackPressureMode::RELIABLE));
  const auto noop = [](const Message&) {};

  // Shared-thread callbacks share one subscription, configured by the Bus' BusOptions
  EXPECT_EQ(bus->add_subscriber(noop, {.backpressure_mode = BackPressureMode::BEST_EFFORT}), std::unexpected(Error::INVALID_ARGUMENT));
  EXPECT_EQ(bus->add_subscriber(noop, {.zero_copy_reads = true}), std::unexpected(Error::INVALID_ARGUMENT));
  EXPECT_EQ(bus->add_subscriber(noop, {.own_thread = true, .backpressure_mode = BackPressureMode::BEST_EFFORT, .zero_copy_reads = true}),
            std::unexpected(Error::INVALID_ARGUMENT));
  EXPECT_EQ(bus->subscribe({.own_thread = true}), std::unexpected(Error::INVALID_ARGUMENT));
  EXPECT_EQ(bus->subscribe({.cpu = 0}), std::unexpected(Error::INVALID_ARGUMENT));
  EXPECT_EQ(bus->subscribe({.backpressure_mode = BackPressureMode::BEST_EFFORT, .zero_copy_reads = true}),
            std::unexpected(Error::INVALID_ARGUMENT));

  EXPECT_NE(subscribe(*bus, {.zero_copy_reads = true}), nullptr);
  EXPECT_TRUE(bus->add_subscriber(noop, {.own_thread = true, .zero_copy_reads = true}));
}

TEST(MixedModes, BatchLargerThanTheBusIsInvalidWithoutReliableSubscribers) {
  ScopedBusName name;
  auto bus = make_bus(name, test_config(BackPressureMode::BEST_EFFORT, 4));
  const std::vector<uint8_t> payload(8);
  const std::vector<std::span<const uint8_t>> batch(5, payload);
  EXPECT_EQ(bus->publish_batch(batch), std::unexpected(Error::INVALID_ARGUMENT));
}
