// Config::zero_copy_reads: callbacks read slots in place, which RELIABLE mode keeps stable until they return
#include <thread>

#include <gtest/gtest.h>

#include "redline/bus.hpp"
#include "test_helpers.hpp"

using namespace redline;
using namespace redline::test;
using namespace std::chrono_literals;

namespace {
Config zero_copy_config(uint32_t bus_size = 64, uint32_t max_message_size = 256) {
  Config config = test_config(BackPressureMode::RELIABLE, bus_size, max_message_size);
  config.zero_copy_reads = true;
  return config;
}
}

TEST(ZeroCopy, RequiresReliableMode) {
  ScopedBusName name;
  Config config = test_config(BackPressureMode::BEST_EFFORT);
  config.zero_copy_reads = true;
  EXPECT_THROW(create_bus(name, config), std::invalid_argument);
}

TEST(ZeroCopy, OnlyAffectsTheProcessThatAsksForIt) {
  // Copying and zero-copy subscribers can share a bus
  ScopedBusName name;
  Received copied, in_place;
  auto copying = create_bus(name, test_config(BackPressureMode::RELIABLE));
  auto zero_copy = create_bus(name, zero_copy_config());
  ASSERT_EQ(copying->add_subscriber([&](const Message& m) { copied.add(check_payload(m.data).counter); }), ErrorCode::OK);
  ASSERT_EQ(zero_copy->add_subscriber([&](const Message& m) { in_place.add(check_payload(m.data).counter); }), ErrorCode::OK);

  for (uint32_t i = 0; i < 200; ++i) ASSERT_EQ(copying->publish(make_payload(0, i, 16 + i), 1'000'000), ErrorCode::OK);
  ASSERT_TRUE(wait_until([&] { return copied.size() == 200 && in_place.size() == 200; }));
  EXPECT_EQ(copied.values(), in_place.values());
}

TEST(ZeroCopy, SlotIsHeldUntilTheCallbackReturns) {
  ScopedBusName name;
  constexpr uint32_t slots = 8;
  Gate gate;
  std::vector<uint8_t> seen_after_wait;
  std::atomic_bool seen{false};
  auto bus = create_bus(name, zero_copy_config(slots));
  ASSERT_EQ(bus->add_subscriber([&](const Message& m) {
    if (m.sequence != 0) return;
    gate.wait();
    // Publishers have been trying to reuse this slot the whole time
    seen_after_wait.assign(m.data.begin(), m.data.end());
    seen = true;
  }), ErrorCode::OK);

  const auto first = make_payload(0, 0, 64);
  ASSERT_EQ(bus->publish(first, 0), ErrorCode::OK);
  ASSERT_TRUE(wait_until([&] { return gate.has_waiter(); }));

  // Unlike copy mode, message 0's slot is still in use, so only slots - 1 more fit
  uint64_t published = 1;
  while (bus->publish(make_payload(1, published, 64), 1000) == ErrorCode::OK) ++published;
  EXPECT_EQ(published, slots);

  gate.open();
  ASSERT_TRUE(wait_until([&] { return seen.load(); }));
  EXPECT_EQ(seen_after_wait, first);
  ASSERT_TRUE(wait_until([&] { return bus->get_subscriber_metrics().current_lag == 0; }));
  EXPECT_EQ(bus->publish(as_bytes("room again"), 0), ErrorCode::OK);
}

TEST(ZeroCopy, ManyProducersNeverCorruptOrLose) {
  ScopedBusName name;
  constexpr uint32_t producers = 4;
  constexpr uint64_t per_producer = 10'000;
  std::vector<uint64_t> next_expected(producers, 0);
  std::atomic_uint64_t received{0}, errors{0};
  auto bus = create_bus(name, zero_copy_config(16, 512));
  ASSERT_EQ(bus->add_subscriber([&](const Message& m) {
    // Read the payload slowly, giving any producer that wrongly reused the slot time to tear it
    std::this_thread::yield();
    const auto info = check_payload(m.data);
    if (!info.valid || info.producer >= producers || info.counter != next_expected[info.producer]) {
      ++errors;
    } else {
      ++next_expected[info.producer];
    }
    ++received;
  }), ErrorCode::OK);

  std::vector<std::thread> threads;
  for (uint32_t p = 0; p < producers; ++p) {
    threads.emplace_back([&, p] {
      for (uint64_t i = 0; i < per_producer; ++i) {
        ASSERT_EQ(bus->publish(make_payload(p, i, 16 + uint32_t(i % 496)), 5'000'000), ErrorCode::OK);
      }
    });
  }
  for (auto& t : threads) t.join();
  ASSERT_TRUE(wait_until([&] { return received == producers * per_producer; }, 10s));
  EXPECT_EQ(errors.load(), 0u);
}

TEST(ZeroCopy, StalledSubscriberIsNeverEvicted) {
  ScopedBusName name;
  Config config = zero_copy_config(4);
  config.subscriber_timeout_us = 10'000;
  Gate gate;
  auto publisher = create_bus(name, config);
  auto subscriber = create_bus(name, config);
  ASSERT_EQ(subscriber->add_subscriber([&](const Message& m) { if (m.sequence == 0) gate.wait(); }), ErrorCode::OK);
  ASSERT_EQ(publisher->publish(as_bytes("m"), 0), ErrorCode::OK);
  ASSERT_TRUE(wait_until([&] { return gate.has_waiter(); }));

  ErrorCode last = ErrorCode::OK;
  for (int i = 0; i < 10 && last == ErrorCode::OK; ++i) last = publisher->publish(as_bytes("m"), 300'000);
  EXPECT_EQ(last, ErrorCode::BACKPRESSURE_TIMEOUT) << "evicting it would let publishers overwrite the slot it is reading";

  gate.open();
  EXPECT_TRUE(wait_until([&] { return subscriber->get_subscriber_metrics().messages_consumed == 4; }));
  EXPECT_EQ(subscriber->get_subscriber_metrics().dropped_messages, 0u);
}

TEST(ZeroCopy, PolledSubscription) {
  ScopedBusName name;
  auto bus = create_bus(name, zero_copy_config(4));
  auto subscription = bus->subscribe();
  for (int i = 0; i < 4; ++i) ASSERT_EQ(bus->publish(make_payload(0, uint64_t(i), 32), 0), ErrorCode::OK);

  // While the callback runs, its slot is still unavailable to publishers
  EXPECT_EQ(subscription->poll([&](const Message& m) {
    EXPECT_TRUE(check_payload(m.data).valid);
    EXPECT_EQ(bus->publish(as_bytes("no room"), 0), ErrorCode::BACKPRESSURE_TIMEOUT);
  }, 1), 1u);
  EXPECT_EQ(bus->publish(as_bytes("room"), 0), ErrorCode::OK);
}
