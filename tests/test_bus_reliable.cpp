// RELIABLE mode: publishers never overwrite a message a live subscriber hasn't consumed
#include <thread>

#include <gtest/gtest.h>

#include "redline/bus.hpp"
#include "test_helpers.hpp"

using namespace redline;
using namespace redline::test;
using namespace std::chrono_literals;

TEST(Reliable, NoSubscribersNeverBlocks) {
  ScopedBusName name;
  auto bus = create_bus(name, test_config(BackPressureMode::RELIABLE, 8));
  for (int i = 0; i < 1000; ++i) {
    ASSERT_EQ(bus->publish(as_bytes("nobody listening"), 0), ErrorCode::OK) << i;
  }
}

TEST(Reliable, StalledSubscriberCausesBackpressureTimeoutWithoutLoss) {
  ScopedBusName name;
  constexpr uint32_t slots = 16;
  Gate gate;
  Received received;
  auto publisher = create_bus(name, test_config(BackPressureMode::RELIABLE, slots));
  auto subscriber = create_bus(name, test_config(BackPressureMode::RELIABLE, slots));
  ASSERT_EQ(subscriber->add_subscriber([&](const Message& m) {
    if (m.sequence == 0) gate.wait();
    received.add(check_payload(m.data).counter);
  }), ErrorCode::OK);

  ASSERT_EQ(publisher->publish(make_payload(0, 0, 16), 0), ErrorCode::OK);
  ASSERT_TRUE(wait_until([&] { return gate.has_waiter(); }));

  // Message 0 has been copied out and its slot released, so the ring can hold `slots` more
  uint64_t published = 1;
  while (publisher->publish(make_payload(0, published, 16), 1000) == ErrorCode::OK) {
    ++published;
    ASSERT_LE(published, slots + 1) << "overran a subscriber that hasn't consumed its messages";
  }
  EXPECT_EQ(published, slots + 1);

  // A zero timeout returns straight away
  const auto start = std::chrono::steady_clock::now();
  EXPECT_EQ(publisher->publish(make_payload(0, published, 16), 0), ErrorCode::BACKPRESSURE_TIMEOUT);
  EXPECT_LT(std::chrono::steady_clock::now() - start, 50ms);

  gate.open();
  ASSERT_TRUE(wait_until([&] { return received.size() == published; }));
  const auto values = received.values();
  for (uint64_t i = 0; i < published; ++i) EXPECT_EQ(values[i], i);
  EXPECT_EQ(subscriber->get_subscriber_metrics().dropped_messages, 0u);

  // ...and publishing resumes
  EXPECT_EQ(publisher->publish(make_payload(0, published, 16), 1'000'000), ErrorCode::OK);
}

TEST(Reliable, BlockingPublishResumesWhenSubscriberCatchesUp) {
  ScopedBusName name;
  std::atomic_int received{0};
  auto bus = create_bus(name, test_config(BackPressureMode::RELIABLE, 4));
  ASSERT_EQ(bus->add_subscriber([&](const Message&) {
    std::this_thread::sleep_for(200us);
    ++received;
  }), ErrorCode::OK);

  // Far more messages than slots, with no timeout: every publish must eventually succeed
  for (int i = 0; i < 200; ++i) ASSERT_EQ(bus->publish(as_bytes("slowly"), -1), ErrorCode::OK);
  ASSERT_TRUE(wait_until([&] { return received == 200; }));
  EXPECT_EQ(bus->get_subscriber_metrics().dropped_messages, 0u);
}

// Several threads publishing at once used to be able to pass the capacity check together and overrun subscribers
TEST(Reliable, MultipleProducersDeliverEverythingExactlyOnceInOrder) {
  ScopedBusName name;
  constexpr uint32_t producers = 4;
  constexpr uint64_t per_producer = 20'000;
  std::vector<uint64_t> next_expected(producers, 0);
  std::atomic_uint64_t received{0}, errors{0};
  auto bus = create_bus(name, test_config(BackPressureMode::RELIABLE, 32, 128));
  ASSERT_EQ(bus->add_subscriber([&](const Message& m) {
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
        ASSERT_EQ(bus->publish(make_payload(p, i, 16 + uint32_t(i % 100)), 5'000'000), ErrorCode::OK);
      }
    });
  }
  for (auto& t : threads) t.join();

  ASSERT_TRUE(wait_until([&] { return received == producers * per_producer; }, 10s)) << received;
  EXPECT_EQ(errors.load(), 0u);
  EXPECT_EQ(bus->get_subscriber_metrics().dropped_messages, 0u);
  for (uint32_t p = 0; p < producers; ++p) EXPECT_EQ(next_expected[p], per_producer);
}

TEST(Reliable, SlowestSubscriberThrottlesButEveryoneGetsEverything) {
  ScopedBusName name;
  const Config config = test_config(BackPressureMode::RELIABLE, 8);
  std::atomic_int fast_count{0}, slow_count{0};
  auto publisher = create_bus(name, config);
  auto fast = create_bus(name, config);
  auto slow = create_bus(name, config);
  ASSERT_EQ(fast->add_subscriber([&](const Message&) { ++fast_count; }), ErrorCode::OK);
  ASSERT_EQ(slow->add_subscriber([&](const Message&) {
    std::this_thread::sleep_for(50us);
    ++slow_count;
  }), ErrorCode::OK);

  for (int i = 0; i < 500; ++i) ASSERT_EQ(publisher->publish(as_bytes("m"), 5'000'000), ErrorCode::OK);
  ASSERT_TRUE(wait_until([&] { return fast_count == 500 && slow_count == 500; }));
  EXPECT_EQ(fast->get_subscriber_metrics().dropped_messages, 0u);
  EXPECT_EQ(slow->get_subscriber_metrics().dropped_messages, 0u);
}

TEST(Reliable, StalledSubscriberIsEvictedWhenTimeoutConfigured) {
  ScopedBusName name;
  Config config = test_config(BackPressureMode::RELIABLE, 8);
  config.subscriber_timeout_us = 50'000;
  Gate gate;
  Received received;
  auto publisher = create_bus(name, config);
  auto subscriber = create_bus(name, config);
  ASSERT_EQ(subscriber->add_subscriber([&](const Message& m) {
    if (m.sequence == 0) gate.wait();
    received.add(m.sequence);
  }), ErrorCode::OK);

  // Unlimited timeout: without eviction this would block forever
  const auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < 100; ++i) ASSERT_EQ(publisher->publish(as_bytes("m"), -1), ErrorCode::OK);
  EXPECT_LT(std::chrono::steady_clock::now() - start, 2s);

  // Once unstuck, the subscriber notices it was evicted, skips ahead and keeps receiving new messages
  gate.open();
  ASSERT_TRUE(wait_until([&] { return subscriber->get_subscriber_metrics().dropped_messages > 0; }));
  ASSERT_EQ(publisher->publish(as_bytes("after eviction"), 1'000'000), ErrorCode::OK);
  ASSERT_TRUE(wait_until([&] { const auto v = received.values(); return !v.empty() && v.back() == 100; }));
  EXPECT_EQ(received.values(), (std::vector<uint64_t>{0, 100}));

  // Every sequence up to the head was either delivered or counted as dropped
  const auto metrics = subscriber->get_subscriber_metrics();
  EXPECT_EQ(metrics.messages_consumed + metrics.dropped_messages, 101u);
}

TEST(Reliable, StalledSubscriberIsNotEvictedByDefault) {
  ScopedBusName name;
  const Config config = test_config(BackPressureMode::RELIABLE, 8);
  Gate gate;
  auto publisher = create_bus(name, config);
  auto subscriber = create_bus(name, config);
  ASSERT_EQ(subscriber->add_subscriber([&](const Message& m) { if (m.sequence == 0) gate.wait(); }), ErrorCode::OK);
  ASSERT_EQ(publisher->publish(as_bytes("m"), 0), ErrorCode::OK);
  ASSERT_TRUE(wait_until([&] { return gate.has_waiter(); }));

  ErrorCode last = ErrorCode::OK;
  for (int i = 0; i < 20 && last == ErrorCode::OK; ++i) last = publisher->publish(as_bytes("m"), 200'000);
  EXPECT_EQ(last, ErrorCode::BACKPRESSURE_TIMEOUT);

  gate.open();
  ASSERT_TRUE(wait_until([&] { return subscriber->get_subscriber_metrics().messages_consumed == 9; }));
  EXPECT_EQ(subscriber->get_subscriber_metrics().dropped_messages, 0u);
}
