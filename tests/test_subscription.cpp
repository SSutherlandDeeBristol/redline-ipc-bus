// Subscriptions polled by the caller, and callbacks on their own threads
#include <thread>

#include <gtest/gtest.h>

#include "redline/bus.hpp"
#include "shm/layout.hpp"
#include "test_helpers.hpp"

using namespace redline;
using namespace redline::test;
using namespace std::chrono_literals;

TEST(Subscription, PollDeliversReadyMessagesInOrderWithoutBlocking) {
  ScopedBusName name;
  auto bus = create_bus(name, test_config());
  auto subscription = bus->subscribe();
  ASSERT_NE(subscription, nullptr);

  std::vector<uint64_t> counters;
  const auto collect = [&](const Message& m) {
    const auto info = check_payload(m.data);
    ASSERT_TRUE(info.valid);
    counters.push_back(info.counter);
  };

  EXPECT_EQ(subscription->poll(collect), 0u) << "nothing published yet";
  for (uint32_t i = 0; i < 10; ++i) ASSERT_EQ(bus->publish(make_payload(0, i, 40)), ErrorCode::OK);

  EXPECT_EQ(subscription->poll(collect, 3), 3u);
  EXPECT_EQ(subscription->poll(collect), 7u);
  EXPECT_EQ(subscription->poll(collect), 0u);
  EXPECT_EQ(counters, (std::vector<uint64_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));

  const auto metrics = subscription->get_metrics();
  EXPECT_EQ(metrics.messages_consumed, 10u);
  EXPECT_EQ(metrics.current_lag, 0u);
  EXPECT_GT(metrics.p50_latency_ns, 0);
  subscription->reset_metrics();
  EXPECT_EQ(subscription->get_metrics().p50_latency_ns, 0);
}

TEST(Subscription, WaitTimesOutWhenIdleAndWakesOnPublish) {
  ScopedBusName name;
  Config config = test_config();
  config.consumer_busy_spin_duration_us = 0;
  auto bus = create_bus(name, config);
  auto subscription = bus->subscribe();
  ASSERT_NE(subscription, nullptr);

  auto start = std::chrono::steady_clock::now();
  EXPECT_FALSE(subscription->wait(20ms));
  EXPECT_GE(std::chrono::steady_clock::now() - start, 20ms);

  std::thread publisher([&] {
    std::this_thread::sleep_for(20ms);
    EXPECT_EQ(bus->publish(as_bytes("wake")), ErrorCode::OK);
  });
  start = std::chrono::steady_clock::now();
  EXPECT_TRUE(subscription->wait(5s));
  EXPECT_LT(std::chrono::steady_clock::now() - start, 1s) << "woken by the publish, not the timeout";
  publisher.join();

  std::string received;
  EXPECT_EQ(subscription->poll([&](const Message& m) { received = m.data_as_string(); }), 1u);
  EXPECT_EQ(received, "wake");
  EXPECT_TRUE(subscription->wait(0us) == false);
}

TEST(Subscription, StartsAtHeadAndIsIndependentOfOtherSubscribers) {
  ScopedBusName name;
  auto bus = create_bus(name, test_config(BackPressureMode::BEST_EFFORT));
  ASSERT_EQ(bus->publish(as_bytes("before")), ErrorCode::OK);
  auto a = bus->subscribe();
  ASSERT_EQ(bus->publish(as_bytes("one")), ErrorCode::OK);
  auto b = bus->subscribe();
  ASSERT_EQ(bus->publish(as_bytes("two")), ErrorCode::OK);

  std::vector<std::string> from_a, from_b;
  a->poll([&](const Message& m) { from_a.emplace_back(m.data_as_string()); });
  b->poll([&](const Message& m) { from_b.emplace_back(m.data_as_string()); });
  EXPECT_EQ(from_a, (std::vector<std::string>{"one", "two"}));
  EXPECT_EQ(from_b, (std::vector<std::string>{"two"}));
}

TEST(Subscription, AppliesBackpressureUntilDestroyed) {
  ScopedBusName name;
  auto bus = create_bus(name, test_config(BackPressureMode::RELIABLE, 4));
  auto subscription = bus->subscribe();
  for (int i = 0; i < 4; ++i) ASSERT_EQ(bus->publish(as_bytes("m"), 0), ErrorCode::OK);
  EXPECT_EQ(bus->publish(as_bytes("m"), 0), ErrorCode::BACKPRESSURE_TIMEOUT) << "an unpolled subscription holds publishers back";

  EXPECT_EQ(subscription->poll([](const Message&) {}, 1), 1u);
  EXPECT_EQ(bus->publish(as_bytes("m"), 0), ErrorCode::OK);

  subscription.reset();
  for (int i = 0; i < 20; ++i) ASSERT_EQ(bus->publish(as_bytes("m"), 0), ErrorCode::OK);
}

TEST(Subscription, ThrowingCallbackPropagatesAndConsumesTheMessage) {
  ScopedBusName name;
  auto bus = create_bus(name, test_config(BackPressureMode::RELIABLE, 4));
  auto subscription = bus->subscribe();
  for (uint32_t i = 0; i < 3; ++i) ASSERT_EQ(bus->publish(make_payload(0, i, 16)), ErrorCode::OK);

  EXPECT_THROW(subscription->poll([](const Message& m) { if (m.sequence == 1) throw std::runtime_error("boom"); }),
               std::runtime_error);
  std::vector<uint64_t> rest;
  subscription->poll([&](const Message& m) { rest.push_back(m.sequence); });
  EXPECT_EQ(rest, (std::vector<uint64_t>{2}));
}

TEST(Subscription, MayOutliveItsBus) {
  ScopedBusName name;
  auto publisher = create_bus(name, test_config());
  auto creator = create_bus(name, test_config());
  auto subscription = creator->subscribe();
  creator.reset();

  ASSERT_EQ(publisher->publish(as_bytes("still here")), ErrorCode::OK);
  std::string received;
  EXPECT_EQ(subscription->poll([&](const Message& m) { received = m.data_as_string(); }), 1u);
  EXPECT_EQ(received, "still here");
}

TEST(Subscription, NullWhenRegistryFull) {
  ScopedBusName name;
  Config config = test_config();
  config.max_subscribers = 8;
  auto bus = create_bus(name, config);
  std::vector<std::unique_ptr<Subscription>> subscriptions;
  for (uint32_t i = 0; i < config.max_subscribers; ++i) {
    subscriptions.push_back(bus->subscribe());
    ASSERT_NE(subscriptions.back(), nullptr) << i;
  }
  EXPECT_EQ(bus->subscribe(), nullptr);
  EXPECT_EQ(bus->add_subscriber([](const Message&) {}, {.own_thread = true}), ErrorCode::SUBSCRIBER_REGISTRY_FULL);
  subscriptions.pop_back();
  EXPECT_NE(bus->subscribe(), nullptr);
}

TEST(Subscription, RegistryCanHoldHundreds) {
  ScopedBusName name;
  Config config = test_config(BackPressureMode::RELIABLE, 16);
  config.max_subscribers = 600; // past the old fixed 256, but each lease is an fd and soft limits are often 1024
  auto bus = create_bus(name, config);
  std::vector<std::unique_ptr<Subscription>> subscriptions;
  for (uint32_t i = 0; i < config.max_subscribers; ++i) {
    subscriptions.push_back(bus->subscribe());
    ASSERT_NE(subscriptions.back(), nullptr) << i;
  }
  EXPECT_EQ(bus->subscribe(), nullptr);

  // The last one registered still holds publishers back, and still receives
  for (int i = 0; i < 16; ++i) ASSERT_EQ(bus->publish(as_bytes("m"), 0), ErrorCode::OK);
  for (size_t i = 0; i + 1 < subscriptions.size(); ++i) subscriptions[i]->poll([](const Message&) {});
  EXPECT_EQ(bus->publish(as_bytes("m"), 0), ErrorCode::BACKPRESSURE_TIMEOUT);
  EXPECT_EQ(subscriptions.back()->poll([](const Message&) {}), 16u);
  EXPECT_EQ(bus->publish(as_bytes("m"), 0), ErrorCode::OK);
}

TEST(OwnThread, SlowCallbackDoesNotDelayOthers) {
  ScopedBusName name;
  Gate gate;
  std::atomic_int slow_count{0}, shared_count{0}, other_own_count{0};
  auto bus = create_bus(name, test_config(BackPressureMode::BEST_EFFORT, 64));

  ASSERT_EQ(bus->add_subscriber([&](const Message&) {
    gate.wait();
    ++slow_count;
  }, {.own_thread = true}), ErrorCode::OK);
  ASSERT_EQ(bus->add_subscriber([&](const Message&) { ++shared_count; }), ErrorCode::OK);
  ASSERT_EQ(bus->add_subscriber([&](const Message&) { ++other_own_count; }, {.own_thread = true}), ErrorCode::OK);

  for (int i = 0; i < 20; ++i) ASSERT_EQ(bus->publish(as_bytes("m")), ErrorCode::OK);
  EXPECT_TRUE(wait_until([&] { return shared_count == 20 && other_own_count == 20; }));
  EXPECT_EQ(slow_count, 0);

  gate.open();
  EXPECT_TRUE(wait_until([&] { return slow_count == 20; }));
  EXPECT_EQ(bus->get_subscriber_metrics().messages_consumed, 20u) << "covers the shared thread only";
}

TEST(OwnThread, EachCallbackGetsEveryMessageAndExceptionsAreContained) {
  ScopedBusName name;
  Received a, b;
  auto bus = create_bus(name, test_config(BackPressureMode::RELIABLE, 8));
  ASSERT_EQ(bus->add_subscriber([&](const Message& m) {
    a.add(m.sequence);
    if (m.sequence % 2 == 0) throw std::runtime_error("ignored");
  }, {.own_thread = true}), ErrorCode::OK);
  ASSERT_EQ(bus->add_subscriber(create_subscriber([&](const Message& m) { b.add(m.sequence); }), {.own_thread = true}), ErrorCode::OK);
  EXPECT_EQ(bus->add_subscriber(std::function<void(const Message&)>{}, {.own_thread = true}), ErrorCode::INVALID_ARGUMENT);

  for (int i = 0; i < 100; ++i) ASSERT_EQ(bus->publish(as_bytes("m"), 1'000'000), ErrorCode::OK);
  ASSERT_TRUE(wait_until([&] { return a.size() == 100 && b.size() == 100; }));
  EXPECT_EQ(a.values(), b.values());
}

TEST(OwnThread, DestroyingTheBusStopsAndDeregistersThem) {
  ScopedBusName name;
  Config config = test_config(BackPressureMode::RELIABLE, 4);
  config.consumer_busy_spin_duration_us = 0;
  auto publisher = create_bus(name, config);
  {
    auto bus = create_bus(name, config);
    for (int i = 0; i < 3; ++i) ASSERT_EQ(bus->add_subscriber([](const Message&) {}, {.own_thread = true}), ErrorCode::OK);
    std::this_thread::sleep_for(20ms); // let them fall asleep
    const auto start = std::chrono::steady_clock::now();
    bus.reset();
    EXPECT_LT(std::chrono::steady_clock::now() - start, 500ms);
  }
  for (int i = 0; i < 20; ++i) ASSERT_EQ(publisher->publish(as_bytes("m"), 0), ErrorCode::OK);
}
