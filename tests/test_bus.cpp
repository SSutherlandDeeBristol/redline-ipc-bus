// Behaviour common to both backpressure modes, plus Bus lifecycle and cross-process tests
#include <csignal>
#include <mutex>
#include <sys/wait.h>
#include <thread>

#include <gtest/gtest.h>

#include "redline/bus.hpp"
#include "redline/utils.hpp"
#include "shm/layout.hpp"
#include "test_helpers.hpp"

using namespace redline;
using namespace redline::test;
using namespace std::chrono_literals;

namespace {
// Collects everything a subscriber receives
struct Collector {
  std::mutex mutex;
  std::vector<uint64_t> sequences;
  std::vector<std::vector<uint8_t>> payloads;
  std::vector<int64_t> timestamps;

  std::function<void(const Message&)> callback() {
    return [this](const Message& m) {
      std::lock_guard lk(mutex);
      sequences.push_back(m.sequence);
      payloads.emplace_back(m.data.begin(), m.data.end());
      timestamps.push_back(m.publish_timestamp_ns);
    };
  }

  size_t size() {
    std::lock_guard lk(mutex);
    return sequences.size();
  }
};

class BusAnyMode : public ::testing::TestWithParam<BackPressureMode> {
protected:
  Config config() const { return test_config(GetParam()); }
};

std::string mode_name(const ::testing::TestParamInfo<BackPressureMode>& info) {
  return info.param == BackPressureMode::RELIABLE ? "Reliable" : "BestEffort";
}
}

TEST_P(BusAnyMode, DeliversMessagesInOrderWithContents) {
  ScopedBusName name;
  Collector collector;
  auto bus = make_bus(name, config());
  ASSERT_TRUE(bus->add_subscriber(collector.callback()));

  const int64_t before = now_ns();
  for (uint32_t i = 0; i < 40; ++i) {
    ASSERT_TRUE(bus->publish(make_payload(1, i, 16 + i * 5)));
  }
  ASSERT_TRUE(wait_until([&] { return collector.size() == 40; }));
  const int64_t after = now_ns();

  for (uint32_t i = 0; i < 40; ++i) {
    EXPECT_EQ(collector.sequences[i], i);
    EXPECT_EQ(collector.payloads[i], make_payload(1, i, 16 + i * 5));
    // Timestamps use the same clock as now_ns(), so they can be compared directly
    EXPECT_GE(collector.timestamps[i], before);
    EXPECT_LE(collector.timestamps[i], after);
    if (i > 0) {
      EXPECT_GE(collector.timestamps[i], collector.timestamps[i - 1]);
    }
  }
  EXPECT_EQ(bus->get_subscriber_metrics().dropped_messages, 0u);
}

TEST_P(BusAnyMode, EmptyAndMaximumSizeMessages) {
  ScopedBusName name;
  Collector collector;
  auto bus = make_bus(name, config());
  ASSERT_TRUE(bus->add_subscriber(collector.callback()));

  const std::vector<uint8_t> max(config().max_message_size, 0x5A);
  EXPECT_TRUE(bus->publish(std::span<const uint8_t>{}));
  EXPECT_TRUE(bus->publish(max));
  EXPECT_EQ(bus->publish(std::vector<uint8_t>(config().max_message_size + 1)), std::unexpected(Error::MESSAGE_TOO_LARGE));

  ASSERT_TRUE(wait_until([&] { return collector.size() == 2; }));
  EXPECT_TRUE(collector.payloads[0].empty());
  EXPECT_EQ(collector.payloads[1], max);
  // The rejected message must not have consumed a sequence number
  EXPECT_TRUE(bus->publish(as_bytes("next")));
  ASSERT_TRUE(wait_until([&] { return collector.size() == 3; }));
  EXPECT_EQ(collector.sequences[2], 2u);
}

TEST_P(BusAnyMode, PublishesStrings) {
  ScopedBusName name;
  Collector collector;
  auto bus = make_bus(name, config());
  ASSERT_TRUE(bus->add_subscriber(collector.callback()));

  const std::string owned = "from a std::string";
  ASSERT_TRUE(bus->publish("literal"));
  ASSERT_TRUE(bus->publish(owned));
  ASSERT_TRUE(bus->publish(std::string_view()));
  EXPECT_EQ(bus->publish(std::string(config().max_message_size + 1, 'x')), std::unexpected(Error::MESSAGE_TOO_LARGE));

  ASSERT_TRUE(wait_until([&] { return collector.size() == 3; }));
  const auto text = [&](size_t i) { return std::string(collector.payloads[i].begin(), collector.payloads[i].end()); };
  EXPECT_EQ(text(0), "literal");
  EXPECT_EQ(text(1), owned);
  EXPECT_EQ(text(2), "");
}

TEST_P(BusAnyMode, AllCallbacksOnABusReceiveEveryMessage) {
  ScopedBusName name;
  Collector a, b, c;
  auto bus = make_bus(name, config());
  ASSERT_TRUE(bus->add_subscriber(a.callback()));
  ASSERT_TRUE(bus->add_subscriber(b.callback()));
  ASSERT_TRUE(bus->add_subscriber(c.callback()));

  for (uint32_t i = 0; i < 20; ++i) ASSERT_TRUE(bus->publish(make_payload(0, i, 32)));
  ASSERT_TRUE(wait_until([&] { return a.size() == 20 && b.size() == 20 && c.size() == 20; }));
  EXPECT_EQ(a.payloads, b.payloads);
  EXPECT_EQ(a.payloads, c.payloads);
}

TEST_P(BusAnyMode, EverySubscriberBusReceivesEveryMessage) {
  ScopedBusName name;
  std::vector<std::unique_ptr<Collector>> collectors;
  auto publisher = make_bus(name, config());
  std::vector<std::unique_ptr<Bus>> subscribers;
  for (int i = 0; i < 4; ++i) {
    subscribers.push_back(make_bus(name, config()));
    collectors.push_back(std::make_unique<Collector>());
    ASSERT_TRUE(subscribers.back()->add_subscriber(collectors.back()->callback()));
  }

  for (uint32_t i = 0; i < 50; ++i) ASSERT_TRUE(publisher->publish(make_payload(0, i, 64)));
  for (auto& collector : collectors) {
    ASSERT_TRUE(wait_until([&] { return collector->size() == 50; }));
    EXPECT_EQ(collector->payloads, collectors[0]->payloads);
  }
}

TEST_P(BusAnyMode, LateSubscriberStartsAtHead) {
  ScopedBusName name;
  auto bus = make_bus(name, config());
  for (uint32_t i = 0; i < 10; ++i) ASSERT_TRUE(bus->publish(make_payload(0, i, 16)));

  Collector collector;
  auto late = make_bus(name, config());
  ASSERT_TRUE(late->add_subscriber(collector.callback()));
  ASSERT_TRUE(bus->publish(make_payload(0, 10, 16)));
  ASSERT_TRUE(wait_until([&] { return collector.size() == 1; }));
  std::this_thread::sleep_for(20ms);
  EXPECT_EQ(collector.size(), 1u);
  EXPECT_EQ(collector.sequences[0], 10u);
}

TEST_P(BusAnyMode, ThrowingCallbacksDoNotStopDelivery) {
  ScopedBusName name;
  Collector collector;
  auto bus = make_bus(name, config());
  ASSERT_TRUE(bus->add_subscriber([](const Message& m) {
    if (m.sequence == 0) throw std::runtime_error("std exception");
    if (m.sequence == 1) throw 42; // not a std::exception, used to terminate the process
  }));
  ASSERT_TRUE(bus->add_subscriber(collector.callback()));

  for (uint32_t i = 0; i < 3; ++i) ASSERT_TRUE(bus->publish(make_payload(0, i, 16)));
  ASSERT_TRUE(wait_until([&] { return collector.size() == 3; }));
}

TEST_P(BusAnyMode, MetricsTrackConsumptionAndLatency) {
  ScopedBusName name;
  std::atomic_int received{0};
  auto bus = make_bus(name, config());
  ASSERT_TRUE(bus->add_subscriber([&](const Message&) { ++received; }));

  for (uint32_t i = 0; i < 1000; ++i) {
    ASSERT_TRUE(bus->publish(make_payload(0, i, 16)));
    if (i % 32 == 31) {
      ASSERT_TRUE(wait_until([&] { return received == int(i) + 1; })); // keep BEST_EFFORT from lapping the subscriber
    }
  }
  ASSERT_TRUE(wait_until([&] { return received == 1000; }));

  const auto metrics = bus->get_subscriber_metrics();
  EXPECT_EQ(metrics.messages_consumed, 1000u);
  EXPECT_EQ(metrics.dropped_messages, 0u);
  EXPECT_EQ(metrics.current_lag, 0u);
  EXPECT_GE(metrics.max_lag, 1u);
  EXPECT_GT(metrics.last_latency_ns, 0);
  EXPECT_GT(metrics.p50_latency_ns, 0);
  EXPECT_LE(metrics.p50_latency_ns, metrics.p90_latency_ns);
  EXPECT_LE(metrics.p90_latency_ns, metrics.p99_latency_ns);
  EXPECT_LE(metrics.p99_latency_ns, metrics.p999_latency_ns);
  EXPECT_LT(metrics.p50_latency_ns, 1'000'000'000) << "latency should be far below a second";

  bus->reset_subscriber_metrics();
  const auto reset = bus->get_subscriber_metrics();
  EXPECT_EQ(reset.p50_latency_ns, 0);
  EXPECT_EQ(reset.p999_latency_ns, 0);
  EXPECT_EQ(reset.max_lag, 0u);
  EXPECT_EQ(reset.messages_consumed, 1000u) << "counters are cumulative";
}

INSTANTIATE_TEST_SUITE_P(Modes, BusAnyMode,
                         ::testing::Values(BackPressureMode::RELIABLE, BackPressureMode::BEST_EFFORT), mode_name);

TEST(Bus, RejectsEmptyCallbacks) {
  ScopedBusName name;
  auto bus = make_bus(name, test_config());
  EXPECT_EQ(bus->add_subscriber(std::function<void(const Message&)>{}), std::unexpected(Error::INVALID_ARGUMENT));
  // Neither attempt should have registered this bus as a subscriber
  EXPECT_EQ(bus->get_subscriber_metrics().messages_consumed, 0u);
  auto other = make_bus(name, test_config());
  ASSERT_TRUE(other->add_subscriber([](const Message&) {}));
}

TEST(Bus, NoLimitOnCallbacks) {
  ScopedBusName name;
  constexpr int callbacks = 1000;
  std::atomic_int received{0};
  auto bus = make_bus(name, test_config());
  for (int i = 0; i < callbacks; ++i) {
    ASSERT_TRUE(bus->add_subscriber([&](const Message&) { ++received; })) << i;
  }
  ASSERT_TRUE(bus->publish(as_bytes("to everyone")));
  EXPECT_TRUE(wait_until([&] { return received == callbacks; }));
}

TEST(Bus, CallbacksCanBeAddedWhileMessagesAreDelivered) {
  // Each callback sees a contiguous run of messages from the point it was added
  ScopedBusName name;
  constexpr int callbacks = 50;
  constexpr uint64_t messages = 20'000;
  std::vector<std::unique_ptr<Received>> received;
  for (int i = 0; i < callbacks; ++i) received.push_back(std::make_unique<Received>());
  auto bus = make_bus(name, test_config(BackPressureMode::RELIABLE, 64));

  // The first callback subscribes the Bus, so it must exist before publishing starts to see every message
  ASSERT_TRUE(bus->add_subscriber([sink = received[0].get()](const Message& m) { sink->add(m.sequence); }));
  std::thread publisher([&] {
    for (uint64_t i = 0; i < messages; ++i) ASSERT_TRUE(bus->publish(make_payload(0, i, 16), 5'000'000));
  });
  for (int i = 1; i < callbacks; ++i) {
    auto* sink = received[size_t(i)].get();
    ASSERT_TRUE(bus->add_subscriber([sink](const Message& m) { sink->add(m.sequence); }));
    std::this_thread::sleep_for(std::chrono::microseconds(200));
  }
  publisher.join();

  ASSERT_TRUE(wait_until([&] { const auto v = received[0]->values(); return !v.empty() && v.back() == messages - 1; }));
  for (int i = 0; i < callbacks; ++i) {
    ASSERT_TRUE(wait_until([&] { const auto v = received[size_t(i)]->values(); return v.empty() || v.back() == messages - 1; }));
    const auto v = received[size_t(i)]->values();
    for (size_t k = 1; k < v.size(); ++k) ASSERT_EQ(v[k], v[k - 1] + 1) << "callback " << i;
    if (i > 0 && !v.empty() && !received[size_t(i) - 1]->values().empty()) {
      EXPECT_LE(received[size_t(i) - 1]->values().front(), v.front()) << "added later, so starts no earlier";
    }
  }
  EXPECT_EQ(received[0]->size(), messages);
}

TEST(Bus, MetricsAreZeroWithoutSubscriber) {
  ScopedBusName name;
  std::atomic_int received{0};
  auto subscriber = make_bus(name, test_config());
  ASSERT_TRUE(subscriber->add_subscriber([&](const Message&) { ++received; }));

  auto publisher = make_bus(name, test_config());
  for (int i = 0; i < 5; ++i) ASSERT_TRUE(publisher->publish(as_bytes("x")));
  ASSERT_TRUE(wait_until([&] { return received == 5; }));

  // Used to report whichever subscriber happened to be in registry slot 0
  const auto metrics = publisher->get_subscriber_metrics();
  EXPECT_EQ(metrics.messages_consumed, 0u);
  EXPECT_EQ(metrics.p50_latency_ns, 0);
  publisher->reset_subscriber_metrics(); // must be harmless
}

TEST(Bus, DestroyingPublisherOnlyBusKeepsOtherSubscribers) {
  // A Bus that never subscribed used to deregister registry slot 0 - someone else's subscriber - when destroyed
  ScopedBusName name;
  Collector collector;
  auto subscriber = make_bus(name, test_config());
  ASSERT_TRUE(subscriber->add_subscriber(collector.callback()));

  {
    auto publisher = make_bus(name, test_config());
    ASSERT_TRUE(publisher->publish(as_bytes("one")));
  }

  // If our registration had been wiped, a new subscriber would share it and RELIABLE backpressure would ignore us
  Collector second_collector;
  auto second = make_bus(name, test_config());
  ASSERT_TRUE(second->add_subscriber(second_collector.callback()));

  auto publisher = make_bus(name, test_config());
  ASSERT_TRUE(publisher->publish(as_bytes("two")));
  ASSERT_TRUE(wait_until([&] { return collector.size() == 2 && second_collector.size() == 1; }));
  std::this_thread::sleep_for(20ms);
  EXPECT_EQ(collector.size(), 2u);
  EXPECT_EQ(second_collector.size(), 1u);
  EXPECT_EQ(subscriber->get_subscriber_metrics().messages_consumed, 2u);
  EXPECT_EQ(second->get_subscriber_metrics().messages_consumed, 1u);
}

TEST(Bus, RegistryFullIsReportedAndRecoverable) {
  ScopedBusName name;
  std::atomic_int received{0};
  Config config = test_config();
  config.max_subscribers = 16;
  std::vector<std::unique_ptr<Bus>> buses;
  for (uint32_t i = 0; i < config.max_subscribers; ++i) {
    buses.push_back(make_bus(name, config));
    ASSERT_TRUE(buses.back()->add_subscriber([](const Message&) {})) << i;
  }

  auto overflow = make_bus(name, config);
  EXPECT_EQ(overflow->add_subscriber([](const Message&) {}), std::unexpected(Error::SUBSCRIBER_REGISTRY_FULL));
  // Used to return OK here, with no poll thread behind it
  EXPECT_EQ(overflow->add_subscriber([](const Message&) {}), std::unexpected(Error::SUBSCRIBER_REGISTRY_FULL));
  EXPECT_EQ(overflow->get_subscriber_metrics().messages_consumed, 0u);

  buses.pop_back();
  ASSERT_TRUE(overflow->add_subscriber([&](const Message&) { ++received; }));
  ASSERT_TRUE(overflow->publish(as_bytes("hello")));
  EXPECT_TRUE(wait_until([&] { return received == 1; }));

  // Destroying everything (including the bus that was once rejected) must not touch memory outside the registry
  buses.clear();
  overflow.reset();
}

TEST(Bus, DestructorDoesNotHangWithIdleSubscriber) {
  ScopedBusName name;
  Config config = test_config();
  config.consumer_busy_spin_duration_us = 0;
  auto bus = make_bus(name, config);
  ASSERT_TRUE(bus->add_subscriber([](const Message&) {}));
  std::this_thread::sleep_for(100ms); // let the poll thread go to sleep on the futex

  const auto start = std::chrono::steady_clock::now();
  bus.reset();
  EXPECT_LT(std::chrono::steady_clock::now() - start, 500ms);
}

TEST(Bus, DestructorWithSubscriberDeregisters) {
  ScopedBusName name;
  auto publisher = make_bus(name, test_config(BackPressureMode::RELIABLE, 4));
  {
    auto subscriber = make_bus(name, test_config(BackPressureMode::RELIABLE, 4));
    ASSERT_TRUE(subscriber->add_subscriber([](const Message&) {}));
  }
  // With the subscriber gone nothing can apply backpressure
  for (int i = 0; i < 20; ++i) ASSERT_TRUE(publisher->publish(as_bytes("x"), 0));
}

TEST(Bus, SleepingSubscriberIsWokenByPublish) {
  ScopedBusName name;
  Config config = test_config();
  config.consumer_busy_spin_duration_us = 0;
  std::atomic_int64_t latency_ns{-1};
  auto bus = make_bus(name, config);
  ASSERT_TRUE(bus->add_subscriber([&](const Message& m) { latency_ns = now_ns() - m.publish_timestamp_ns; }));
  std::this_thread::sleep_for(100ms);

  for (int i = 0; i < 20; ++i) {
    latency_ns = -1;
    ASSERT_TRUE(bus->publish(as_bytes("wake up")));
    ASSERT_TRUE(wait_until([&] { return latency_ns >= 0; }, 1000ms));
    // Must be a futex wake, not the 50ms safety-net timeout
    EXPECT_LT(latency_ns.load(), 20'000'000) << "iteration " << i;
    std::this_thread::sleep_for(5ms);
  }
}


TEST(CrossProcess, PublishFromAnotherProcess) {
  ScopedBusName name;
  std::atomic_uint64_t received{0}, bad{0};
  auto bus = make_bus(name, test_config(BackPressureMode::RELIABLE, 64, 256));
  ASSERT_TRUE(bus->add_subscriber([&](const Message& m) {
    const auto info = check_payload(m.data);
    if (!info.valid || info.producer != 99 || info.counter != received) ++bad;
    ++received;
  }));

  const pid_t child = spawn_helper({"publish", name.str(), "64", "256", "20000"});
  const int status = wait_for_exit(child);
  ASSERT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0) << status;
  ASSERT_TRUE(wait_until([&] { return received == 20000; }));
  EXPECT_EQ(bad.load(), 0u);
  EXPECT_EQ(bus->get_subscriber_metrics().dropped_messages, 0u);
}

TEST(CrossProcess, CrashedSubscriberDoesNotBlockReliablePublishers) {
  ScopedBusName name;
  const Config config = test_config(BackPressureMode::RELIABLE, 16, 64);
  auto bus = make_bus(name, config);

  const pid_t child = spawn_helper({"subscribe-and-crash", name.str(), "16", "64"});
  const int status = wait_for_exit(child);
  ASSERT_TRUE(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL) << status;

  // The dead subscriber is still registered and a full ring behind after 16 messages. Publishers must notice its
  // process is gone and reclaim it instead of blocking forever.
  const auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < 64; ++i) {
    ASSERT_TRUE(bus->publish(as_bytes("still flowing"), 2'000'000)) << i;
  }
  EXPECT_LT(std::chrono::steady_clock::now() - start, 1s);
}
