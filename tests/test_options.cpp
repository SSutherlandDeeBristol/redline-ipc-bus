// open_bus, and the BusOptions and SubscriberOptions that tune a subscriber or the segment's memory
#include <sched.h>
#include <sys/resource.h>
#include <thread>

#include <gtest/gtest.h>

#include "redline/bus.hpp"
#include "redline/logging.hpp"
#include "test_helpers.hpp"

using namespace redline;
using namespace redline::test;
using namespace std::chrono_literals;

namespace {
std::vector<uint64_t> drain(Subscription& subscription) {
  std::vector<uint64_t> sequences;
  subscription.poll([&](const Message& m) { sequences.push_back(m.sequence); });
  return sequences;
}

// A CPU this process may run on
int allowed_cpu() {
  cpu_set_t set;
  CPU_ZERO(&set);
  EXPECT_EQ(sched_getaffinity(0, sizeof(set), &set), 0);
  for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
    if (CPU_ISSET(static_cast<size_t>(cpu), &set)) return cpu;
  }
  return 0;
}
}

TEST(OpenBus, FailsWhenTheBusDoesNotExist) {
  ScopedBusName name;
  EXPECT_EQ(open_bus(name), std::unexpected(Error::NOT_FOUND));
}

TEST(OpenBus, TakesTheLayoutFromTheSegment) {
  ScopedBusName name;
  Config config = test_config(BackPressureMode::RELIABLE, 32, 100);
  config.max_subscribers = 7;
  config.max_publishers = 9;
  auto creator = make_bus(name, config);

  auto opened = open_bus(name);
  ASSERT_TRUE(opened);
  auto& bus = *opened;
  EXPECT_EQ(bus->layout(), config.layout());
  EXPECT_EQ(creator->layout(), config.layout());

  auto subscription = subscribe(*bus);
  ASSERT_TRUE(creator->publish("from the creator"));
  EXPECT_EQ(drain(*subscription), (std::vector<uint64_t>{0}));
  EXPECT_EQ(bus->publish(std::vector<uint8_t>(101)), std::unexpected(Error::MESSAGE_TOO_LARGE));
}

TEST(OpenBus, RejectsInvalidOptions) {
  ScopedBusName name;
  auto creator = make_bus(name, test_config());
  BusOptions options;
  options.latency_histogram_significant_figures = 9;
  set_log_handler({});
  EXPECT_EQ(open_bus(name, options), std::unexpected(Error::INVALID_ARGUMENT));
  set_log_handler(default_log_handler);
}

TEST(OnLapped, ResumeAtOldestKeepsAsMuchAsPossible) {
  ScopedBusName name;
  auto bus = make_bus(name, test_config(BackPressureMode::BEST_EFFORT, 8));
  auto subscription = subscribe(*bus);
  for (int i = 0; i < 30; ++i) ASSERT_TRUE(bus->publish("m"));

  const auto sequences = drain(*subscription);
  ASSERT_FALSE(sequences.empty());
  EXPECT_EQ(sequences.front(), 30u - 8 + 1) << "the oldest message that can still be in the ring";
  EXPECT_EQ(sequences.back(), 29u);
}

TEST(OnLapped, ResumeAtNewestSkipsToTheLatestMessage) {
  ScopedBusName name;
  auto bus = make_bus(name, test_config(BackPressureMode::BEST_EFFORT, 8));
  auto subscription = subscribe(*bus, {.on_lapped = OnLapped::RESUME_AT_NEWEST});
  for (int i = 0; i < 30; ++i) ASSERT_TRUE(bus->publish("m"));

  EXPECT_EQ(drain(*subscription), (std::vector<uint64_t>{29}));
  EXPECT_EQ(subscription->get_metrics().dropped_messages, 29u);

  // Not lapped: nothing is skipped
  for (int i = 0; i < 3; ++i) ASSERT_TRUE(bus->publish("m"));
  EXPECT_EQ(drain(*subscription), (std::vector<uint64_t>{30, 31, 32}));
}

TEST(OnLapped, BusOptionsSetTheDefault) {
  ScopedBusName name;
  Config config = test_config(BackPressureMode::BEST_EFFORT, 8);
  config.on_lapped = OnLapped::RESUME_AT_NEWEST;
  auto bus = make_bus(name, config);
  auto subscription = subscribe(*bus);
  for (int i = 0; i < 20; ++i) ASSERT_TRUE(bus->publish("m"));
  EXPECT_EQ(drain(*subscription), (std::vector<uint64_t>{19}));
}

TEST(LatencyMetrics, CanBeTurnedOff) {
  ScopedBusName name;
  auto bus = make_bus(name, test_config());
  auto with = subscribe(*bus);
  auto without = subscribe(*bus, {.latency_metrics = false});
  for (int i = 0; i < 10; ++i) ASSERT_TRUE(bus->publish("m"));
  drain(*with);
  drain(*without);

  const auto on = with->get_metrics();
  EXPECT_EQ(on.messages_consumed, 10u);
  EXPECT_GT(on.last_latency_ns, 0);
  EXPECT_GT(on.p50_latency_ns, 0);

  const auto off = without->get_metrics();
  EXPECT_EQ(off.messages_consumed, 10u) << "the counters are still kept";
  EXPECT_GT(off.max_lag, 0u);
  EXPECT_EQ(off.last_latency_ns, 0);
  EXPECT_EQ(off.p50_latency_ns, 0);
  EXPECT_EQ(off.p999_latency_ns, 0);
}

TEST(LatencyMetrics, SubscriberWithoutThemIsNotEvictedWhileMakingProgress) {
  // Without latency metrics the heartbeat comes from message counts and poll calls rather than the clock, and must
  // still keep a slow but progressing subscriber from being evicted
  ScopedBusName name;
  Config config = test_config(BackPressureMode::RELIABLE, 8);
  config.subscriber_timeout_us = 20'000;
  config.latency_metrics = false;
  auto bus = make_bus(name, config);
  std::atomic_uint64_t received{0};
  ASSERT_TRUE(bus->add_subscriber([&](const Message&) {
    std::this_thread::sleep_for(2ms);
    ++received;
  }));
  for (int i = 0; i < 40; ++i) ASSERT_TRUE(bus->publish("m", 2'000'000)) << i;
  ASSERT_TRUE(wait_until([&] { return received == 40; }));
  EXPECT_EQ(bus->get_subscriber_metrics().dropped_messages, 0u);
}

TEST(Memory, PrefaultAndHugePages) {
  ScopedBusName name;
  Config config = test_config();
  config.prefault = true;
  config.huge_pages = true; // only advice: fine whether or not the kernel allows shmem huge pages
  set_log_handler({});
  auto bus = make_bus(name, config);
  set_log_handler(default_log_handler);
  ASSERT_NE(bus, nullptr);

  // A second process mapping the same segment prefaults its own mapping
  auto other = open_bus(name, config.options());
  ASSERT_TRUE(other);
  auto subscription = subscribe(**other);
  ASSERT_TRUE(bus->publish("m"));
  EXPECT_EQ(drain(*subscription).size(), 1u);
}

TEST(Memory, LockMemory) {
  ScopedBusName name;
  Config config = test_config(BackPressureMode::RELIABLE, 16, 64);
  config.max_subscribers = 8;
  config.max_publishers = 8;
  config.lock_memory = true;
  rlimit limit{};
  ASSERT_EQ(getrlimit(RLIMIT_MEMLOCK, &limit), 0);
  if (limit.rlim_cur != RLIM_INFINITY && limit.rlim_cur < 1024 * 1024) {
    GTEST_SKIP() << "RLIMIT_MEMLOCK is only " << limit.rlim_cur << " bytes";
  }
  auto bus = make_bus(name, config);
  ASSERT_NE(bus, nullptr);
  EXPECT_TRUE(bus->publish("locked"));
}

TEST(Memory, LockingMoreThanTheLimitFails) {
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
  GTEST_SKIP() << "the sanitizer runtimes replace mlock with a no-op that succeeds";
#endif
  ScopedBusName name;
  rlimit limit{};
  ASSERT_EQ(getrlimit(RLIMIT_MEMLOCK, &limit), 0);
  if (limit.rlim_cur == RLIM_INFINITY || limit.rlim_cur > 64 * 1024 * 1024) {
    GTEST_SKIP() << "RLIMIT_MEMLOCK is too high to exceed cheaply";
  }
  Config config = test_config(BackPressureMode::RELIABLE, 1u << 16, 4096); // 256 MiB
  config.lock_memory = true;
  set_log_handler({});
  EXPECT_EQ(create_bus(name, config.layout(), config.options()), std::unexpected(Error::SYSTEM_ERROR));
  set_log_handler(default_log_handler);
  EXPECT_EQ(destroy_bus(name), std::unexpected(Error::NOT_FOUND)) << "the half-made segment is removed";
}

TEST(CpuPinning, OwnThreadCallbackRunsOnItsCpu) {
  ScopedBusName name;
  auto bus = make_bus(name, test_config());
  const int cpu = allowed_cpu();
  std::atomic_int seen{-1};
  ASSERT_TRUE(bus->add_subscriber([&](const Message&) { seen = sched_getcpu(); }, {.own_thread = true, .cpu = cpu}));
  ASSERT_TRUE(bus->publish("m"));
  ASSERT_TRUE(wait_until([&] { return seen != -1; }));
  EXPECT_EQ(seen.load(), cpu);
}

TEST(CpuPinning, SharedPollThreadRunsOnItsCpu) {
  ScopedBusName name;
  Config config = test_config();
  config.poll_thread_cpu = allowed_cpu();
  auto bus = make_bus(name, config);
  std::atomic_int seen{-1};
  ASSERT_TRUE(bus->add_subscriber([&](const Message&) { seen = sched_getcpu(); }));
  ASSERT_TRUE(bus->publish("m"));
  ASSERT_TRUE(wait_until([&] { return seen != -1; }));
  EXPECT_EQ(seen.load(), *config.poll_thread_cpu);
}

TEST(CpuPinning, InvalidCpusAreRejected) {
  ScopedBusName name;
  auto bus = make_bus(name, test_config());
  set_log_handler({});
  const auto noop = [](const Message&) {};
  EXPECT_EQ(bus->add_subscriber(noop, {.own_thread = true, .cpu = -1}), std::unexpected(Error::INVALID_ARGUMENT));
  EXPECT_EQ(bus->add_subscriber(noop, {.own_thread = true, .cpu = CPU_SETSIZE}), std::unexpected(Error::INVALID_ARGUMENT));
  EXPECT_EQ(bus->add_subscriber(noop, {.cpu = 0}), std::unexpected(Error::INVALID_ARGUMENT)) << "use poll_thread_cpu";
  set_log_handler(default_log_handler);
}

TEST(Errors, HaveNames) {
  EXPECT_EQ(to_string(Error::BACKPRESSURE_TIMEOUT), "BACKPRESSURE_TIMEOUT");
  EXPECT_EQ(to_string(Error::INCOMPATIBLE), "INCOMPATIBLE");
  EXPECT_EQ(to_string(Error::SYSTEM_ERROR), "SYSTEM_ERROR");
}
