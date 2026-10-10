// Each thread that publishes keeps one publisher lease per Bus, rather than taking one for every publish
#include <thread>

#include <gtest/gtest.h>

#include "redline/bus.hpp"
#include "shm/layout.hpp"
#include "shm/segment.hpp"
#include "test_helpers.hpp"

using namespace redline;
using namespace redline::test;

namespace {
// Publisher registry entries in use, as other processes see them
uint32_t leases_in_use(const ScopedBusName& name) {
  auto mapping = shm::SegmentMapping::open("/redline." + name.str(), BusOptions{}).value();
  uint32_t count = 0;
  for (uint32_t i = 0; i < mapping->publisher_count(); ++i) {
    count += mapping->publisher(i).state.load() != uint32_t(shm::PublisherState::EMPTY);
  }
  return count;
}

void publish_from_new_thread(Bus& bus, int count) {
  std::thread([&] {
    for (int i = 0; i < count; ++i) ASSERT_TRUE(bus.publish("m"));
  }).join();
}
}

TEST(PublisherLeases, AThreadKeepsOneLeaseUntilItExits) {
  ScopedBusName name;
  auto bus = make_bus(name, test_config(BackPressureMode::BEST_EFFORT));
  EXPECT_EQ(leases_in_use(name), 0u);

  for (int i = 0; i < 100; ++i) ASSERT_TRUE(bus->publish("m"));
  EXPECT_EQ(leases_in_use(name), 1u);

  // Another thread needs its own while this one holds on to it, and gives it back when it exits for the next to reuse
  publish_from_new_thread(*bus, 100);
  EXPECT_EQ(leases_in_use(name), 2u);
  publish_from_new_thread(*bus, 100);
  EXPECT_EQ(leases_in_use(name), 2u);
}

TEST(PublisherLeases, DestroyingTheBusReleasesThreadsLeases) {
  ScopedBusName name;
  auto keep_open = make_bus(name, test_config(BackPressureMode::BEST_EFFORT));
  {
    auto bus = make_bus(name, test_config(BackPressureMode::BEST_EFFORT));
    ASSERT_TRUE(bus->publish("m"));
    EXPECT_EQ(leases_in_use(name), 1u);
  }
  EXPECT_EQ(leases_in_use(name), 0u);

  // This thread's cache still mentions the destroyed Bus; a new one (possibly at the same address) must not be confused
  for (int round = 0; round < 20; ++round) {
    auto bus = make_bus(name, test_config(BackPressureMode::BEST_EFFORT));
    ASSERT_TRUE(bus->publish("m"));
    EXPECT_EQ(leases_in_use(name), 1u) << round;
  }
  EXPECT_EQ(leases_in_use(name), 0u);
}

TEST(PublisherLeases, AThreadPublishingOnManyBusesStillWorks) {
  // More buses than a thread caches leases for: it cycles through them, giving leases back as it goes
  ScopedBusName name;
  std::vector<std::unique_ptr<Bus>> buses;
  for (int i = 0; i < 20; ++i) buses.push_back(make_bus(name, test_config(BackPressureMode::BEST_EFFORT)));
  auto subscription = subscribe(*buses[0]);
  for (int round = 0; round < 3; ++round) {
    for (auto& bus : buses) ASSERT_TRUE(bus->publish("m"));
  }
  EXPECT_LE(leases_in_use(name), 20u);
  uint64_t received = 0;
  subscription->poll([&](const Message&) { ++received; });
  EXPECT_EQ(received, 60u);
}

TEST(PublisherLeases, ClaimsTakeTheirOwnLease) {
  ScopedBusName name;
  Config config = test_config(BackPressureMode::BEST_EFFORT);
  config.max_publishers = 2;
  auto bus = make_bus(name, config);
  ASSERT_TRUE(bus->publish("m")); // this thread's lease

  auto claim = bus->claim();
  ASSERT_TRUE(claim);
  EXPECT_TRUE(bus->publish("while claimed")) << "the thread's own lease is still free";
  EXPECT_EQ(bus->claim(), std::unexpected(Error::PUBLISHER_REGISTRY_FULL));
  ASSERT_TRUE(claim->commit(0));
  EXPECT_TRUE(bus->claim());
}
