#include <set>
#include <sys/wait.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "redline/utils.hpp"
#include "shm/layout.hpp"
#include "shm/segment.hpp"
#include "shm/subscriber_registry.hpp"
#include "test_helpers.hpp"

using namespace redline;
using namespace redline::test;
using shm::SubscriberLease;
using shm::SubscriberState;

namespace {
struct RegistryFixture {
  RegistryFixture()
      : mapping(shm::SegmentMapping::open_or_create("/redline." + name.str(), test_config(BackPressureMode::RELIABLE, 16))),
        registry(mapping),
        segment(mapping->segment()) {}

  ScopedBusName name;
  std::shared_ptr<shm::SegmentMapping> mapping;
  shm::SubscriberRegistry registry;
  shm::SHMSegment* segment;

  SubscriberState state(uint32_t i) const { return SubscriberState(mapping->subscriber(i).state.load()); }
  uint32_t capacity() const { return mapping->subscriber_count(); }

  std::unique_ptr<SubscriberLease> claim(bool evictable = true) {
    auto lease = registry.register_subscriber(evictable);
    EXPECT_NE(lease, nullptr);
    return lease;
  }

  // A registered entry whose owner has "crashed": still OK in shared memory, but nobody holds its lease
  uint32_t crashed() {
    auto lease = claim();
    const uint32_t index = lease->index();
    lease->abandon();
    return index;
  }
};
}

TEST(Registry, ClaimsDistinctEntriesUntilFull) {
  RegistryFixture f;
  std::vector<std::unique_ptr<SubscriberLease>> leases;
  std::set<uint32_t> claimed;
  for (uint32_t i = 0; i < f.capacity(); ++i) {
    leases.push_back(f.registry.register_subscriber());
    ASSERT_NE(leases.back(), nullptr);
    const uint32_t index = leases.back()->index();
    EXPECT_TRUE(claimed.insert(index).second);
    EXPECT_EQ(f.state(index), SubscriberState::OK);
    EXPECT_EQ(f.mapping->subscriber(index).pid.load(), getpid());
  }
  EXPECT_EQ(f.registry.register_subscriber(), nullptr);

  // Destroying a lease deregisters its entry
  const uint32_t freed = leases[17]->index();
  leases[17].reset();
  EXPECT_EQ(f.state(freed), SubscriberState::EMPTY);
  auto again = f.registry.register_subscriber();
  ASSERT_NE(again, nullptr);
  EXPECT_EQ(again->index(), freed);
}

TEST(Registry, NewSubscriberStartsAtHeadWithFreshMetrics) {
  RegistryFixture f;
  // Leave junk behind as a previous occupant would have
  auto& entry = f.mapping->subscriber(0);
  entry.dropped_count = 5;
  entry.messages_consumed = 6;
  entry.max_lag = 7;
  entry.last_latency_ns = 8;
  entry.flags = shm::SUBSCRIBER_NO_EVICT;
  f.segment->write_seq = 42;

  const auto lease = f.claim();
  ASSERT_EQ(lease->index(), 0u);
  EXPECT_EQ(entry.read_seq.load(), 42u);
  EXPECT_EQ(entry.dropped_count.load(), 0u);
  EXPECT_EQ(entry.messages_consumed.load(), 0u);
  EXPECT_EQ(entry.max_lag.load(), 0u);
  EXPECT_EQ(entry.last_latency_ns.load(), 0);
  EXPECT_EQ(entry.flags.load(), 0u);
  EXPECT_GT(entry.heartbeat_ns.load(), 0);
}

TEST(Registry, MinReadSeqOnlyCountsLiveSubscribers) {
  RegistryFixture f;
  EXPECT_FALSE(f.registry.min_read_seq().has_value());

  const auto a = f.claim();
  auto b = f.claim();
  const auto c = f.claim();
  a->entry().read_seq = 10;
  b->entry().read_seq = 5;
  c->entry().read_seq = 1;
  c->entry().state = uint32_t(SubscriberState::DEAD);
  EXPECT_EQ(f.registry.min_read_seq(), std::optional<uint64_t>(5));

  b.reset();
  EXPECT_EQ(f.registry.min_read_seq(), std::optional<uint64_t>(10));
}

TEST(Registry, HighWaterMarkBoundsScans) {
  RegistryFixture f;
  EXPECT_EQ(f.segment->subscriber_high_water.load(), 0u);
  auto a = f.claim();
  auto b = f.claim();
  EXPECT_EQ(f.segment->subscriber_high_water.load(), 2u);
  b.reset();
  EXPECT_EQ(f.segment->subscriber_high_water.load(), 2u) << "never lowered, so racing scans can't miss an entry";

  // An OK entry above the mark (which can't happen for real) is invisible to scans
  f.mapping->subscriber(100).state = uint32_t(SubscriberState::OK);
  f.mapping->subscriber(100).read_seq = 0;
  a->entry().read_seq = 7;
  EXPECT_EQ(f.registry.min_read_seq(), std::optional<uint64_t>(7));
  f.mapping->subscriber(100).state = uint32_t(SubscriberState::EMPTY);
}

TEST(Registry, LeaseHeldOnlyWhileOwnerIsAlive) {
  RegistryFixture f;
  auto lease = f.claim();
  const uint32_t index = lease->index();
  EXPECT_TRUE(f.registry.lease_held(index));
  EXPECT_FALSE(f.registry.lease_held(index + 1));

  lease->abandon();
  EXPECT_FALSE(f.registry.lease_held(index));
}

TEST(Registry, LeasesInOneProcessConflictWithEachOther) {
  // Every lease uses its own file description, so a second registration in the same process (or through another
  // mapping of the same bus) can never take an entry that is already leased
  RegistryFixture f;
  auto other_mapping = shm::SegmentMapping::open_or_create("/redline." + f.name.str(), test_config(BackPressureMode::RELIABLE, 16));
  shm::SubscriberRegistry other(other_mapping);

  auto a = f.claim();
  // Pretend the entry was wrongly left EMPTY: the held lease must still stop anyone claiming it
  a->entry().state = uint32_t(SubscriberState::EMPTY);
  auto b = other.register_subscriber();
  ASSERT_NE(b, nullptr);
  EXPECT_NE(b->index(), a->index());
  EXPECT_TRUE(other.lease_held(a->index()));
  a->entry().state = uint32_t(SubscriberState::OK);
}

TEST(Registry, ReapReclaimsEntriesWhoseOwnerHasGone) {
  RegistryFixture f;
  const auto alive = f.claim();
  const uint32_t dead = f.crashed();
  const uint32_t dead_and_evicted = f.crashed();
  f.mapping->subscriber(dead_and_evicted).state = uint32_t(SubscriberState::DEAD);
  const uint32_t dead_mid_registration = f.crashed();
  f.mapping->subscriber(dead_mid_registration).state = uint32_t(SubscriberState::INITIALISING);

  EXPECT_EQ(f.registry.reap(now_ns(), 0), 3u);
  EXPECT_EQ(f.state(alive->index()), SubscriberState::OK);
  EXPECT_EQ(f.state(dead), SubscriberState::EMPTY);
  EXPECT_EQ(f.state(dead_and_evicted), SubscriberState::EMPTY);
  EXPECT_EQ(f.state(dead_mid_registration), SubscriberState::EMPTY);
  EXPECT_EQ(f.registry.reap(now_ns(), 0), 0u);
}

TEST(Registry, ReapIgnoresThePid) {
  // Pids aren't meaningful across pid namespaces, so a live owner with a pid that doesn't exist here stays registered
  RegistryFixture f;
  const auto lease = f.claim();
  const pid_t pid = fork();
  if (pid == 0) _exit(0);
  waitpid(pid, nullptr, 0);
  lease->entry().pid = pid;

  EXPECT_EQ(f.registry.reap(now_ns(), 0), 0u);
  EXPECT_EQ(f.state(lease->index()), SubscriberState::OK);
}

TEST(Registry, RegisterReapsWhenFull) {
  RegistryFixture f;
  for (uint32_t i = 0; i < f.capacity(); ++i) {
    f.crashed();
  }
  // Every entry belongs to a crashed owner, so registering must reclaim them rather than report full
  EXPECT_NE(f.registry.register_subscriber(), nullptr);
}

TEST(Registry, EvictsStalledSubscribersOnlyWhenConfigured) {
  RegistryFixture f; // 16 slots
  const auto stalled = f.claim();
  const auto behind_but_recent = f.claim();
  const auto caught_up_but_idle = f.claim();
  const auto stalled_but_not_evictable = f.claim(false);
  const int64_t now = now_ns();
  const int64_t timeout = 1'000'000;

  f.segment->write_seq = 100;
  stalled->entry().read_seq = 100 - 16;
  stalled->entry().heartbeat_ns = now - 2 * timeout;
  behind_but_recent->entry().read_seq = 100 - 16;
  behind_but_recent->entry().heartbeat_ns = now;
  caught_up_but_idle->entry().read_seq = 100;
  caught_up_but_idle->entry().heartbeat_ns = now - 2 * timeout;
  stalled_but_not_evictable->entry().read_seq = 100 - 16;
  stalled_but_not_evictable->entry().heartbeat_ns = now - 2 * timeout;

  EXPECT_EQ(f.registry.reap(now, 0), 0u) << "eviction is off by default";
  EXPECT_EQ(f.state(stalled->index()), SubscriberState::OK);

  EXPECT_EQ(f.registry.reap(now, timeout), 1u);
  EXPECT_EQ(f.state(stalled->index()), SubscriberState::DEAD);
  EXPECT_EQ(f.state(behind_but_recent->index()), SubscriberState::OK);
  EXPECT_EQ(f.state(caught_up_but_idle->index()), SubscriberState::OK);
  EXPECT_EQ(f.state(stalled_but_not_evictable->index()), SubscriberState::OK);

  // An evicted subscriber that is still alive keeps its entry until it rejoins
  EXPECT_EQ(f.registry.reap(now, timeout), 0u);
  EXPECT_EQ(f.state(stalled->index()), SubscriberState::DEAD);
  const auto newcomer = f.claim();
  EXPECT_NE(newcomer->index(), stalled->index());
}

TEST(Registry, ConcurrentRegistrationNeverDoubleClaims) {
  RegistryFixture f;
  std::vector<std::thread> threads;
  std::vector<std::vector<std::unique_ptr<SubscriberLease>>> claimed(8);
  for (size_t t = 0; t < claimed.size(); ++t) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < 32; ++i) {
        if (auto lease = f.registry.register_subscriber()) claimed[t].push_back(std::move(lease));
      }
    });
  }
  for (auto& t : threads) t.join();
  std::set<uint32_t> all;
  for (const auto& v : claimed) {
    for (const auto& lease : v) EXPECT_TRUE(all.insert(lease->index()).second) << "entry " << lease->index() << " claimed twice";
  }
  EXPECT_EQ(all.size(), f.capacity());
}

TEST(Registry, ConcurrentReapingAndRegistrationNeverStealLiveEntries) {
  // Reapers in this process share one file description for probing; they must not let each other reclaim an entry
  // that has just been re-registered
  RegistryFixture f;
  std::atomic_bool stop{false};
  std::atomic_uint64_t stolen{0};
  std::vector<std::thread> threads;
  for (int r = 0; r < 2; ++r) {
    threads.emplace_back([&] { while (!stop) f.registry.reap(now_ns(), 0); });
  }
  for (int w = 0; w < 4; ++w) {
    threads.emplace_back([&] {
      for (int i = 0; i < 2000; ++i) {
        auto lease = f.registry.register_subscriber();
        if (!lease) continue;
        for (int k = 0; k < 10; ++k) {
          if (f.state(lease->index()) != SubscriberState::OK) ++stolen;
        }
        if (i % 3 == 0) lease->abandon(); // leave some for the reapers
      }
    });
  }
  for (size_t i = 2; i < threads.size(); ++i) threads[i].join();
  stop = true;
  threads[0].join();
  threads[1].join();
  EXPECT_EQ(stolen.load(), 0u);
}
