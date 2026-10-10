// Publishers that die mid-publish: their unfinished sequences are recovered, and live publishers are never touched
#include <csignal>
#include <cstring>
#include <thread>

#include <gtest/gtest.h>

#include "redline/bus.hpp"
#include "redline/utils.hpp"
#include "shm/layout.hpp"
#include "shm/publisher_registry.hpp"
#include "shm/ring_buffer.hpp"
#include "shm/segment.hpp"
#include "test_helpers.hpp"

using namespace redline;
using namespace redline::test;
using namespace std::chrono_literals;
using shm::PublisherLease;
using shm::PublisherState;
using ReadResult = shm::RingBuffer::ReadResult;

namespace {
// Direct access to a bus' segment, to play the part of a publisher that dies at a chosen point
struct Internals {
  Internals(const ScopedBusName& name, const Config& config)
      : mapping(shm::SegmentMapping::open_or_create("/redline." + name.str(), config.layout(), config.options()).value()),
        segment(mapping->segment()),
        registry(mapping),
        ring(mapping) {}

  std::shared_ptr<shm::SegmentMapping> mapping;
  shm::SHMSegment* segment;
  shm::PublisherRegistry registry;
  shm::RingBuffer ring;

  // Reserves the next sequence the way Bus does, optionally claims its slot, then "crashes"
  uint64_t die_after_reserving(bool claim_slot) {
    auto lease = registry.register_publisher().value_or(nullptr);
    EXPECT_NE(lease, nullptr);
    uint64_t seq = segment->write_seq.load();
    lease->reserving(seq, 1);
    EXPECT_TRUE(segment->write_seq.compare_exchange_strong(seq, seq + 1));
    if (claim_slot) {
      uint8_t* payload = ring.begin_write(seq);
      EXPECT_NE(payload, nullptr);
      std::memset(payload, 0xAB, 8);
    }
    lease->abandon();
    return seq;
  }

  ReadResult read(uint64_t seq) {
    std::vector<uint8_t> out(segment->slot_size_bytes);
    return ring.read(seq, out.data()).result;
  }

  PublisherState state(const PublisherLease& lease) const { return PublisherState(lease.entry().state.load()); }
};
}

TEST(PublisherRecovery, FinishesSequenceReservedByDeadPublisher) {
  ScopedBusName name;
  Internals in(name, test_config());
  const uint64_t seq = in.die_after_reserving(false);
  EXPECT_EQ(in.read(seq), ReadResult::NOT_READY);

  EXPECT_EQ(in.registry.recover(), 1u);
  EXPECT_EQ(in.read(seq), ReadResult::LOST);
  EXPECT_EQ(in.segment->recovered_sequences.load(), 1u);
  EXPECT_EQ(in.registry.recover(), 0u) << "nothing left to do";
}

TEST(PublisherRecovery, FinishesSlotDeadPublisherWasWriting) {
  ScopedBusName name;
  Internals in(name, test_config());
  const uint64_t seq = in.die_after_reserving(true);
  EXPECT_EQ(in.read(seq), ReadResult::NOT_READY);

  EXPECT_EQ(in.registry.recover(), 1u);
  EXPECT_EQ(in.read(seq), ReadResult::LOST);
  // The next lap can use the slot again
  EXPECT_TRUE(in.ring.write(seq + in.segment->slot_count, as_bytes("next lap"), now_ns()));
}

TEST(PublisherRecovery, FreesDeadEntriesSoTheRegistryDoesNotFillUp) {
  ScopedBusName name;
  Config config = test_config();
  config.max_publishers = 2;
  Internals in(name, config);
  in.die_after_reserving(false);
  in.die_after_reserving(true);
  // Full of dead publishers: registering recovers them
  auto lease = in.registry.register_publisher().value_or(nullptr);
  ASSERT_NE(lease, nullptr);
  EXPECT_EQ(in.segment->recovered_sequences.load(), 2u);
}

TEST(PublisherRecovery, NeverTouchesALivePublisher) {
  ScopedBusName name;
  Internals in(name, test_config());
  auto reserved_only = in.registry.register_publisher().value_or(nullptr);
  auto writing = in.registry.register_publisher().value_or(nullptr);

  uint64_t first = in.segment->write_seq.load();
  reserved_only->reserving(first, 1);
  ASSERT_TRUE(in.segment->write_seq.compare_exchange_strong(first, first + 1));
  uint64_t second = in.segment->write_seq.load();
  writing->reserving(second, 1);
  ASSERT_TRUE(in.segment->write_seq.compare_exchange_strong(second, second + 1));
  uint8_t* payload = in.ring.begin_write(second);
  ASSERT_NE(payload, nullptr);

  EXPECT_EQ(in.registry.recover(), 0u);
  EXPECT_EQ(in.read(first), ReadResult::NOT_READY);
  EXPECT_EQ(in.read(second), ReadResult::NOT_READY);

  // ...and both can still finish normally
  EXPECT_TRUE(in.ring.write(first, as_bytes("first"), now_ns()));
  in.ring.fill_and_commit(second, payload, as_bytes("second"), now_ns());
  EXPECT_EQ(in.read(first), ReadResult::OK);
  EXPECT_EQ(in.read(second), ReadResult::OK);
}

TEST(PublisherRecovery, DeadRecordOverlappingALiveReservationIsLeftAlone) {
  // A publisher that died after recording an attempt that lost the race: the sequence it named belongs to a live one
  ScopedBusName name;
  Internals in(name, test_config());
  auto live = in.registry.register_publisher().value_or(nullptr);
  auto dead = in.registry.register_publisher().value_or(nullptr);

  uint64_t seq = in.segment->write_seq.load();
  dead->reserving(seq, 1);
  live->reserving(seq, 1);
  ASSERT_TRUE(in.segment->write_seq.compare_exchange_strong(seq, seq + 1));
  dead->abandon();

  EXPECT_EQ(in.registry.recover(), 0u);
  EXPECT_EQ(in.read(seq), ReadResult::NOT_READY);
  EXPECT_EQ(in.state(*dead), PublisherState::OK) << "kept until the overlap is resolved";

  ASSERT_TRUE(in.ring.write(seq, as_bytes("live"), now_ns()));
  live->done();
  EXPECT_EQ(in.registry.recover(), 0u);
  EXPECT_EQ(in.read(seq), ReadResult::OK);
  EXPECT_EQ(in.state(*dead), PublisherState::EMPTY);
}

TEST(PublisherRecovery, DeadRecordThatNeverReservedIsJustFreed) {
  ScopedBusName name;
  Internals in(name, test_config());
  auto lease = in.registry.register_publisher().value_or(nullptr);
  lease->reserving(in.segment->write_seq.load(), 3); // the compare-and-swap never happened
  lease->abandon();

  EXPECT_EQ(in.registry.recover(), 0u);
  EXPECT_EQ(in.state(*lease), PublisherState::EMPTY);
  EXPECT_EQ(in.segment->write_seq.load(), 0u);
}

TEST(PublisherRecovery, DeadPublishersOnTwoLapsOfOneSlot) {
  ScopedBusName name;
  Internals in(name, test_config(BackPressureMode::BEST_EFFORT, 4));
  const uint64_t first = in.die_after_reserving(true); // slot left odd
  for (uint64_t seq = 1; seq < 4; ++seq) {
    in.segment->write_seq = seq + 1;
    ASSERT_TRUE(in.ring.write(seq, as_bytes("m"), now_ns()));
  }
  const uint64_t second = in.die_after_reserving(false); // same slot, a lap later, stuck behind the first
  ASSERT_EQ(second, first + 4);

  EXPECT_EQ(in.registry.recover(), 2u);
  EXPECT_EQ(in.read(second), ReadResult::LOST);
}

TEST(PublisherRecovery, StuckSubscriberRecoversAndCarriesOn) {
  ScopedBusName name;
  const Config config = test_config(BackPressureMode::RELIABLE, 8);
  Received received;
  auto bus = make_bus(name, config);
  ASSERT_TRUE(bus->add_subscriber([&](const Message& m) { received.add(check_payload(m.data).counter); }));

  ASSERT_TRUE(bus->publish(make_payload(0, 0, 16)));
  ASSERT_TRUE(wait_until([&] { return received.size() == 1; }));
  Internals in(name, config);
  in.die_after_reserving(true);

  // More than a ring's worth: RELIABLE publishing only gets through if the subscriber gets past the dead sequence
  const auto start = std::chrono::steady_clock::now();
  for (uint64_t i = 1; i <= 50; ++i) ASSERT_TRUE(bus->publish(make_payload(0, i, 16), 2'000'000)) << i;
  ASSERT_TRUE(wait_until([&] { return received.size() == 51; }));
  EXPECT_LT(std::chrono::steady_clock::now() - start, 1s);

  const auto values = received.values();
  for (uint64_t i = 0; i < 51; ++i) EXPECT_EQ(values[i], i);
  EXPECT_EQ(bus->get_subscriber_metrics().dropped_messages, 1u) << "the dead publisher's message";
}

TEST(PublisherRecovery, PublisherLappingADeadWriterRecovers) {
  ScopedBusName name;
  const Config config = test_config(BackPressureMode::BEST_EFFORT, 4);
  auto bus = make_bus(name, config);
  Internals in(name, config);
  in.die_after_reserving(true);

  // No subscribers to notice: the publisher reaching the stuck slot a lap later has to
  const auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < 20; ++i) ASSERT_TRUE(bus->publish(as_bytes("m"), 0));
  EXPECT_LT(std::chrono::steady_clock::now() - start, 1s);
  EXPECT_EQ(in.segment->recovered_sequences.load(), 1u);
}

TEST(PublisherRecovery, SlowClaimIsNeverTreatedAsDead) {
  ScopedBusName name;
  Received received;
  auto bus = make_bus(name, test_config(BackPressureMode::RELIABLE, 8));
  auto subscription = subscribe(*bus);

  Claim claim;
  ASSERT_TRUE(claim_into(*bus, claim));
  ASSERT_TRUE(bus->publish(as_bytes("behind the claim")));
  // Far longer than the stall threshold, with the subscriber waiting on the claim the whole time
  EXPECT_FALSE(subscription->wait(100ms));

  std::memcpy(claim.data().data(), "claimed", 7);
  ASSERT_TRUE(claim.commit(7));
  std::vector<std::string> messages;
  subscription->poll([&](const Message& m) { messages.emplace_back(m.data_as_string()); });
  EXPECT_EQ(messages, (std::vector<std::string>{"claimed", "behind the claim"}));
  EXPECT_EQ(subscription->get_metrics().dropped_messages, 0u);
}

TEST(PublisherRecovery, PublisherRegistryFull) {
  ScopedBusName name;
  Config config = test_config();
  config.max_publishers = 2;
  auto bus = make_bus(name, config);
  Claim a, b;
  ASSERT_TRUE(claim_into(*bus, a));
  ASSERT_TRUE(claim_into(*bus, b));
  EXPECT_EQ(bus->publish(as_bytes("no lease left")), std::unexpected(Error::PUBLISHER_REGISTRY_FULL));
  ASSERT_TRUE(a.commit(0));
  EXPECT_TRUE(bus->publish(as_bytes("reuses a's lease")));
}

TEST(PublisherRecovery, ConstantRecoveryNeverDisturbsLivePublishers) {
  ScopedBusName name;
  constexpr uint32_t producers = 4;
  constexpr uint64_t per_producer = 20'000;
  const Config config = test_config(BackPressureMode::RELIABLE, 32, 128);
  std::vector<uint64_t> next_expected(producers, 0);
  std::atomic_uint64_t received{0}, errors{0};
  auto bus = make_bus(name, config);
  ASSERT_TRUE(bus->add_subscriber([&](const Message& m) {
    const auto info = check_payload(m.data);
    if (!info.valid || info.producer >= producers || info.counter != next_expected[info.producer]) {
      ++errors;
    } else {
      ++next_expected[info.producer];
    }
    ++received;
  }));

  Internals in(name, config);
  std::atomic_bool stop{false};
  std::thread recoverer([&] { while (!stop) in.registry.recover(); });

  std::vector<std::thread> threads;
  for (uint32_t p = 0; p < producers; ++p) {
    threads.emplace_back([&, p] {
      Claim claim;
      for (uint64_t i = 0; i < per_producer; ++i) {
        const auto payload = make_payload(p, i, 16 + uint32_t(i % 100));
        if (i % 2 == 0) {
          ASSERT_TRUE(bus->publish(payload, 5'000'000));
        } else {
          ASSERT_TRUE(claim_into(*bus, claim, 5'000'000));
          std::memcpy(claim.data().data(), payload.data(), payload.size());
          ASSERT_TRUE(claim.commit(payload.size()));
        }
      }
    });
  }
  for (auto& t : threads) t.join();
  ASSERT_TRUE(wait_until([&] { return received == producers * per_producer; }, 10s));
  stop = true;
  recoverer.join();
  EXPECT_EQ(errors.load(), 0u);
  EXPECT_EQ(bus->get_subscriber_metrics().dropped_messages, 0u);
  EXPECT_EQ(in.segment->recovered_sequences.load(), 0u);
}

TEST(CrossProcess, PublisherKilledMidClaimDoesNotStallTheBus) {
  ScopedBusName name;
  const Config config = test_config(BackPressureMode::RELIABLE, 16, 64);
  auto bus = make_bus(name, config);
  auto subscription = subscribe(*bus);

  const pid_t child = spawn_helper({"claim-and-crash", name.str(), "16", "64"});
  const int status = wait_for_exit(child);
  ASSERT_TRUE(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL) << status;

  for (int i = 0; i < 40; ++i) {
    ASSERT_TRUE(bus->publish(make_payload(1, uint64_t(i), 32), 2'000'000)) << i;
    // Keep up, so RELIABLE publishing depends on the subscriber getting past the dead claim
    while (subscription->poll([](const Message& m) { EXPECT_TRUE(check_payload(m.data).valid); }) == 0 &&
           subscription->get_metrics().current_lag > 8) {
      subscription->wait(10ms);
    }
  }
  ASSERT_TRUE(wait_until([&] {
    subscription->poll([](const Message&) {});
    return subscription->get_metrics().messages_consumed == 40;
  }));
  EXPECT_EQ(subscription->get_metrics().dropped_messages, 1u);
}
