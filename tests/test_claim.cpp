// Bus::claim: building messages in place in shared memory
#include <cstring>
#include <thread>

#include <gtest/gtest.h>

#include "redline/bus.hpp"
#include "redline/utils.hpp"
#include "test_helpers.hpp"

using namespace redline;
using namespace redline::test;
using namespace std::chrono_literals;

namespace {
void write_payload(Claim& claim, uint32_t producer, uint64_t counter, uint32_t len, size_t& size) {
  const auto payload = make_payload(producer, counter, len);
  ASSERT_LE(payload.size(), claim.data().size());
  std::memcpy(claim.data().data(), payload.data(), payload.size());
  size = payload.size();
}

std::vector<std::vector<uint8_t>> drain(Subscription& subscription) {
  std::vector<std::vector<uint8_t>> out;
  subscription.poll([&](const Message& m) { out.emplace_back(m.data.begin(), m.data.end()); });
  return out;
}
}

TEST(Claim, CommitPublishesWhatWasWrittenInPlace) {
  ScopedBusName name;
  auto bus = create_bus(name, test_config(BackPressureMode::RELIABLE, 8, 128));
  auto subscription = bus->subscribe();

  Claim claim;
  EXPECT_FALSE(claim);
  ASSERT_EQ(bus->claim(claim), ErrorCode::OK);
  ASSERT_TRUE(claim);
  EXPECT_EQ(claim.data().size(), 128u);
  EXPECT_EQ(claim.sequence(), 0u);

  size_t size = 0;
  write_payload(claim, 1, 42, 100, size);
  const int64_t before_commit = now_ns();
  EXPECT_EQ(claim.commit(size), ErrorCode::OK);
  EXPECT_FALSE(claim);
  EXPECT_TRUE(claim.data().empty());
  EXPECT_EQ(claim.commit(size), ErrorCode::INVALID_ARGUMENT) << "already committed";

  std::vector<uint8_t> payload;
  subscription->poll([&](const Message& m) {
    EXPECT_EQ(m.sequence, 0u);
    EXPECT_GE(m.publish_timestamp_ns, before_commit) << "stamped at commit";
    payload.assign(m.data.begin(), m.data.end());
  });
  EXPECT_EQ(payload, make_payload(1, 42, 100));
}

TEST(Claim, CommittingTooMuchLeavesTheClaimOpen) {
  ScopedBusName name;
  auto bus = create_bus(name, test_config(BackPressureMode::RELIABLE, 8, 64));
  auto subscription = bus->subscribe();
  Claim claim;
  ASSERT_EQ(bus->claim(claim), ErrorCode::OK);
  EXPECT_EQ(claim.commit(65), ErrorCode::MESSAGE_TOO_LARGE);
  ASSERT_TRUE(claim);
  std::memcpy(claim.data().data(), "ok", 2);
  EXPECT_EQ(claim.commit(2), ErrorCode::OK);
  EXPECT_EQ(drain(*subscription), (std::vector<std::vector<uint8_t>>{{'o', 'k'}}));
}

TEST(Claim, AbortedClaimsAreSkippedWithoutCountingAsDropped) {
  ScopedBusName name;
  auto bus = create_bus(name, test_config(BackPressureMode::RELIABLE, 8, 64));
  auto subscription = bus->subscribe();

  {
    Claim explicit_abort;
    ASSERT_EQ(bus->claim(explicit_abort), ErrorCode::OK);
    explicit_abort.abort();
    EXPECT_FALSE(explicit_abort);
    explicit_abort.abort(); // harmless

    Claim abandoned;
    ASSERT_EQ(bus->claim(abandoned), ErrorCode::OK);
    // Reusing a claim aborts the one it held
    ASSERT_EQ(bus->claim(abandoned), ErrorCode::OK);
    EXPECT_EQ(abandoned.sequence(), 2u);
  } // ...and destroying it aborts too

  ASSERT_EQ(bus->publish(as_bytes("real")), ErrorCode::OK);
  std::vector<uint64_t> sequences;
  subscription->poll([&](const Message& m) { sequences.push_back(m.sequence); });
  EXPECT_EQ(sequences, (std::vector<uint64_t>{3}));
  EXPECT_EQ(subscription->get_metrics().dropped_messages, 0u);
  EXPECT_EQ(subscription->get_metrics().messages_consumed, 1u);
}

TEST(Claim, OpenClaimHoldsBackLaterMessagesUntilFinished) {
  ScopedBusName name;
  auto bus = create_bus(name, test_config(BackPressureMode::RELIABLE, 8, 64));
  auto subscription = bus->subscribe();

  Claim claim;
  ASSERT_EQ(bus->claim(claim), ErrorCode::OK);
  ASSERT_EQ(bus->publish(as_bytes("second")), ErrorCode::OK);
  EXPECT_TRUE(drain(*subscription).empty()) << "messages stay in order, so 'second' waits for the claim";
  EXPECT_FALSE(subscription->wait(1ms));

  std::memcpy(claim.data().data(), "first", 5);
  ASSERT_EQ(claim.commit(5), ErrorCode::OK);
  const auto messages = drain(*subscription);
  ASSERT_EQ(messages.size(), 2u);
  EXPECT_EQ(std::string(messages[0].begin(), messages[0].end()), "first");
  EXPECT_EQ(std::string(messages[1].begin(), messages[1].end()), "second");
}

TEST(Claim, SleepingSubscriberIsWokenByCommitAndAbort) {
  ScopedBusName name;
  Config config = test_config(BackPressureMode::RELIABLE, 8, 64);
  config.consumer_busy_spin_duration_us = 0;
  auto bus = create_bus(name, config);
  auto subscription = bus->subscribe();

  for (const bool commit : {true, false}) {
    Claim claim;
    ASSERT_EQ(bus->claim(claim), ErrorCode::OK);
    std::thread finisher([&] {
      std::this_thread::sleep_for(20ms);
      if (commit) {
        EXPECT_EQ(claim.commit(0), ErrorCode::OK);
      } else {
        claim.abort();
      }
    });
    if (commit) {
      const auto start = std::chrono::steady_clock::now();
      EXPECT_TRUE(subscription->wait(5s));
      EXPECT_LT(std::chrono::steady_clock::now() - start, 1s);
    }
    finisher.join();
    drain(*subscription);
  }
  // The aborted sequence doesn't block the next message
  ASSERT_EQ(bus->publish(as_bytes("after")), ErrorCode::OK);
  EXPECT_EQ(drain(*subscription).size(), 1u);
}

TEST(Claim, MovingTransfersTheClaim) {
  ScopedBusName name;
  auto bus = create_bus(name, test_config(BackPressureMode::RELIABLE, 8, 64));
  auto subscription = bus->subscribe();

  Claim original;
  ASSERT_EQ(bus->claim(original), ErrorCode::OK);
  Claim moved(std::move(original));
  EXPECT_FALSE(original);
  ASSERT_TRUE(moved);

  Claim assigned;
  ASSERT_EQ(bus->claim(assigned), ErrorCode::OK); // sequence 1, aborted by the assignment below
  assigned = std::move(moved);
  ASSERT_TRUE(assigned);
  EXPECT_EQ(assigned.sequence(), 0u);

  std::thread([&] {
    std::memcpy(assigned.data().data(), "moved", 5);
    EXPECT_EQ(assigned.commit(5), ErrorCode::OK);
  }).join();

  std::vector<uint64_t> sequences;
  subscription->poll([&](const Message& m) { sequences.push_back(m.sequence); });
  EXPECT_EQ(sequences, (std::vector<uint64_t>{0}));
}

TEST(Claim, WaitsForReliableSubscribers) {
  ScopedBusName name;
  auto bus = create_bus(name, test_config(BackPressureMode::RELIABLE, 4, 64));
  auto subscription = bus->subscribe();
  for (int i = 0; i < 4; ++i) ASSERT_EQ(bus->publish(as_bytes("m"), 0), ErrorCode::OK);

  Claim claim;
  EXPECT_EQ(bus->claim(claim, 0), ErrorCode::BACKPRESSURE_TIMEOUT);
  EXPECT_FALSE(claim);
  EXPECT_EQ(subscription->poll([](const Message&) {}, 1), 1u);
  EXPECT_EQ(bus->claim(claim, 0), ErrorCode::OK);
}

// Producers build messages in place while one subscriber copies and another reads in place: everything arrives
// intact, exactly once, in each producer's order
TEST(Claim, ManyProducersEndToEndZeroCopy) {
  ScopedBusName name;
  constexpr uint32_t producers = 4;
  constexpr uint64_t per_producer = 10'000;
  std::vector<uint64_t> copied_next(producers, 0), in_place_next(producers, 0);
  std::atomic_uint64_t copied{0}, in_place{0}, errors{0};
  const auto checker = [&](std::vector<uint64_t>& next, std::atomic_uint64_t& count) {
    return [&](const Message& m) {
      const auto info = check_payload(m.data);
      if (!info.valid || info.producer >= producers || info.counter != next[info.producer]) {
        ++errors;
      } else {
        ++next[info.producer];
      }
      ++count;
    };
  };

  Config config = test_config(BackPressureMode::RELIABLE, 32, 512);
  auto bus = create_bus(name, config);
  ASSERT_EQ(bus->add_subscriber(checker(copied_next, copied), {.own_thread = true}), ErrorCode::OK);
  ASSERT_EQ(bus->add_subscriber(checker(in_place_next, in_place), {.own_thread = true, .zero_copy_reads = true}), ErrorCode::OK);

  std::vector<std::thread> threads;
  for (uint32_t p = 0; p < producers; ++p) {
    threads.emplace_back([&, p] {
      Claim claim;
      for (uint64_t i = 0; i < per_producer; ++i) {
        ASSERT_EQ(bus->claim(claim, 5'000'000), ErrorCode::OK);
        if (i % 50 == 0) {
          claim.abort(); // gaps must be invisible to subscribers
          ASSERT_EQ(bus->claim(claim, 5'000'000), ErrorCode::OK);
        }
        size_t size = 0;
        write_payload(claim, p, i, 16 + uint32_t(i % 400), size);
        ASSERT_EQ(claim.commit(size), ErrorCode::OK);
      }
    });
  }
  for (auto& t : threads) t.join();

  ASSERT_TRUE(wait_until([&] { return copied == producers * per_producer && in_place == producers * per_producer; }, 10s))
      << copied << " " << in_place;
  EXPECT_EQ(errors.load(), 0u);
}
