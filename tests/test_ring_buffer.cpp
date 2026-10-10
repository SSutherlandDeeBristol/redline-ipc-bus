#include <atomic>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "shm/layout.hpp"
#include "shm/ring_buffer.hpp"
#include "shm/segment.hpp"
#include "test_helpers.hpp"

using namespace redline;
using namespace redline::test;
using ReadResult = shm::RingBuffer::ReadResult;

namespace {
struct RingFixture {
  explicit RingFixture(uint32_t slots = 8, uint32_t slot_size = 128)
      : mapping(shm::SegmentMapping::open_or_create("/redline." + name.str(), test_config(BackPressureMode::BEST_EFFORT, slots, slot_size).layout(), test_config(BackPressureMode::BEST_EFFORT, slots, slot_size).options()).value()),
        ring(mapping),
        out(slot_size) {}

  ScopedBusName name;
  std::shared_ptr<shm::SegmentMapping> mapping;
  shm::RingBuffer ring;
  std::vector<uint8_t> out;
};
}

TEST(RingBuffer, NothingReadableBeforeWrite) {
  RingFixture f;
  EXPECT_EQ(f.ring.read(0, f.out.data()).result, ReadResult::NOT_READY);
  EXPECT_FALSE(f.ring.is_ready(0));
}

TEST(RingBuffer, RoundTripsMessage) {
  RingFixture f;
  const auto payload = make_payload(3, 0, 100);
  ASSERT_TRUE(f.ring.write(0, payload, 12345));
  EXPECT_TRUE(f.ring.is_ready(0));
  EXPECT_FALSE(f.ring.is_ready(1));

  const auto outcome = f.ring.read(0, f.out.data());
  ASSERT_EQ(outcome.result, ReadResult::OK);
  EXPECT_EQ(outcome.size, 100u);
  EXPECT_EQ(outcome.publish_timestamp_ns, 12345);
  EXPECT_TRUE(std::equal(payload.begin(), payload.end(), f.out.begin()));
}

TEST(RingBuffer, ZeroLengthMessage) {
  RingFixture f;
  ASSERT_TRUE(f.ring.write(0, {}, 1));
  const auto outcome = f.ring.read(0, f.out.data());
  EXPECT_EQ(outcome.result, ReadResult::OK);
  EXPECT_EQ(outcome.size, 0u);
}

TEST(RingBuffer, NextLapIsNotReadyUntilWritten) {
  RingFixture f(8);
  ASSERT_TRUE(f.ring.write(0, make_payload(0, 0, 16), 1));
  // Sequence 8 shares slot 0, but slot 0 still holds sequence 0
  EXPECT_EQ(f.ring.read(8, f.out.data()).result, ReadResult::NOT_READY);
}

TEST(RingBuffer, DetectsOverwriteByLaterLap) {
  RingFixture f(8);
  ASSERT_TRUE(f.ring.write(0, make_payload(0, 0, 16), 1));
  ASSERT_TRUE(f.ring.write(8, make_payload(0, 8, 16), 2));
  EXPECT_EQ(f.ring.read(0, f.out.data()).result, ReadResult::OVERWRITTEN);
  EXPECT_TRUE(f.ring.is_ready(0)) << "an overwritten sequence must not leave a reader waiting";

  const auto outcome = f.ring.read(8, f.out.data());
  ASSERT_EQ(outcome.result, ReadResult::OK);
  EXPECT_EQ(check_payload({f.out.data(), outcome.size}).counter, 8u);
}

TEST(RingBuffer, StaleWriterCannotClobberNewerLap) {
  RingFixture f(8);
  ASSERT_TRUE(f.ring.write(8, make_payload(0, 8, 16), 2));
  // A producer that reserved sequence 0 but was descheduled for a whole lap must not overwrite sequence 8
  EXPECT_FALSE(f.ring.write(0, make_payload(0, 0, 16), 1));
  const auto outcome = f.ring.read(8, f.out.data());
  ASSERT_EQ(outcome.result, ReadResult::OK);
  EXPECT_EQ(check_payload({f.out.data(), outcome.size}).counter, 8u);
}

TEST(RingBuffer, InProgressWriteIsNotReadable) {
  RingFixture f(8);
  // Simulate a producer that has claimed sequence 0 but not committed it
  f.mapping->header_at(0)->version.store(shm::writing_version(0));
  EXPECT_EQ(f.ring.read(0, f.out.data()).result, ReadResult::NOT_READY);
  EXPECT_FALSE(f.ring.is_ready(0));
  // ...and a writer for the next lap waits for it rather than interleaving
  std::atomic_bool done{false};
  std::thread next_lap([&] {
    EXPECT_TRUE(f.ring.write(8, make_payload(0, 8, 16), 2));
    done = true;
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(done);
  f.mapping->header_at(0)->version.store(shm::committed_version(0));
  next_lap.join();
  EXPECT_EQ(f.ring.read(8, f.out.data()).result, ReadResult::OK);
}

// Many producers hammering a tiny ring while readers chase them: every successful read must be an intact message
// for exactly the sequence requested. This is the scenario that used to deliver torn messages.
TEST(RingBuffer, ConcurrentWritersAndReadersNeverTear) {
  RingFixture f(4, 512);
  std::atomic_uint64_t write_seq{0};
  std::atomic_bool stop{false};
  std::atomic_uint64_t ok_reads{0}, bad_reads{0};

  std::vector<std::thread> threads;
  for (uint32_t p = 0; p < 3; ++p) {
    threads.emplace_back([&, p] {
      while (!stop) {
        const uint64_t seq = write_seq.fetch_add(1);
        f.ring.write(seq, make_payload(p, seq, 16 + uint32_t(seq % 480)), int64_t(seq));
      }
    });
  }
  for (int r = 0; r < 2; ++r) {
    threads.emplace_back([&] {
      std::vector<uint8_t> out(512);
      while (!stop) {
        const uint64_t head = write_seq.load();
        for (uint64_t seq = head > 4 ? head - 4 : 0; seq < head; ++seq) {
          const auto outcome = f.ring.read(seq, out.data());
          if (outcome.result != ReadResult::OK) continue;
          const auto info = check_payload({out.data(), outcome.size});
          if (info.valid && info.counter == seq && outcome.publish_timestamp_ns == int64_t(seq)) {
            ++ok_reads;
          } else {
            ++bad_reads;
          }
        }
      }
    });
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  stop = true;
  for (auto& t : threads) t.join();

  EXPECT_EQ(bad_reads.load(), 0u);
  EXPECT_GT(ok_reads.load(), 100u);
}
