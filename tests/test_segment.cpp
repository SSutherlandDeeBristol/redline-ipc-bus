#include <chrono>
#include <mutex>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "redline/bus.hpp"
#include "redline/logging.hpp"
#include "shm/layout.hpp"
#include "shm/segment.hpp"
#include "test_helpers.hpp"

using namespace redline;
using namespace redline::test;

namespace {
std::string seg(const ScopedBusName& name) { return "/redline." + name.str(); }

bool valid(const Config& config) {
  return shm::SegmentMapping::validate(config.layout()) && shm::SegmentMapping::validate(config.options());
}

void expect_invalid(const std::function<void(Config&)>& mutate) {
  Config config = test_config();
  mutate(config);
  EXPECT_FALSE(valid(config));
}

Result<std::unique_ptr<Bus>> try_create(const std::string& name, const Config& config) {
  return create_bus(name, config.layout(), config.options());
}

// Runs f, returning everything it logged at ERROR
template <typename F>
std::string logged_errors(F&& f) {
  std::mutex mutex;
  std::string errors;
  set_log_handler([&](LogLevel level, std::string_view message) {
    if (level == LogLevel::ERROR) {
      std::lock_guard lk(mutex);
      errors.append(message).append("\n");
    }
  });
  f();
  set_log_handler(default_log_handler);
  return errors;
}
}

TEST(Segment, ValidatesConfig) {
  set_log_handler({});
  EXPECT_TRUE(valid(test_config()));
  expect_invalid([](Config& c) { c.bus_size = 0; });
  expect_invalid([](Config& c) { c.bus_size = 1; });
  expect_invalid([](Config& c) { c.bus_size = 3; });
  expect_invalid([](Config& c) { c.bus_size = 1000; });
  expect_invalid([](Config& c) { c.bus_size = 1u << 31; });
  expect_invalid([](Config& c) { c.max_message_size = 0; });
  expect_invalid([](Config& c) { c.max_message_size = (1u << 30) + 1; });
  expect_invalid([](Config& c) { c.latency_histogram_significant_figures = 0; });
  expect_invalid([](Config& c) { c.latency_histogram_significant_figures = 6; });
  expect_invalid([](Config& c) { c.consumer_busy_spin_duration_us = -1; });
  expect_invalid([](Config& c) { c.subscriber_timeout_us = -1; });
  expect_invalid([](Config& c) { c.init_timeout_ms = -1; });
  expect_invalid([](Config& c) { c.backpressure_mode = static_cast<BackPressureMode>(7); });
  expect_invalid([](Config& c) { c.max_subscribers = 0; });
  expect_invalid([](Config& c) { c.max_subscribers = (1u << 16) + 1; });
  expect_invalid([](Config& c) { c.max_publishers = 0; });
  expect_invalid([](Config& c) { c.on_lapped = static_cast<OnLapped>(7); });
  expect_invalid([](Config& c) { c.backpressure_mode = BackPressureMode::BEST_EFFORT; c.zero_copy_reads = true; });
  expect_invalid([](Config& c) { c.poll_thread_cpu = -1; });
  set_log_handler(default_log_handler);
}

TEST(Segment, CreateBusRejectsInvalidConfigAndNames) {
  ScopedBusName name;
  Config config = test_config();
  config.bus_size = 100;
  EXPECT_NE(logged_errors([&] { EXPECT_EQ(try_create(name, config), std::unexpected(Error::INVALID_ARGUMENT)); })
                .find("bus_size must be a power of two"), std::string::npos);
  EXPECT_NE(logged_errors([&] { EXPECT_EQ(try_create("", test_config()), std::unexpected(Error::INVALID_ARGUMENT)); })
                .find("Bus name"), std::string::npos);
  set_log_handler({});
  EXPECT_EQ(try_create("a/b", test_config()), std::unexpected(Error::INVALID_ARGUMENT));
  EXPECT_EQ(open_bus("a/b"), std::unexpected(Error::INVALID_ARGUMENT));
  EXPECT_EQ(destroy_bus("a/b"), std::unexpected(Error::INVALID_ARGUMENT));
  set_log_handler(default_log_handler);
  // Nothing should have been left behind by the failed attempt
  EXPECT_EQ(access(name.shm_path().c_str(), F_OK), -1);
}

TEST(Segment, MessageSizeNeedNotBeMultipleOf64) {
  ScopedBusName name;
  auto bus = make_bus(name, test_config(BackPressureMode::RELIABLE, 64, 100));
  EXPECT_TRUE(bus->publish(std::vector<uint8_t>(100, 1)));
  EXPECT_EQ(bus->publish(std::vector<uint8_t>(101, 1)), std::unexpected(Error::MESSAGE_TOO_LARGE));
}

TEST(Segment, CreatedWithRequestedPermissions) {
  ScopedBusName a, b;
  {
    auto bus = make_bus(a, test_config());
    struct stat st{};
    ASSERT_EQ(stat(a.shm_path().c_str(), &st), 0);
    EXPECT_EQ(st.st_mode & 0777, 0600u);
  }
  {
    Config config = test_config();
    config.shm_permissions = 0640;
    auto bus = make_bus(b, config);
    struct stat st{};
    ASSERT_EQ(stat(b.shm_path().c_str(), &st), 0);
    EXPECT_EQ(st.st_mode & 0777, 0640u);
  }
}

TEST(Segment, SecondMappingSharesMemory) {
  ScopedBusName name;
  auto first = shm::SegmentMapping::open_or_create(seg(name), test_config().layout(), test_config().options()).value();
  auto second = shm::SegmentMapping::open_or_create(seg(name), test_config().layout(), test_config().options()).value();
  ASSERT_NE(first->segment(), second->segment());
  first->payload_at(5)[0] = 0xAB;
  EXPECT_EQ(second->payload_at(5)[0], 0xAB);
  first->segment()->write_seq.store(77);
  EXPECT_EQ(second->segment()->write_seq.load(), 77u);
}

TEST(Segment, SlotLayoutIsCacheAlignedAndNonOverlapping) {
  ScopedBusName name;
  auto mapping = shm::SegmentMapping::open_or_create(seg(name), test_config(BackPressureMode::RELIABLE, 8, 100).layout(), test_config(BackPressureMode::RELIABLE, 8, 100).options()).value();
  for (uint64_t seq = 0; seq < 8; ++seq) {
    const auto header = reinterpret_cast<uintptr_t>(mapping->header_at(seq));
    EXPECT_EQ(header % shm::CACHE_LINE_SIZE, 0u);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(mapping->payload_at(seq)), header + shm::SLOT_HEADER_SIZE);
    if (seq > 0) {
      EXPECT_GE(header, reinterpret_cast<uintptr_t>(mapping->payload_at(seq - 1)) + 100);
    }
  }
  // Sequences a lap apart share a slot
  EXPECT_EQ(mapping->header_at(3), mapping->header_at(3 + 8));
}

TEST(Segment, RegistrySizedByConfigSitsBetweenHeaderAndSlots) {
  for (const uint32_t count : {1u, 256u, 5000u}) {
    ScopedBusName name;
    Config config = test_config(BackPressureMode::RELIABLE, 8, 100);
    config.max_subscribers = count;
    auto mapping = shm::SegmentMapping::open_or_create(seg(name), config.layout(), config.options()).value();
    ASSERT_EQ(mapping->subscriber_count(), count);

    const auto base = reinterpret_cast<uintptr_t>(mapping->segment());
    const auto first = reinterpret_cast<uintptr_t>(&mapping->subscriber(0));
    const auto end = reinterpret_cast<uintptr_t>(&mapping->subscriber(count - 1)) + sizeof(shm::SubscriberEntry);
    EXPECT_EQ(first % shm::CACHE_LINE_SIZE, 0u);
    EXPECT_GE(first, base + sizeof(shm::SHMSegment));
    EXPECT_LE(end, reinterpret_cast<uintptr_t>(mapping->header_at(0)));
    EXPECT_EQ(mapping->subscriber(count - 1).state.load(), uint32_t(shm::SubscriberState::EMPTY));

    // Another process sees the same registry
    auto other = shm::SegmentMapping::open_or_create(seg(name), config.layout(), config.options()).value();
    mapping->subscriber(count - 1).pid = 1234;
    EXPECT_EQ(other->subscriber(count - 1).pid.load(), 1234);
  }
}

TEST(Segment, RejectsMismatchedLayout) {
  ScopedBusName name;
  auto bus = make_bus(name, test_config(BackPressureMode::RELIABLE, 64, 256));

  EXPECT_NE(logged_errors([&] { EXPECT_EQ(try_create(name, test_config(BackPressureMode::RELIABLE, 64, 512)), std::unexpected(Error::INCOMPATIBLE)); }).find("asked for max_message_size=512"), std::string::npos);
  EXPECT_NE(logged_errors([&] { EXPECT_EQ(try_create(name, test_config(BackPressureMode::RELIABLE, 128, 256)), std::unexpected(Error::INCOMPATIBLE)); }).find("asked for max_message_size=256 bus_size=128"), std::string::npos);
  Config more_subscribers = test_config(BackPressureMode::RELIABLE, 64, 256);
  more_subscribers.max_subscribers = 512;
  EXPECT_NE(logged_errors([&] { EXPECT_EQ(try_create(name, more_subscribers), std::unexpected(Error::INCOMPATIBLE)); }).find("max_subscribers=512"), std::string::npos);
  EXPECT_NE(make_bus(name, test_config(BackPressureMode::RELIABLE, 64, 256)), nullptr);
  // The mode is each subscriber's own choice, so it needn't match
  EXPECT_NE(make_bus(name, test_config(BackPressureMode::BEST_EFFORT, 64, 256)), nullptr);
}

TEST(Segment, RejectsIncompatibleVersionOrMagic) {
  ScopedBusName name;
  {
    auto mapping = shm::SegmentMapping::open_or_create(seg(name), test_config().layout(), test_config().options()).value();
    mapping->segment()->version = shm::VERSION + 1;
  }
  EXPECT_NE(logged_errors([&] { EXPECT_EQ(try_create(name, test_config()), std::unexpected(Error::INCOMPATIBLE)); }).find("version"), std::string::npos);
  {
    // Fix the version, break the magic
    const int fd = shm_open(seg(name).c_str(), O_RDWR, 0);
    ASSERT_GE(fd, 0);
    auto* segment = static_cast<shm::SHMSegment*>(mmap(nullptr, sizeof(shm::SHMSegment), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
    segment->version = shm::VERSION;
    segment->magic = 0;
    munmap(segment, sizeof(shm::SHMSegment));
    close(fd);
  }
  EXPECT_NE(logged_errors([&] { EXPECT_EQ(try_create(name, test_config()), std::unexpected(Error::INCOMPATIBLE)); }).find("magic"), std::string::npos);
}

TEST(Segment, RejectsLayoutDrift) {
  ScopedBusName name;
  {
    auto mapping = shm::SegmentMapping::open_or_create(seg(name), test_config().layout(), test_config().options()).value();
    mapping->segment()->subscriber_entry_bytes += 64;
  }
  EXPECT_NE(logged_errors([&] { EXPECT_EQ(try_create(name, test_config()), std::unexpected(Error::INCOMPATIBLE)); }).find("subscriber entry size"), std::string::npos);
}

TEST(Segment, TimesOutOnSegmentThatWasNeverSized) {
  ScopedBusName name;
  // Simulates a creator that crashed straight after shm_open
  const int fd = shm_open(seg(name).c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
  ASSERT_GE(fd, 0);
  close(fd);

  Config config = test_config();
  config.init_timeout_ms = 50;
  const auto start = std::chrono::steady_clock::now();
  const std::string message = logged_errors([&] { EXPECT_EQ(try_create(name, config), std::unexpected(Error::SYSTEM_ERROR)); });
  EXPECT_NE(message.find("Timed out"), std::string::npos) << message;
  EXPECT_NE(message.find("destroy_bus"), std::string::npos) << message;
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(2));
}

TEST(Segment, TimesOutOnSegmentThatWasNeverInitialised) {
  ScopedBusName name;
  // Simulates a creator that crashed after sizing but before marking the segment initialised
  const int fd = shm_open(seg(name).c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(ftruncate(fd, 1 << 20), 0);
  close(fd);

  Config config = test_config();
  config.init_timeout_ms = 50;
  EXPECT_NE(logged_errors([&] { EXPECT_EQ(try_create(name, config), std::unexpected(Error::SYSTEM_ERROR)); }).find("not initialised"), std::string::npos);

  // The documented recovery works
  EXPECT_TRUE(destroy_bus(name));
  EXPECT_NE(make_bus(name, config), nullptr);
}

TEST(Segment, ConcurrentCreateAndOpenAgree) {
  // Many threads racing to create/open the same bus must all end up on one initialised segment
  for (int round = 0; round < 20; ++round) {
    ScopedBusName name;
    std::vector<std::shared_ptr<shm::SegmentMapping>> mappings(8);
    std::vector<std::thread> threads;
    for (size_t i = 0; i < mappings.size(); ++i) {
      threads.emplace_back([&, i] { mappings[i] = shm::SegmentMapping::open_or_create(seg(name), test_config().layout(), test_config().options()).value(); });
    }
    for (auto& t : threads) t.join();
    mappings[0]->segment()->write_seq.store(uint64_t(round) + 1);
    for (const auto& m : mappings) {
      ASSERT_EQ(m->segment()->write_seq.load(), uint64_t(round) + 1);
      ASSERT_EQ(m->segment()->slot_count, 64u);
    }
  }
}

TEST(Segment, LargeBusSizeDoesNotOverflow) {
  // 2^20 slots * 4 KiB used to overflow 32-bit arithmetic into a 64 MB segment and crash on publish
  ScopedBusName name;
  auto mapping = shm::SegmentMapping::open_or_create(seg(name), test_config(BackPressureMode::BEST_EFFORT, 1u << 20, 4096).layout(), test_config(BackPressureMode::BEST_EFFORT, 1u << 20, 4096).options()).value();
  struct stat st{};
  ASSERT_EQ(stat(name.shm_path().c_str(), &st), 0);
  EXPECT_GE(uint64_t(st.st_size), (uint64_t(1) << 20) * 4096);

  // Touch the very last byte of the very last slot (tmpfs only allocates the pages we touch)
  mapping->payload_at((1u << 20) - 1)[4095] = 1;
  EXPECT_EQ(mapping->payload_at((1u << 20) - 1)[4095], 1);
}

TEST(Segment, DestroyBus) {
  ScopedBusName name;
  EXPECT_EQ(destroy_bus(name), std::unexpected(Error::NOT_FOUND));
  {
    auto bus = make_bus(name, test_config());
  }
  EXPECT_EQ(access(name.shm_path().c_str(), F_OK), 0) << "segment should outlive its last Bus";
  EXPECT_TRUE(destroy_bus(name));
  EXPECT_EQ(access(name.shm_path().c_str(), F_OK), -1);
  EXPECT_EQ(destroy_bus(name), std::unexpected(Error::NOT_FOUND));
}

TEST(Segment, DestroyedBusKeepsWorkingForExistingUsers) {
  ScopedBusName name;
  auto bus = make_bus(name, test_config());
  std::atomic_int received{0};
  ASSERT_TRUE(bus->add_subscriber([&](const Message&) { ++received; }));
  ASSERT_TRUE(destroy_bus(name));
  ASSERT_TRUE(bus->publish(as_bytes("still here")));
  EXPECT_TRUE(wait_until([&] { return received == 1; }));
}
