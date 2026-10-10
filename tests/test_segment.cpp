#include <chrono>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "redline/bus.hpp"
#include "shm/layout.hpp"
#include "shm/segment.hpp"
#include "test_helpers.hpp"

using namespace redline;
using namespace redline::test;

namespace {
std::string seg(const ScopedBusName& name) { return "/redline." + name.str(); }

void expect_invalid(const std::function<void(Config&)>& mutate) {
  Config config = test_config();
  mutate(config);
  EXPECT_THROW(shm::SegmentMapping::validate_config(config), std::invalid_argument);
}

template <typename F>
std::string error_message(F&& f) {
  try {
    f();
  } catch (const std::exception& e) {
    return e.what();
  }
  return "";
}
}

TEST(Segment, ValidatesConfig) {
  EXPECT_NO_THROW(shm::SegmentMapping::validate_config(test_config()));
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
}

TEST(Segment, CreateBusRejectsInvalidConfigAndNames) {
  ScopedBusName name;
  Config config = test_config();
  config.bus_size = 100;
  EXPECT_THROW(create_bus(name, config), std::invalid_argument);
  EXPECT_THROW(create_bus("", test_config()), std::invalid_argument);
  EXPECT_THROW(create_bus("a/b", test_config()), std::invalid_argument);
  // Nothing should have been left behind by the failed attempt
  EXPECT_EQ(access(name.shm_path().c_str(), F_OK), -1);
}

TEST(Segment, MessageSizeNeedNotBeMultipleOf64) {
  ScopedBusName name;
  auto bus = create_bus(name, test_config(BackPressureMode::RELIABLE, 64, 100));
  EXPECT_EQ(bus->publish(std::vector<uint8_t>(100, 1)), ErrorCode::OK);
  EXPECT_EQ(bus->publish(std::vector<uint8_t>(101, 1)), ErrorCode::MESSAGE_TOO_LARGE);
}

TEST(Segment, CreatedWithRequestedPermissions) {
  ScopedBusName a, b;
  {
    auto bus = create_bus(a, test_config());
    struct stat st{};
    ASSERT_EQ(stat(a.shm_path().c_str(), &st), 0);
    EXPECT_EQ(st.st_mode & 0777, 0600u);
  }
  {
    Config config = test_config();
    config.shm_permissions = 0640;
    auto bus = create_bus(b, config);
    struct stat st{};
    ASSERT_EQ(stat(b.shm_path().c_str(), &st), 0);
    EXPECT_EQ(st.st_mode & 0777, 0640u);
  }
}

TEST(Segment, SecondMappingSharesMemory) {
  ScopedBusName name;
  auto first = shm::SegmentMapping::open_or_create(seg(name), test_config());
  auto second = shm::SegmentMapping::open_or_create(seg(name), test_config());
  ASSERT_NE(first->segment(), second->segment());
  first->payload_at(5)[0] = 0xAB;
  EXPECT_EQ(second->payload_at(5)[0], 0xAB);
  first->segment()->write_seq.store(77);
  EXPECT_EQ(second->segment()->write_seq.load(), 77u);
}

TEST(Segment, SlotLayoutIsCacheAlignedAndNonOverlapping) {
  ScopedBusName name;
  auto mapping = shm::SegmentMapping::open_or_create(seg(name), test_config(BackPressureMode::RELIABLE, 8, 100));
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
    auto mapping = shm::SegmentMapping::open_or_create(seg(name), config);
    ASSERT_EQ(mapping->subscriber_count(), count);

    const auto base = reinterpret_cast<uintptr_t>(mapping->segment());
    const auto first = reinterpret_cast<uintptr_t>(&mapping->subscriber(0));
    const auto end = reinterpret_cast<uintptr_t>(&mapping->subscriber(count - 1)) + sizeof(shm::SubscriberEntry);
    EXPECT_EQ(first % shm::CACHE_LINE_SIZE, 0u);
    EXPECT_GE(first, base + sizeof(shm::SHMSegment));
    EXPECT_LE(end, reinterpret_cast<uintptr_t>(mapping->header_at(0)));
    EXPECT_EQ(mapping->subscriber(count - 1).state.load(), uint32_t(shm::SubscriberState::EMPTY));

    // Another process sees the same registry
    auto other = shm::SegmentMapping::open_or_create(seg(name), config);
    mapping->subscriber(count - 1).pid = 1234;
    EXPECT_EQ(other->subscriber(count - 1).pid.load(), 1234);
  }
}

TEST(Segment, RejectsMismatchedConfig) {
  ScopedBusName name;
  auto bus = create_bus(name, test_config(BackPressureMode::RELIABLE, 64, 256));

  EXPECT_NE(error_message([&] { create_bus(name, test_config(BackPressureMode::RELIABLE, 64, 512)); }).find("slot size"), std::string::npos);
  EXPECT_NE(error_message([&] { create_bus(name, test_config(BackPressureMode::RELIABLE, 128, 256)); }).find("slot count"), std::string::npos);
  Config more_subscribers = test_config(BackPressureMode::RELIABLE, 64, 256);
  more_subscribers.max_subscribers = 512;
  EXPECT_NE(error_message([&] { create_bus(name, more_subscribers); }).find("max subscribers"), std::string::npos);
  EXPECT_NO_THROW(create_bus(name, test_config(BackPressureMode::RELIABLE, 64, 256)));
  // The mode is each subscriber's own choice, so it needn't match
  EXPECT_NO_THROW(create_bus(name, test_config(BackPressureMode::BEST_EFFORT, 64, 256)));
}

TEST(Segment, RejectsIncompatibleVersionOrMagic) {
  ScopedBusName name;
  {
    auto mapping = shm::SegmentMapping::open_or_create(seg(name), test_config());
    mapping->segment()->version = shm::VERSION + 1;
  }
  EXPECT_NE(error_message([&] { create_bus(name, test_config()); }).find("version"), std::string::npos);
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
  EXPECT_NE(error_message([&] { create_bus(name, test_config()); }).find("magic"), std::string::npos);
}

TEST(Segment, RejectsLayoutDrift) {
  ScopedBusName name;
  {
    auto mapping = shm::SegmentMapping::open_or_create(seg(name), test_config());
    mapping->segment()->subscriber_entry_bytes += 64;
  }
  EXPECT_NE(error_message([&] { create_bus(name, test_config()); }).find("subscriber entry size"), std::string::npos);
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
  const std::string message = error_message([&] { create_bus(name, config); });
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
  EXPECT_NE(error_message([&] { create_bus(name, config); }).find("not initialised"), std::string::npos);

  // The documented recovery works
  EXPECT_EQ(destroy_bus(name), ErrorCode::OK);
  EXPECT_NO_THROW(create_bus(name, config));
}

TEST(Segment, ConcurrentCreateAndOpenAgree) {
  // Many threads racing to create/open the same bus must all end up on one initialised segment
  for (int round = 0; round < 20; ++round) {
    ScopedBusName name;
    std::vector<std::shared_ptr<shm::SegmentMapping>> mappings(8);
    std::vector<std::thread> threads;
    for (size_t i = 0; i < mappings.size(); ++i) {
      threads.emplace_back([&, i] { mappings[i] = shm::SegmentMapping::open_or_create(seg(name), test_config()); });
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
  auto mapping = shm::SegmentMapping::open_or_create(seg(name), test_config(BackPressureMode::BEST_EFFORT, 1u << 20, 4096));
  struct stat st{};
  ASSERT_EQ(stat(name.shm_path().c_str(), &st), 0);
  EXPECT_GE(uint64_t(st.st_size), (uint64_t(1) << 20) * 4096);

  // Touch the very last byte of the very last slot (tmpfs only allocates the pages we touch)
  mapping->payload_at((1u << 20) - 1)[4095] = 1;
  EXPECT_EQ(mapping->payload_at((1u << 20) - 1)[4095], 1);
}

TEST(Segment, DestroyBus) {
  ScopedBusName name;
  EXPECT_EQ(destroy_bus(name), ErrorCode::NOT_FOUND);
  {
    auto bus = create_bus(name, test_config());
  }
  EXPECT_EQ(access(name.shm_path().c_str(), F_OK), 0) << "segment should outlive its last Bus";
  EXPECT_EQ(destroy_bus(name), ErrorCode::OK);
  EXPECT_EQ(access(name.shm_path().c_str(), F_OK), -1);
  EXPECT_EQ(destroy_bus(name), ErrorCode::NOT_FOUND);
}

TEST(Segment, DestroyedBusKeepsWorkingForExistingUsers) {
  ScopedBusName name;
  auto bus = create_bus(name, test_config());
  std::atomic_int received{0};
  ASSERT_EQ(bus->add_subscriber([&](const Message&) { ++received; }), ErrorCode::OK);
  ASSERT_EQ(destroy_bus(name), ErrorCode::OK);
  ASSERT_EQ(bus->publish(as_bytes("still here")), ErrorCode::OK);
  EXPECT_TRUE(wait_until([&] { return received == 1; }));
}
