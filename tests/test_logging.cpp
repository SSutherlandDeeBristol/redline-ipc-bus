// The process-wide log hook that replaced glog
#include <mutex>

#include <gtest/gtest.h>

#include "redline/bus.hpp"
#include "redline/logging.hpp"
#include "test_helpers.hpp"

using namespace redline;
using namespace redline::test;

namespace {
struct CapturedLogs {
  std::mutex mutex;
  std::vector<std::pair<LogLevel, std::string>> lines;

  bool contains(LogLevel level, const std::string& text) {
    std::lock_guard lk(mutex);
    for (const auto& [l, line] : lines) {
      if (l == level && line.find(text) != std::string::npos) return true;
    }
    return false;
  }
};

// Installs a capturing handler for the duration of a test, then restores the default
class LogCapture {
public:
  LogCapture() {
    set_log_handler([this](LogLevel level, std::string_view message) {
      std::lock_guard lk(logs.mutex);
      logs.lines.emplace_back(level, std::string(message));
    });
  }
  ~LogCapture() { set_log_handler(default_log_handler); }
  CapturedLogs logs;
};
}

TEST(Logging, HandlerReceivesLibraryMessages) {
  LogCapture capture;
  ScopedBusName name;
  std::atomic_int delivered{0};
  auto bus = make_bus(name, test_config());
  EXPECT_TRUE(capture.logs.contains(LogLevel::INFO, "Created new shared memory segment"));

  ASSERT_TRUE(bus->add_subscriber([&](const Message&) {
    ++delivered;
    throw std::runtime_error("callback failure");
  }));
  ASSERT_TRUE(bus->publish(as_bytes("x")));
  ASSERT_TRUE(wait_until([&] { return delivered == 1; }));
  EXPECT_TRUE(wait_until([&] { return capture.logs.contains(LogLevel::ERROR, "callback failure"); }));
}

TEST(Logging, EmptyHandlerDiscardsAndThrowingHandlerIsContained) {
  ScopedBusName name;
  set_log_handler({});
  EXPECT_NE(make_bus(name, test_config()), nullptr);

  set_log_handler([](LogLevel, std::string_view) { throw std::runtime_error("broken handler"); });
  EXPECT_NE(make_bus(name, test_config()), nullptr);
  set_log_handler(default_log_handler);
}
