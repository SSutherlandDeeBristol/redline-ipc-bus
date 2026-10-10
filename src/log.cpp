#include "log.hpp"

#include <cstdio>
#include <memory>
#include <mutex>

namespace redline {
void default_log_handler(const LogLevel level, const std::string_view message) {
  if (level == LogLevel::INFO) {
    return;
  }
  std::fprintf(stderr, "redline %s: %.*s\n", level == LogLevel::WARNING ? "WARNING" : "ERROR",
               static_cast<int>(message.size()), message.data());
}

namespace {
std::mutex handler_mutex;
// Held by shared_ptr so a handler being replaced stays alive until calls already using it have finished
std::shared_ptr<const LogHandler> handler = std::make_shared<const LogHandler>(default_log_handler);
}

void set_log_handler(LogHandler new_handler) {
  auto replacement = std::make_shared<const LogHandler>(std::move(new_handler));
  std::lock_guard lk(handler_mutex);
  handler = std::move(replacement);
}

namespace detail {
void emit_log(const LogLevel level, const std::string_view message) noexcept {
  std::shared_ptr<const LogHandler> current;
  {
    std::lock_guard lk(handler_mutex);
    current = handler;
  }
  if (*current) {
    try {
      (*current)(level, message);
    } catch (...) {
      // A failing log handler must not take down a poll thread
    }
  }
}
}
}
