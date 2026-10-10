#pragma once

#include <sstream>

#include "redline/logging.hpp"

namespace redline::detail {
void emit_log(LogLevel level, std::string_view message) noexcept;

// Collects a message with << and hands it to the log handler when destroyed: REDLINE_LOG(WARNING) << "x=" << x;
class LogMessage {
public:
  explicit LogMessage(LogLevel level) : level_(level) {}
  ~LogMessage() { emit_log(level_, stream_.view()); }

  LogMessage(const LogMessage&) = delete;
  LogMessage& operator=(const LogMessage&) = delete;

  template <typename T>
  LogMessage& operator<<(const T& value) {
    stream_ << value;
    return *this;
  }

private:
  LogLevel level_;
  std::ostringstream stream_;
};
}

#define REDLINE_LOG(level) ::redline::detail::LogMessage(::redline::LogLevel::level)
