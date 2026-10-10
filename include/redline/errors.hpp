#pragma once

#include <expected>
#include <string_view>

namespace redline {
enum class Error {
  BACKPRESSURE_TIMEOUT,     // a RELIABLE subscriber didn't free the slot within the timeout
  MESSAGE_TOO_LARGE,        // larger than the bus' max_message_size
  SUBSCRIBER_REGISTRY_FULL, // all max_subscribers registry entries are in use, across all processes
  PUBLISHER_REGISTRY_FULL,  // all max_publishers registry entries are in use, across all processes
  INVALID_ARGUMENT,         // an invalid name, layout, option or batch, an empty callback, or a claim that isn't open
  NOT_FOUND,                // no bus exists with the given name
  INCOMPATIBLE,             // the bus exists but has a different layout, or was made by an incompatible version
  SYSTEM_ERROR,             // a system call failed or timed out
};

// Failures with more to say than their Error, such as which option is invalid or which system call failed, are also
// logged (see logging.hpp)
template <typename T = void>
using Result = std::expected<T, Error>;

std::string_view to_string(Error error) noexcept;
}
