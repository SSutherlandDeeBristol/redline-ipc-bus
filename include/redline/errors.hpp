#pragma once

namespace redline {
enum class ErrorCode {
  OK = 0,
  BACKPRESSURE_TIMEOUT, // Producer has timed out waiting for subscriber backpressure to ease
  MESSAGE_TOO_LARGE, // Message size exceeds max_message_size configured for the bus
  SUBSCRIBER_CALLBACKS_FULL, // No longer returned: there is no limit on callbacks per Bus
  SUBSCRIBER_REGISTRY_FULL, // All Config::max_subscribers registry entries are in use, across all processes
  NOT_FOUND, // No bus exists with the given name
  INVALID_ARGUMENT, // e.g. an empty callback
  UNKNOWN_ERROR,
  PUBLISHER_REGISTRY_FULL, // All Config::max_publishers registry entries are in use, across all processes
};
}
