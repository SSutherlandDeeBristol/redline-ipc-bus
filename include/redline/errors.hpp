#pragma once

namespace redline {
enum class ErrorCode {
  OK = 0,
  BACKPRESSURE_TIMEOUT, // Producer has timed out waiting for subscriber backpressure to ease
  MESSAGE_TOO_LARGE, // Message size exceeds max_message_size configured for the bus
  SUBSCRIBER_CALLBACKS_FULL, // Too many subscriber callbacks registered in this instance
  SUBSCRIBER_REGISTRY_FULL, // Too many subscribers registered across all processes
  UNKNOWN_ERROR
};
}