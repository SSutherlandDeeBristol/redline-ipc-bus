#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>

#include "errors.hpp"
#include "subscriber.hpp"
#include "redline/config.hpp"

namespace redline {
struct SubscriberMetricsSnapshot {
  uint64_t messages_consumed = 0; // cumulative since this subscriber registered
  uint64_t dropped_messages = 0;  // cumulative since this subscriber registered (overwritten in BEST_EFFORT mode, or skipped after eviction)
  uint64_t current_lag = 0;
  uint64_t max_lag = 0;           // since registration or the last reset_subscriber_metrics()
  int64_t last_latency_ns = 0;

  // Percentiles since registration or the last reset_subscriber_metrics()
  int64_t p50_latency_ns = 0;
  int64_t p90_latency_ns = 0;
  int64_t p99_latency_ns = 0;
  int64_t p999_latency_ns = 0;
};

// A subscriber that the caller drives, for use from your own thread or event loop. It has its own position on the bus,
// starting at the newest message when it is created. A RELIABLE subscription holds publishers back until it has read
// each message, so poll it regularly. Destroying it unsubscribes. It may outlive the Bus that created it.
//
// poll() and wait() must not be called concurrently with each other; the metrics functions may be called from any thread.
class Subscription {
public:
  // Delivers up to max_messages messages that are ready, in order, without blocking. Returns how many were delivered.
  // If the callback throws, the message counts as delivered and the exception propagates to the caller.
  virtual size_t poll(const std::function<void(const Message&)>& callback,
                      size_t max_messages = std::numeric_limits<size_t>::max()) = 0;

  // Blocks until a message is ready to poll or the timeout expires, busy spinning for consumer_busy_spin_duration_us
  // first. Returns true if a message is ready.
  virtual bool wait(std::chrono::microseconds timeout) = 0;

  virtual SubscriberMetricsSnapshot get_metrics() const = 0;
  virtual void reset_metrics() = 0;

  virtual ~Subscription() = default;
};

struct SubscriberOptions {
  // add_subscriber only: run this callback on a thread of its own, with its own position on the bus, so that it can't
  // delay the other callbacks on this Bus or be delayed by them. Costs a thread and a subscriber registry entry per
  // callback. Its metrics are not included in Bus::get_subscriber_metrics(); use subscribe() if you need them.
  bool own_thread = false;

  // Override the Bus' Config for this subscriber. Only for own_thread callbacks and subscribe(): callbacks sharing the
  // Bus' poll thread always use its Config. zero_copy_reads requires RELIABLE.
  std::optional<BackPressureMode> backpressure_mode = std::nullopt;
  std::optional<bool> zero_copy_reads = std::nullopt;
};

class Bus {
public:
  // Waits up to timeout_us (-1 = forever, 0 = not at all) while the slot is still holding a message some RELIABLE
  // subscriber hasn't read; returns BACKPRESSURE_TIMEOUT if it doesn't free up in time. Never waits for BEST_EFFORT
  // subscribers, which are overwritten instead.
  //
  // thread-safe: multiple threads can call publish concurrently
  virtual ErrorCode publish(const std::span<const uint8_t>& data, int64_t timeout_us = -1) = 0;

  // Publishes several messages with a single reservation, so they get consecutive sequence numbers and contend with
  // other producers once rather than once per message. All or nothing: nothing is published if any message is too
  // large or there is no room for all of them before the timeout. A batch may hold at most bus_size messages,
  // otherwise INVALID_ARGUMENT.
  virtual ErrorCode publish_batch(std::span<const std::span<const uint8_t>> messages, int64_t timeout_us = -1) = 0;

  // By default all callbacks on a Bus share one poll thread and one position on the bus: they run one after another
  // for each message, starting from messages published after the first callback was added, so a slow callback delays
  // the others. Set SubscriberOptions::own_thread to avoid that.
  // Callbacks run on a background thread and must return quickly. Anything they capture must outlive the Bus.
  // If a callback throws, the exception is caught and logged, and the callback keeps receiving messages.
  // add_subscriber is thread-safe but may wait on a mutex if another thread is currently adding a subscriber.
  // Returns INVALID_ARGUMENT for an empty callback or invalid options.
  virtual ErrorCode add_subscriber(const std::function<void(const Message&)>& callback, const SubscriberOptions& options = {}) = 0;
  virtual ErrorCode add_subscriber(std::shared_ptr<Subscriber> subscriber, const SubscriberOptions& options = {}) = 0;

  // Creates a Subscription to poll from your own thread. Returns nullptr if the subscriber registry is full. Throws
  // std::invalid_argument for invalid options (including own_thread), and std::runtime_error if the subscription
  // can't be set up.
  virtual std::unique_ptr<Subscription> subscribe(const SubscriberOptions& options = {}) = 0;

  // Metrics for the callbacks sharing this Bus' poll thread. All zeros if there are none.
  virtual SubscriberMetricsSnapshot get_subscriber_metrics() const = 0;

  // Resets the latency percentiles and max_lag, e.g. to report them per interval
  virtual void reset_subscriber_metrics() = 0;

  virtual ~Bus() = default;
};

// Opens the bus with the given name, creating it if it doesn't exist yet.
// Throws std::invalid_argument for an invalid config, and std::runtime_error if the bus cannot be created or opened
// (including when an existing bus was created with an incompatible config or by an incompatible version of redline).
std::unique_ptr<Bus> create_bus(const std::string& name, const Config& config);

// Removes the named bus' shared memory segment. Processes that already have it open keep working, but new calls to
// create_bus will create a fresh bus. Returns NOT_FOUND if no such bus exists.
ErrorCode destroy_bus(const std::string& name);
}
