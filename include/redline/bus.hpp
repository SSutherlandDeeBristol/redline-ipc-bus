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
#include <string_view>

#include "redline/config.hpp"
#include "redline/errors.hpp"
#include "redline/message.hpp"

namespace redline {
struct SubscriberMetricsSnapshot {
  uint64_t messages_consumed = 0; // cumulative since this subscriber registered
  uint64_t dropped_messages = 0;  // cumulative since this subscriber registered (lapped, evicted, or lost with a crashed publisher)
  uint64_t current_lag = 0;
  uint64_t max_lag = 0;           // since registration or the last reset

  // Publish-to-delivery latency: of the latest message, and percentiles since registration or the last reset. All 0
  // when latency_metrics is off.
  int64_t last_latency_ns = 0;
  int64_t p50_latency_ns = 0;
  int64_t p90_latency_ns = 0;
  int64_t p99_latency_ns = 0;
  int64_t p999_latency_ns = 0;
};

using Callback = std::function<void(const Message&)>;

// A subscriber that the caller drives, for use from your own thread or event loop. It has its own position on the bus,
// starting at the newest message when it is created. A RELIABLE subscription holds publishers back until it has read
// each message, so poll it regularly. Destroying it unsubscribes. It may outlive the Bus that created it.
//
// poll() and wait() must not be called concurrently with each other; the metrics functions may be called from any thread.
class Subscription {
public:
  // Delivers up to max_messages messages that are ready, in order, without blocking. Returns how many were delivered.
  // If the callback throws, the message counts as delivered and the exception propagates to the caller.
  virtual size_t poll(const Callback& callback, size_t max_messages = std::numeric_limits<size_t>::max()) = 0;

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

  // Override the Bus' BusOptions for this subscriber. Only for own_thread callbacks and subscribe(): callbacks sharing
  // the Bus' poll thread always use its BusOptions. zero_copy_reads requires RELIABLE.
  std::optional<BackPressureMode> backpressure_mode = std::nullopt;
  std::optional<OnLapped> on_lapped = std::nullopt;
  std::optional<bool> zero_copy_reads = std::nullopt;
  std::optional<bool> latency_metrics = std::nullopt;

  // own_thread only: pin the callback's thread to this CPU
  std::optional<int> cpu = std::nullopt;
};

class Producer;

namespace detail {
class ClaimOwner {
public:
  virtual void commit_claim(uint64_t sequence, uint32_t size, void* context) noexcept = 0;
  virtual void abort_claim(uint64_t sequence, void* context) noexcept = 0;

protected:
  ~ClaimOwner() = default;
};
}

// A slot reserved by Bus::claim(), for building a message directly in shared memory instead of copying it in.
// Write the message into data(), then commit() the bytes used. A claim that is destroyed or reassigned without being
// committed is aborted: its sequence number is skipped and subscribers never see it.
//
// Subscribers receive messages in order, so none of them can get past this sequence until it is committed or aborted:
// keep claims short. In particular, don't publish more than bus_size messages from the thread holding a claim while
// RELIABLE subscribers are on the bus, since they would be waiting on the claim while you wait on them.
// A claim may be moved to and committed from another thread, but must be finished before its Bus is destroyed.
class Claim {
public:
  Claim() = default;
  Claim(Claim&& other) noexcept;
  Claim& operator=(Claim&& other) noexcept;
  ~Claim();

  Claim(const Claim&) = delete;
  Claim& operator=(const Claim&) = delete;

  // True while the claim is open (claimed and not yet committed or aborted)
  explicit operator bool() const noexcept { return owner_ != nullptr; }

  // Where to write the message: max_message_size bytes. Empty if the claim isn't open.
  std::span<uint8_t> data() const noexcept { return data_; }
  uint64_t sequence() const noexcept { return sequence_; }

  // Publishes the first `size` bytes of data(). Fails with INVALID_ARGUMENT if the claim isn't open, and
  // MESSAGE_TOO_LARGE (leaving the claim open) if size is larger than data().
  Result<> commit(size_t size) noexcept;

  // Gives the sequence up. Does nothing if the claim isn't open.
  void abort() noexcept;

private:
  friend class Producer;
  Claim(detail::ClaimOwner* owner, uint64_t sequence, std::span<uint8_t> data, void* context) noexcept
      : owner_(owner), sequence_(sequence), data_(data), context_(context) {}

  detail::ClaimOwner* owner_ = nullptr;
  uint64_t sequence_ = 0;
  std::span<uint8_t> data_;
  void* context_ = nullptr;
};

class Bus {
public:
  // Waits up to timeout_us (-1 = forever, 0 = not at all) while the slot is still holding a message some RELIABLE
  // subscriber hasn't read; fails with BACKPRESSURE_TIMEOUT if it doesn't free up in time. Never waits for BEST_EFFORT
  // subscribers, which are overwritten instead.
  //
  // thread-safe: multiple threads can call publish concurrently
  virtual Result<> publish(std::span<const uint8_t> data, int64_t timeout_us = -1) = 0;

  // Publishes the bytes of a string, e.g. bus->publish("hello")
  Result<> publish(std::string_view data, int64_t timeout_us = -1) {
    return publish(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(data.data()), data.size()), timeout_us);
  }

  // Publishes several messages with a single reservation, so they get consecutive sequence numbers and contend with
  // other producers once rather than once per message. All or nothing: nothing is published if any message is too
  // large or there is no room for all of them before the timeout. A batch may hold at most bus_size messages,
  // otherwise INVALID_ARGUMENT.
  virtual Result<> publish_batch(std::span<const std::span<const uint8_t>> messages, int64_t timeout_us = -1) = 0;

  // Reserves the next slot for writing a message in place, waiting for RELIABLE subscribers like publish(). The
  // message is timestamped when it is committed. Thread-safe.
  //
  //   if (auto claim = bus->claim()) {
  //     size_t size = serialize_into(claim->data());
  //     claim->commit(size);
  //   }
  [[nodiscard]] virtual Result<Claim> claim(int64_t timeout_us = -1) = 0;

  // By default all callbacks on a Bus share one poll thread and one position on the bus: they run one after another
  // for each message, starting from messages published after the first callback was added, so a slow callback delays
  // the others. Set SubscriberOptions::own_thread to avoid that.
  // Callbacks run on a background thread and must return quickly. Anything they capture must outlive the Bus.
  // If a callback throws, the exception is caught and logged, and the callback keeps receiving messages.
  // add_subscriber is thread-safe but may wait on a mutex if another thread is currently adding a subscriber.
  // Fails with INVALID_ARGUMENT for an empty callback or invalid options.
  virtual Result<> add_subscriber(Callback callback, const SubscriberOptions& options = {}) = 0;

  // Creates a Subscription to poll from your own thread. Fails with INVALID_ARGUMENT for invalid options (including
  // own_thread and cpu, which are for add_subscriber).
  [[nodiscard]] virtual Result<std::unique_ptr<Subscription>> subscribe(const SubscriberOptions& options = {}) = 0;

  // Metrics for the callbacks sharing this Bus' poll thread. All zeros if there are none.
  virtual SubscriberMetricsSnapshot get_subscriber_metrics() const = 0;

  // Resets the latency percentiles and max_lag, e.g. to report them per interval
  virtual void reset_subscriber_metrics() = 0;

  virtual const BusLayout& layout() const noexcept = 0;

  virtual ~Bus() = default;
};

// Opens the bus with the given name, creating it with `layout` if it doesn't exist yet. Fails with INVALID_ARGUMENT
// for an invalid name, layout or options, INCOMPATIBLE if the bus exists with a different layout or was created by an
// incompatible version of redline, and SYSTEM_ERROR if the segment can't be created or mapped.
[[nodiscard]] Result<std::unique_ptr<Bus>> create_bus(const std::string& name, const BusLayout& layout,
                                                      const BusOptions& options = {});

// Opens an existing bus, whatever its layout. Fails with NOT_FOUND if there is none, otherwise like create_bus.
[[nodiscard]] Result<std::unique_ptr<Bus>> open_bus(const std::string& name, const BusOptions& options = {});

// Removes the named bus' shared memory segment. Processes that already have it open keep working, but new calls to
// create_bus will create a fresh bus. Fails with NOT_FOUND if no such bus exists.
Result<> destroy_bus(const std::string& name);
}
