#include "redline/bus.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <limits>
#include <list>
#include <mutex>
#include <stdexcept>
#include <sys/mman.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "log.hpp"

#include "redline/config.hpp"
#include "redline/utils.hpp"
#include "cpu.hpp"
#include "histogram.hpp"
#include "shm/futex.hpp"
#include "shm/layout.hpp"
#include "shm/ring_buffer.hpp"
#include "shm/segment.hpp"
#include "shm/subscriber_registry.hpp"

namespace redline {
namespace {
constexpr int64_t SLEEP_TIMEOUT_NS = 50'000'000; // upper bound on how long a sleeping poll thread takes to notice shutdown
constexpr int64_t HEARTBEAT_INTERVAL_NS = 1'000'000;
constexpr int64_t REAP_INTERVAL_NS = 100'000'000;
constexpr uint32_t SPINS_BEFORE_YIELD = 1024;
constexpr uint32_t SPINS_PER_SCAN = 16; // how often a producer waiting for space re-reads subscribers' positions
constexpr size_t POLL_BATCH = 256; // messages a poll thread delivers between checks for shutdown

std::string segment_name(const std::string& name) {
  if (name.empty() || name.find('/') != std::string::npos || name.size() > 200) {
    throw std::invalid_argument("bus name must be 1-200 characters and must not contain '/'");
  }
  return "/redline." + name;
}

void wake_sleepers(shm::SHMSegment* segment) noexcept {
  segment->commit_futex.fetch_add(1, std::memory_order_seq_cst);
  shm::futex_wake(&segment->commit_futex, std::numeric_limits<int>::max());
}

void consume_safely(Subscriber& subscriber, const Message& message, const size_t index) noexcept {
  try {
    subscriber.consume(message);
  } catch (const std::exception& e) {
    REDLINE_LOG(ERROR) << "Subscriber callback " << index << " threw exception: " << e.what();
  } catch (...) {
    REDLINE_LOG(ERROR) << "Subscriber callback " << index << " threw a non-std::exception";
  }
}
}

// One registry entry and read position on the bus. Backs Bus::subscribe(), the callbacks sharing a Bus' poll thread,
// and each own_thread callback.
class SubscriptionImpl final : public Subscription {
public:
  SubscriptionImpl(std::shared_ptr<shm::SegmentMapping> mapping, const Config& config, std::unique_ptr<shm::SubscriberLease> lease)
      : mapping_(std::move(mapping)),
        segment_(mapping_->segment()),
        ring_buffer_(mapping_),
        lease_(std::move(lease)),
        entry_(lease_->entry()),
        name_(mapping_->name()),
        slot_count_(segment_->slot_count),
        zero_copy_(config.zero_copy_reads),
        spin_duration_ns_(config.consumer_busy_spin_duration_us * 1000),
        histogram_(config.latency_histogram_significant_figures),
        read_seq_(entry_.read_seq.load(std::memory_order_acquire)),
        last_heartbeat_ns_(now_ns()) {
    if (!zero_copy_) {
      buffer_.resize(segment_->slot_size_bytes);
    }
  }

  size_t poll(const std::function<void(const Message&)>& callback, const size_t max_messages) override {
    return poll_with(callback, max_messages);
  }

  bool wait(const std::chrono::microseconds timeout) override {
    return wait_until(now_benchmark_ns() + timeout.count() * 1000, nullptr);
  }

  SubscriberMetricsSnapshot get_metrics() const override {
    SubscriberMetricsSnapshot snap;
    const uint64_t read_seq = entry_.read_seq.load(std::memory_order_relaxed);
    const uint64_t write_seq = segment_->write_seq.load(std::memory_order_relaxed);

    snap.messages_consumed = entry_.messages_consumed.load(std::memory_order_relaxed);
    snap.dropped_messages = entry_.dropped_count.load(std::memory_order_relaxed);
    snap.current_lag = write_seq > read_seq ? write_seq - read_seq : 0;
    snap.max_lag = entry_.max_lag.load(std::memory_order_relaxed);
    snap.last_latency_ns = entry_.last_latency_ns.load(std::memory_order_relaxed);

    snap.p50_latency_ns = histogram_.percentile(50.0);
    snap.p90_latency_ns = histogram_.percentile(90.0);
    snap.p99_latency_ns = histogram_.percentile(99.0);
    snap.p999_latency_ns = histogram_.percentile(99.9);
    return snap;
  }

  void reset_metrics() override {
    histogram_.reset();
    entry_.max_lag.store(0, std::memory_order_relaxed);
  }

  // Delivers up to max_messages ready messages to `deliver`, which is called with each Message
  template <typename Deliver>
  size_t poll_with(Deliver&& deliver, const size_t max_messages) {
    size_t delivered = 0;
    // Producers write write_seq on every publish, so reading it for every message would keep stealing its cache line
    // from them. A snapshot is enough to measure lag, refreshed only once we've read past it.
    uint64_t write_seq_snapshot = 0;
    while (delivered < max_messages) {
      if (entry_.state.load(std::memory_order_relaxed) != uint32_t(shm::SubscriberState::OK)) {
        if (!rejoin()) break;
        continue;
      }

      const uint64_t seq = read_seq_;
      const uint8_t* data = buffer_.data();
      const auto outcome = zero_copy_ ? ring_buffer_.peek(seq, data) : ring_buffer_.read(seq, buffer_.data());

      if (outcome.result == shm::RingBuffer::ReadResult::NOT_READY) {
        break;
      }
      if (outcome.result == shm::RingBuffer::ReadResult::OVERWRITTEN) {
        skip_overwritten();
        continue;
      }

      read_seq_ = seq + 1;
      ++delivered;
      if (seq >= write_seq_snapshot) {
        write_seq_snapshot = segment_->write_seq.load(std::memory_order_relaxed);
      }
      record_metrics(write_seq_snapshot > seq ? write_seq_snapshot - seq : 1, outcome.publish_timestamp_ns);
      const Message message{
        .sequence = seq,
        .publish_timestamp_ns = outcome.publish_timestamp_ns,
        .data = std::span<const uint8_t>(data, outcome.size)
      };

      if (zero_copy_) {
        // The callbacks read the slot in place, so only release it to publishers once they are done, even if they throw
        struct Release {
          shm::SubscriberEntry& entry;
          uint64_t next;
          ~Release() { entry.read_seq.store(next, std::memory_order_release); }
        } release{entry_, seq + 1};
        deliver(message);
      } else {
        // The message has been copied out, so release the slot before running callbacks
        entry_.read_seq.store(seq + 1, std::memory_order_release);
        deliver(message);
      }
    }
    return delivered;
  }

  // Waits until a message is ready to poll, the deadline (CLOCK_MONOTONIC_RAW ns) passes, or *stop becomes true.
  // Spins for consumer_busy_spin_duration_us before sleeping on the bus' futex.
  bool wait_until(const int64_t deadline_ns, const std::atomic_bool* stop) {
    const int64_t start_ns = now_benchmark_ns();
    while (true) {
      if (ready()) {
        return true;
      }
      if (stop && stop->load(std::memory_order_relaxed)) {
        return false;
      }
      const int64_t now = now_benchmark_ns();
      if (now >= deadline_ns) {
        return false;
      }
      if (now - start_ns < spin_duration_ns_) {
        cpu_relax();
        continue;
      }
      heartbeat(now_ns());
      sleep_until_ready(std::min(deadline_ns - now, SLEEP_TIMEOUT_NS), stop);
    }
  }

private:
  std::shared_ptr<shm::SegmentMapping> mapping_;
  shm::SHMSegment* segment_;
  shm::RingBuffer ring_buffer_;
  std::unique_ptr<shm::SubscriberLease> lease_;
  shm::SubscriberEntry& entry_;
  std::string name_;
  uint64_t slot_count_;
  bool zero_copy_;
  int64_t spin_duration_ns_;

  Histogram histogram_;

  // Only touched by the polling thread
  uint64_t read_seq_;
  int64_t last_heartbeat_ns_;
  std::vector<uint8_t> buffer_; // copy mode: messages are copied here before being handed to callbacks

  bool ready() const noexcept {
    return entry_.state.load(std::memory_order_relaxed) != uint32_t(shm::SubscriberState::OK) ||
           ring_buffer_.is_ready(read_seq_);
  }

  void heartbeat(const int64_t now) noexcept {
    entry_.heartbeat_ns.store(now, std::memory_order_relaxed);
    last_heartbeat_ns_ = now;
  }

  void record_metrics(const uint64_t lag, const int64_t publish_timestamp_ns) noexcept {
    const int64_t now = now_ns();
    const int64_t latency_ns = now - publish_timestamp_ns;

    // Single writer, so plain stores instead of locked read-modify-writes
    entry_.messages_consumed.store(entry_.messages_consumed.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
    if (lag > entry_.max_lag.load(std::memory_order_relaxed)) {
      entry_.max_lag.store(lag, std::memory_order_relaxed);
    }
    entry_.last_latency_ns.store(latency_ns, std::memory_order_relaxed);
    histogram_.record(latency_ns);

    if (now - last_heartbeat_ns_ > HEARTBEAT_INTERVAL_NS) {
      heartbeat(now);
    }
  }

  void add_dropped(const uint64_t count) noexcept {
    entry_.dropped_count.store(entry_.dropped_count.load(std::memory_order_relaxed) + count, std::memory_order_relaxed);
  }

  // Lapped by producers: skip to the oldest message that can still be in the ring
  void skip_overwritten() noexcept {
    const uint64_t write_seq = segment_->write_seq.load(std::memory_order_acquire);
    const uint64_t oldest = write_seq >= slot_count_ ? write_seq - slot_count_ + 1 : 0;
    const uint64_t new_read_seq = std::max(read_seq_ + 1, oldest);
    add_dropped(new_read_seq - read_seq_);
    entry_.read_seq.store(new_read_seq, std::memory_order_release);
    read_seq_ = new_read_seq;
  }

  // Called when our entry is no longer OK: we were evicted for blocking publishers (DEAD). Rejoins at the head and
  // counts everything skipped as dropped. EMPTY can't normally happen, since nobody reclaims an entry whose lease we
  // hold, but if it does the lease means nobody else can have claimed it, so take it back. Returns false if the entry
  // is in some other state, i.e. there is nothing to poll.
  bool rejoin() {
    uint32_t state = entry_.state.load(std::memory_order_acquire);
    if (state == uint32_t(shm::SubscriberState::EMPTY)) {
      if (!entry_.state.compare_exchange_strong(state, uint32_t(shm::SubscriberState::INITIALISING))) {
        return false;
      }
      entry_.pid.store(getpid(), std::memory_order_relaxed);
    } else if (state != uint32_t(shm::SubscriberState::DEAD)) {
      return state == uint32_t(shm::SubscriberState::OK);
    }

    REDLINE_LOG(WARNING) << "Subscriber on " << name_ << " was evicted for blocking publishers, skipping ahead";
    heartbeat(now_ns());
    entry_.state.store(uint32_t(shm::SubscriberState::OK), std::memory_order_seq_cst);
    const uint64_t write_seq = segment_->write_seq.load(std::memory_order_seq_cst);
    const uint64_t new_read_seq = std::max(write_seq, read_seq_);
    entry_.read_seq.store(new_read_seq, std::memory_order_seq_cst);
    add_dropped(new_read_seq - read_seq_);
    read_seq_ = new_read_seq;
    return true;
  }

  void sleep_until_ready(const int64_t timeout_ns, const std::atomic_bool* stop) {
    segment_->waiters.fetch_add(1, std::memory_order_seq_cst);
    // Pairs with the fence in publish: either the publisher sees our waiter count, or we see its commit
    std::atomic_thread_fence(std::memory_order_seq_cst);
    const uint32_t futex_value = segment_->commit_futex.load(std::memory_order_seq_cst);
    if (!ready() && !(stop && stop->load())) {
      shm::futex_wait(&segment_->commit_futex, futex_value, std::chrono::microseconds(timeout_ns / 1000));
    }
    segment_->waiters.fetch_sub(1, std::memory_order_seq_cst);
  }
};

class BusImpl : public Bus {
public:
  BusImpl(const std::string& name, const Config& config) : name_(name), config_(config) {
    segment_mapping_ = shm::SegmentMapping::open_or_create(segment_name(name), config);
    segment_ = segment_mapping_->segment();
    slot_count_ = segment_->slot_count;
    ring_buffer_ = std::make_unique<shm::RingBuffer>(segment_mapping_);
    subscriber_registry_ = std::make_unique<shm::SubscriberRegistry>(segment_mapping_);
  }

  ErrorCode publish(const std::span<const uint8_t>& data, const int64_t timeout_us) override {
    return publish_batch(std::span<const std::span<const uint8_t>>(&data, 1), timeout_us);
  }

  ErrorCode publish_batch(const std::span<const std::span<const uint8_t>> messages, const int64_t timeout_us) override {
    for (const auto& data : messages) {
      if (data.size() > config_.max_message_size) {
        return ErrorCode::MESSAGE_TOO_LARGE;
      }
    }
    const uint64_t count = messages.size();
    if (count == 0) {
      return ErrorCode::OK;
    }

    const int64_t timestamp_ns = now_ns();

    uint64_t first_seq;
    if (config_.backpressure_mode == BackPressureMode::RELIABLE) {
      if (count > slot_count_) {
        return ErrorCode::INVALID_ARGUMENT;
      }
      if (!reserve_reliable(count, timeout_us, first_seq)) {
        return ErrorCode::BACKPRESSURE_TIMEOUT;
      }
    } else {
      first_seq = segment_->write_seq.fetch_add(count, std::memory_order_seq_cst);
    }

    // A false return means a later lap already took the slot, which subscribers will account for as a drop
    for (uint64_t i = 0; i < count; ++i) {
      ring_buffer_->write(first_seq + i, messages[i], timestamp_ns);
    }

    // Only pay for a syscall when a subscriber is actually asleep. The fence pairs with the one in sleep_until_ready:
    // either we see its waiter count, or it sees our commit.
    std::atomic_thread_fence(std::memory_order_seq_cst);
    if (segment_->waiters.load(std::memory_order_relaxed) != 0) {
      wake_sleepers(segment_);
    }
    return ErrorCode::OK;
  }

  ErrorCode add_subscriber(const std::function<void(const Message&)>& callback, const SubscriberOptions& options) override {
    if (!callback) {
      return ErrorCode::INVALID_ARGUMENT;
    }
    return add_subscriber(create_subscriber(callback), options);
  }

  ErrorCode add_subscriber(std::shared_ptr<Subscriber> subscriber, const SubscriberOptions& options) override {
    if (!subscriber) {
      return ErrorCode::INVALID_ARGUMENT;
    }

    std::lock_guard lk(subscriber_creation_mutex_);

    if (options.own_thread) {
      std::unique_ptr<SubscriptionImpl> subscription;
      if (const auto ec = try_subscribe(subscription); ec != ErrorCode::OK) {
        return ec;
      }
      auto& own = own_thread_subscribers_.emplace_back();
      own.subscription = std::move(subscription);
      own.thread = std::thread([this, sub = own.subscription.get(), subscriber = std::move(subscriber)]() {
        run(*sub, [&](const Message& message) { consume_safely(*subscriber, message, 0); });
      });
      return ErrorCode::OK;
    }

    // The first callback registers this Bus as a subscriber; nothing is recorded unless that succeeds
    const bool first = !shared_subscription_;
    if (first) {
      if (const auto ec = try_subscribe(shared_subscription_); ec != ErrorCode::OK) {
        return ec;
      }
    }

    // Copy on write, so the poll thread can keep using the list it has while we publish a new one
    auto callbacks = std::make_shared<Callbacks>(callbacks_ ? *callbacks_ : Callbacks{});
    callbacks->push_back(std::move(subscriber));
    callbacks_ = std::move(callbacks);
    callbacks_version_.fetch_add(1, std::memory_order_release);

    if (first) {
      shared_subscription_ptr_.store(shared_subscription_.get(), std::memory_order_release);
      poll_thread_ = std::thread([this]() { run_shared(); });
    }

    return ErrorCode::OK;
  }

  std::unique_ptr<Subscription> subscribe() override {
    auto lease = subscriber_registry_->register_subscriber(!config_.zero_copy_reads);
    if (!lease) {
      return nullptr;
    }
    return std::make_unique<SubscriptionImpl>(segment_mapping_, config_, std::move(lease));
  }

  SubscriberMetricsSnapshot get_subscriber_metrics() const override {
    const auto* subscription = shared_subscription_ptr_.load(std::memory_order_acquire);
    return subscription ? subscription->get_metrics() : SubscriberMetricsSnapshot{};
  }

  void reset_subscriber_metrics() override {
    if (auto* subscription = shared_subscription_ptr_.load(std::memory_order_acquire)) {
      subscription->reset_metrics();
    }
  }

  ~BusImpl() override {
    kill_.store(true);
    // Wake our poll threads if they are asleep (other sleeping subscribers just go back to sleep). SLEEP_TIMEOUT_NS
    // covers a thread that was just about to go to sleep and misses this.
    wake_sleepers(segment_);
    if (poll_thread_.joinable()) {
      poll_thread_.join();
    }
    for (auto& own : own_thread_subscribers_) {
      own.thread.join();
    }
    // The subscriptions deregister as they are destroyed
  }

private:
  struct OwnThreadSubscriber {
    std::unique_ptr<SubscriptionImpl> subscription;
    std::thread thread;
  };

  std::string name_;
  Config config_;

  std::shared_ptr<shm::SegmentMapping> segment_mapping_;
  shm::SHMSegment* segment_;
  uint64_t slot_count_;

  std::unique_ptr<shm::RingBuffer> ring_buffer_;
  std::unique_ptr<shm::SubscriberRegistry> subscriber_registry_;

  // Shared by the callbacks on poll_thread_. Created with the first callback and kept until the Bus is destroyed.
  std::unique_ptr<SubscriptionImpl> shared_subscription_;
  std::atomic<SubscriptionImpl*> shared_subscription_ptr_ = nullptr; // for reading metrics without the mutex

  using Callbacks = std::vector<std::shared_ptr<Subscriber>>;
  std::mutex subscriber_creation_mutex_;
  // Callbacks run by poll_thread_, replaced wholesale whenever one is added. callbacks_version_ changes with it, so the
  // poll thread only takes the mutex to pick up a new list, not for every message.
  std::shared_ptr<const Callbacks> callbacks_; // guarded by subscriber_creation_mutex_
  std::atomic_uint64_t callbacks_version_ = 0;

  std::list<OwnThreadSubscriber> own_thread_subscribers_; // guarded by subscriber_creation_mutex_

  // Lower bound on every live subscriber's read_seq, so publishers only scan the registry when the ring looks full.
  // Release/acquire so that a subscriber releasing a slot happens-before a publisher reusing it.
  std::atomic_uint64_t cached_min_read_seq_ = 0;
  std::atomic_bool scanning_ = false; // a producer in this process is refreshing cached_min_read_seq_
  std::atomic_int64_t last_reap_ns_ = 0;

  std::atomic_bool kill_{false};
  std::thread poll_thread_;

  ErrorCode try_subscribe(std::unique_ptr<SubscriptionImpl>& subscription) {
    std::unique_ptr<shm::SubscriberLease> lease;
    try {
      lease = subscriber_registry_->register_subscriber(!config_.zero_copy_reads);
    } catch (const std::exception& e) {
      REDLINE_LOG(ERROR) << "Failed to register subscriber on " << name_ << ": " << e.what();
      return ErrorCode::UNKNOWN_ERROR;
    }
    if (!lease) {
      return ErrorCode::SUBSCRIBER_REGISTRY_FULL;
    }
    subscription = std::make_unique<SubscriptionImpl>(segment_mapping_, config_, std::move(lease));
    return ErrorCode::OK;
  }

  template <typename Deliver>
  void run(SubscriptionImpl& subscription, Deliver&& deliver) {
    while (!kill_.load(std::memory_order_relaxed)) {
      if (subscription.poll_with(deliver, POLL_BATCH) == 0) {
        subscription.wait_until(std::numeric_limits<int64_t>::max(), &kill_);
      }
    }
  }

  // Reserves `count` consecutive sequences once every live subscriber has consumed the messages currently in their
  // slots. The compare-exchange makes the capacity check and the reservation one step, so concurrent producers can't
  // overrun a subscriber between them.
  bool reserve_reliable(const uint64_t count, const int64_t timeout_us, uint64_t& first_seq) {
    uint64_t write_seq = segment_->write_seq.load(std::memory_order_seq_cst);
    int64_t start_us = -1;
    uint32_t spins = 0;

    while (true) {
      if (!fits(write_seq, count, cached_min_read_seq_.load(std::memory_order_acquire))) {
        // Rescanning reads every subscriber's read_seq, which they write for every message. If waiting producers
        // rescanned constantly they would slow down the subscribers they are waiting for, so one thread per process
        // scans, every SPINS_PER_SCAN spins, and the rest wait for its result.
        uint64_t min_read_seq = cached_min_read_seq_.load(std::memory_order_acquire);
        if (spins % SPINS_PER_SCAN == 0 && !scanning_.load(std::memory_order_relaxed) &&
            !scanning_.exchange(true, std::memory_order_acquire)) {
          min_read_seq = subscriber_registry_->min_read_seq().value_or(write_seq);
          cached_min_read_seq_.store(min_read_seq, std::memory_order_release);
          scanning_.store(false, std::memory_order_release);
        }

        if (!fits(write_seq, count, min_read_seq)) {
          maybe_reap();

          if (start_us < 0) {
            start_us = now_benchmark_us();
          } else if (timeout_us >= 0 && now_benchmark_us() - start_us >= timeout_us) {
            return false;
          }

          if (++spins % SPINS_BEFORE_YIELD == 0) {
            std::this_thread::yield();
          } else {
            cpu_relax();
          }
          write_seq = segment_->write_seq.load(std::memory_order_seq_cst);
          continue;
        }
      }

      if (segment_->write_seq.compare_exchange_weak(write_seq, write_seq + count, std::memory_order_seq_cst)) {
        first_seq = write_seq;
        return true;
      }
    }
  }

  // Whether sequences [write_seq, write_seq + count) can be written without overwriting anything at or after min_read_seq
  bool fits(const uint64_t write_seq, const uint64_t count, const uint64_t min_read_seq) const noexcept {
    // min_read_seq can be ahead of a stale write_seq, which means there is plenty of room
    return write_seq < min_read_seq || write_seq + count - min_read_seq <= slot_count_;
  }

  // Checks for subscribers that have died or (if configured) stalled, at most every REAP_INTERVAL_NS per process
  void maybe_reap() noexcept {
    const int64_t now = now_ns();
    int64_t last = last_reap_ns_.load(std::memory_order_relaxed);
    if (now - last < REAP_INTERVAL_NS || !last_reap_ns_.compare_exchange_strong(last, now)) {
      return;
    }
    if (subscriber_registry_->reap(now, config_.subscriber_timeout_us * 1000) > 0) {
      cached_min_read_seq_.store(0, std::memory_order_release); // force a rescan
    }
  }

  void run_shared() {
    std::shared_ptr<const Callbacks> callbacks;
    uint64_t version = std::numeric_limits<uint64_t>::max();
    run(*shared_subscription_, [&](const Message& message) {
      const uint64_t current = callbacks_version_.load(std::memory_order_acquire);
      if (current != version) {
        std::lock_guard lk(subscriber_creation_mutex_);
        callbacks = callbacks_;
        version = current; // if it moved on again since, we'll just pick it up next message
      }
      for (size_t i = 0; i < callbacks->size(); ++i) {
        consume_safely(*(*callbacks)[i], message, i);
      }
    });
  }
};

std::unique_ptr<Bus> create_bus(const std::string &name, const Config &config) {
  return std::make_unique<BusImpl>(name, config);
}

ErrorCode destroy_bus(const std::string& name) {
  if (shm_unlink(segment_name(name).c_str()) == 0) {
    return ErrorCode::OK;
  }
  return errno == ENOENT ? ErrorCode::NOT_FOUND : ErrorCode::UNKNOWN_ERROR;
}
} // namespace redline
