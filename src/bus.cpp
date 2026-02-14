#include "redline/bus.hpp"

#include <chrono>
#include <iostream>
#include <thread>

#include "glog/logging.h"

#include "redline/config.hpp"
#include "redline/utils.hpp"
#include "histogram.h"
#include "shm/futex.hpp"
#include "shm/layout.hpp"
#include "shm/ring_buffer.hpp"
#include "shm/segment.hpp"
#include "shm/subscriber_registry.hpp"

namespace redline {
class BusImpl : public Bus {
public:
  BusImpl(const std::string& name, const Config& config)
      : name_(name), config_(config), histogram_(1, 1'000'000'000LL, config.latency_histogram_num_buckets, config.latency_histogram_significant_figures) {
    if (config_.max_message_size % 64 != 0) {
      throw std::invalid_argument("max_message_size must be a multiple of 64");
    }

    segment_mapping_ = shm::SegmentMapping::open_or_create("/redline." + name, config);
    segment_ = segment_mapping_->segment();
    ring_buffer_ = std::make_unique<shm::RingBuffer>(segment_mapping_);
    subscriber_registry_ = std::make_unique<shm::SubscriberRegistry>(segment_mapping_);
  }

  ErrorCode publish(const std::span<const uint8_t>& data, const int64_t timeout_us) override {
    if (data.size() > config_.max_message_size) {
      return ErrorCode::MESSAGE_TOO_LARGE;
    }

    int64_t timestamp_ns = now_ns();

    if (config_.backpressure_mode == BackPressureMode::RELIABLE) {
      uint64_t write_seq = segment_->write_seq.load(std::memory_order_acquire);
      uint64_t slowest_seq = compute_slowest_subscriber_read_seq();

      // Backpressure check: don't overwrite unread slots
      const int64_t start_time_us = now_benchmark_us();
      while (write_seq - slowest_seq >= segment_->slot_count && (timeout_us < 0 || now_benchmark_us() - start_time_us < timeout_us)) {
        // Wait for the slowest subscriber to catch up
        write_seq = segment_->write_seq.load(std::memory_order_acquire);
        slowest_seq = compute_slowest_subscriber_read_seq();
      }

      if (write_seq - slowest_seq >= segment_->slot_count) {
        // Timed out waiting for backpressure to clear
        return ErrorCode::BACKPRESSURE_TIMEOUT;
      }
    }

    const auto push_result = ring_buffer_->try_push(data, timestamp_ns);
    if (push_result.result == shm::RingBuffer::WriteResult::OK) {
      std::atomic_uint32_t &slot_state = segment_mapping_->header_at(push_result.write_seq)->state;
      // Wake up all subscribers waiting on this slot
      // TODO: handle futex errors
      shm::futex_wake(&slot_state, std::numeric_limits<int>::max());
      shm::futex_wake(&segment_->write_seq_futex, std::numeric_limits<int>::max());
      return ErrorCode::OK;
    }

    return ErrorCode::UNKNOWN_ERROR;
  }

ErrorCode add_subscriber(const std::function<void(const Message&)>& callback) override {
    return add_subscriber(create_subscriber(callback));
  }

  ErrorCode add_subscriber(std::shared_ptr<Subscriber> subscriber) override {
    // Lock here to ensure that we don't need one "ready" atomic per subscriber callback, since the poll thread can read subscriber_callbacks_ without holding this mutex
    std::lock_guard lk(subscriber_creation_mutex_);

    const size_t subscriber_callback_index = next_subscriber_callback_count_.load();

    if (subscriber_callback_index >= subscriber_callbacks_.size()) {
      return ErrorCode::SUBSCRIBER_CALLBACKS_FULL;
    }

    subscriber_callbacks_[subscriber_callback_index] = std::move(subscriber);
    next_subscriber_callback_count_.fetch_add(1, std::memory_order_release);

    // Return if we have already registered this instance of Bus as a subscriber, otherwise we need to register and start the poll thread
    if (subscriber_callback_index > 0) {
      return ErrorCode::OK;
    }

    const auto register_subscriber_result = subscriber_registry_->register_subscriber(subscriber_index_);

    if (register_subscriber_result == shm::SubscribeResult::REGISTRY_FULL) {
      return ErrorCode::SUBSCRIBER_REGISTRY_FULL;
    }

    // Set up a thread to poll for new messages and call the subscriber
    poll_thread_ = std::thread([this]() { poll(); });

    return ErrorCode::OK;
  }

  SubscriberMetricsSnapshot get_subscriber_metrics() const override {
    SubscriberMetricsSnapshot snap;

    const auto &entry = segment_->subscribers[subscriber_index_];

    const uint64_t read_seq =
        entry.read_seq.load(std::memory_order_relaxed);
    const uint64_t write_seq =
        segment_->write_seq.load(std::memory_order_relaxed);

    snap.messages_consumed =
        entry.messages_consumed.load(std::memory_order_relaxed);
    snap.dropped_messages =
        entry.dropped_count.load(std::memory_order_relaxed);
    snap.current_lag = write_seq - read_seq;
    snap.max_lag =
        entry.max_lag.load(std::memory_order_relaxed);
    snap.last_latency_ns = entry.last_latency_ns.load(std::memory_order_relaxed);

    snap.p50_latency_ns = histogram_.percentile(50.0);
    snap.p90_latency_ns = histogram_.percentile(90.0);
    snap.p99_latency_ns = histogram_.percentile(99.0);
    snap.p999_latency_ns = histogram_.percentile(99.9);

    return snap;
  }

  ~BusImpl() override {
    // Stop the poll thread if running
    kill = true;
    if (poll_thread_.joinable()) {
      poll_thread_.join();
    }

    // Release this subscriber's spot in the registry
    subscriber_registry_->remove_subscriber(subscriber_index_);
  }

private:
  std::string name_;
  Config config_;

  std::shared_ptr<shm::SegmentMapping> segment_mapping_;
  shm::SHMSegment* segment_;

  std::unique_ptr<shm::RingBuffer> ring_buffer_;
  std::unique_ptr<shm::SubscriberRegistry> subscriber_registry_;

  uint64_t subscriber_index_ = 0;

  std::array<std::shared_ptr<Subscriber>, 128> subscriber_callbacks_; // TODO: make this dynamicly sized
  std::mutex subscriber_creation_mutex_;
  std::atomic_size_t next_subscriber_callback_count_ = 0;

  Histogram histogram_;

  std::atomic_bool kill{false};
  std::thread poll_thread_;

  uint64_t compute_slowest_subscriber_read_seq() {
    uint64_t min_read_seq = std::numeric_limits<uint64_t>::max();
    bool found_active_subscriber = false;

    for (const auto& subscriber : segment_->subscribers) {
      if (subscriber.state.load() == uint32_t(shm::SubscriberState::OK)) {
        found_active_subscriber = true;
        min_read_seq = std::min(min_read_seq, subscriber.read_seq.load(std::memory_order_acquire));
      }
    }

    return found_active_subscriber ? min_read_seq : segment_->write_seq.load(std::memory_order_acquire);
  }

  void poll() {
    auto start = now_benchmark_us();

    auto& subscriber_entry = segment_->subscribers[subscriber_index_];

    while (!kill) {
      const uint64_t read_seq = subscriber_entry.read_seq.load(std::memory_order_acquire);

      auto pop_result = ring_buffer_->try_pop(read_seq);

      if (pop_result.result == shm::RingBuffer::ReadResult::OK) {
        const uint64_t write_seq = segment_->write_seq.load(std::memory_order_relaxed);

        const uint64_t lag = write_seq - read_seq;

        subscriber_entry.messages_consumed.fetch_add(1, std::memory_order_relaxed);

        // Update max lag
        uint64_t prev_max = subscriber_entry.max_lag.load(std::memory_order_relaxed);
        while (lag > prev_max &&
               !subscriber_entry.max_lag.compare_exchange_weak(
                   prev_max, lag, std::memory_order_relaxed)) {}

        // Record latency
        const int64_t now = now_ns();
        const int64_t latency_ns = now - pop_result.publish_timestamp_ns;

        subscriber_entry.last_latency_ns.store(latency_ns, std::memory_order_relaxed);

        histogram_.record_value(latency_ns);

        // Deliver message to all registered callbacks
        Message message{
          .sequence = read_seq,
          .publish_timestamp_ns = pop_result.publish_timestamp_ns,
          .data = pop_result.data
        };

        const auto callback_count = next_subscriber_callback_count_.load(std::memory_order_acquire);
        for (size_t i = 0; i < callback_count; ++i) {
          try {
            subscriber_callbacks_[i]->consume(message);
          } catch (const std::exception& e) {
            LOG(ERROR) << "Subscriber callback " << i << " threw exception: " << e.what();
          }
        }

        // Update subscriber position
        subscriber_entry.read_seq.fetch_add(1, std::memory_order_release);
        start = now_benchmark_us();
        subscriber_entry.last_read_timestamp = start;
        continue;
      }

      const uint64_t write_seq = segment_->write_seq.load(std::memory_order_acquire);

      if (pop_result.result == shm::RingBuffer::ReadResult::OVERWRITTEN) {
        const uint64_t new_read_seq = write_seq;
        subscriber_entry.read_seq.store(new_read_seq, std::memory_order_release);
        subscriber_entry.dropped_count.fetch_add(new_read_seq - read_seq, std::memory_order_relaxed);
        continue;
      }

      // Spin-then-sleep logic
      if (now_benchmark_us() - start <= config_.consumer_busy_spin_duration_us) continue;

      if (pop_result.result == shm::RingBuffer::ReadResult::TOO_FAR_AHEAD) {
        if (read_seq >= write_seq) {
          shm::futex_wait(&segment_->write_seq_futex, pop_result.write_seq_futex);
        }
      } else if (pop_result.result == shm::RingBuffer::ReadResult::NOT_COMMITTED_YET ||
                 pop_result.result == shm::RingBuffer::ReadResult::EMPTY) {
        if (read_seq < write_seq) continue;
        auto& slot_state = segment_mapping_->header_at(read_seq)->state;
        shm::futex_wait(&slot_state, pop_result.slot_state);
      }
    }
  }
};

std::unique_ptr<Bus> create_bus(const std::string &name, const Config &config) {
  return std::make_unique<BusImpl>(name, config);
}
} // namespace redline