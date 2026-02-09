#include "redline/bus.hpp"

#include <chrono>
#include <iostream>
#include <thread>

#include "glog/logging.h"

#include "redline/config.hpp"
#include "redline/utils.hpp"
#include "shm/futex.hpp"
#include "shm/layout.hpp"
#include "shm/ring_buffer.hpp"
#include "shm/segment.hpp"
#include "shm/subscriber_registry.hpp"

namespace redline {
class BusImpl : public Bus {
public:
  BusImpl(const std::string& name, const Config& config)
      : name_(name), config_(config) {
    segment_mapping_ = shm::SegmentMapping::open_or_create("/redline." + name, config);
    segment_ = segment_mapping_->segment();
    ring_buffer_ = std::make_unique<shm::RingBuffer>(segment_mapping_);
    subscriber_registry_ = std::make_unique<shm::SubscriberRegistry>(segment_mapping_);
  }

  bool publish(const std::span<const uint8_t>& data) override {
    const int64_t timestamp_us = now_us();

    const uint64_t write_seq = segment_->write_seq.load(std::memory_order_acquire);
    const uint64_t slowest_seq = compute_slowest_subscriber_read_seq();

    // Backpressure check: don't overwrite unread slots
    if (write_seq - slowest_seq >= segment_->slot_count) {
      // Handle backpressure: drop message, yield, or spin
      // For example, drop:
      LOG_EVERY_T(WARNING, 1.0) << "Blocking message due to backpressure " << "(write_seq: " << write_seq << ", slowest_seq: " << slowest_seq << ")";
      return false;
    }

    const auto push_result = ring_buffer_->try_push(data, timestamp_us);
    if (push_result.result == shm::RingBuffer::WriteResult::OK) {
      std::atomic_uint32_t &slot_state = segment_mapping_->header_at(push_result.write_seq)->state;
      // Wake up all subscribers waiting on this slot
      // TODO: handle futex errors
      shm::futex_wake(&slot_state, std::numeric_limits<int>::max());
      shm::futex_wake(&segment_->write_seq_futex, std::numeric_limits<int>::max());
      return true;
    }

    return false;
  }

  void create_subscriber(const Subscriber &subscriber) override {
    const size_t subscriber_callback_index = subscriber_callback_count_.load();

    if (subscriber_callback_index >= subscriber_callbacks_.size()) {
      throw std::runtime_error("Maximum number of subscribers exceeded for bus " + name_);
    }

    subscriber_callbacks_[subscriber_callback_index] = subscriber;
    subscriber_callback_count_.fetch_add(1);

    // Return if we have already registered this instance of Bus as a subscriber, otherwise we need to register and start the poll thread
    if (subscriber_callback_index > 0) {
      return;
    }

    const auto register_subscriber_result = subscriber_registry_->register_subscriber(subscriber_index_);

    if (register_subscriber_result == shm::SubscribeResult::REGISTRY_FULL) {
      // TODO: return proper error instead of throwing...
      throw std::runtime_error("Failed to register subscriber for bus " + name_ + ": registry full");
    }

    // Set up a thread to poll for new messages and call the subscriber
    init_poll_thread();
  }

  ~BusImpl() override {
    // Stop the poll thread if running
    stop_polling_ = true;
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

  std::array<Subscriber, 128> subscriber_callbacks_; // TODO: make this dynamic if we want more subscribers than this?
  std::atomic_size_t subscriber_callback_count_ = 0;

  std::atomic_bool stop_polling_{false};
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

  void init_poll_thread() {
    poll_thread_ = std::thread([this]() {
      auto start = now_benchmark_us();

      auto& subscriber_entry = segment_->subscribers[subscriber_index_];

      while (!stop_polling_) {
        const uint64_t read_seq = subscriber_entry.read_seq.load(std::memory_order_acquire);
        auto pop_result = ring_buffer_->try_pop(read_seq);

        if (pop_result.result == shm::RingBuffer::ReadResult::OK) {
          // Deliver message
          const auto callback_count = subscriber_callback_count_.load(std::memory_order_acquire);
          for (size_t i = 0; i < callback_count; ++i) {
            subscriber_callbacks_[i](Message{
              .sequence = read_seq,
              .data = pop_result.data,
              .publish_timestamp = pop_result.publish_timestamp_us
            });
          }
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
    });
  }
};

std::unique_ptr<Bus> create_bus(const std::string &name, const Config &config) {
  return std::make_unique<BusImpl>(name, config);
}
} // namespace redline