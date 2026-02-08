#include "redline/bus.hpp"

#include <chrono>
#include <iostream>
#include <thread>

#include "glog/logging.h"

#include "redline/config.hpp"
#include "shm/futex.hpp"
#include "shm/layout.hpp"
#include "shm/ring_buffer.hpp"
#include "shm/segment.hpp"
#include "shm/subscriber_registry.hpp"

namespace redline {
class BusImpl : public Bus {
public:
  BusImpl(const std::string& name, const Config& config)
      : name_(name), config_(config), consumer_busy_spin_duration_(std::chrono::microseconds(config_.consumer_busy_spin_duration_us)) {
    segment_mapping_ = shm::SegmentMapping::open_or_create("/redline." + name, config);
    segment_ = segment_mapping_->segment();
    ring_buffer_ = std::make_unique<shm::RingBuffer>(segment_mapping_);
    subscriber_registry_ = std::make_unique<shm::SubscriberRegistry>(segment_mapping_);
  }

  bool publish(const std::span<const uint8_t>& data) override {
    {
      const uint64_t write_seq = segment_->write_seq.load(std::memory_order_acquire);
      const uint64_t slowest_seq = compute_slowest_subscriber_read_seq();

      // Backpressure check: don't overwrite unread slots
      if (write_seq - slowest_seq >= segment_->slot_count) {
        // Handle backpressure: drop message, yield, or spin
        // For example, drop:
        LOG_EVERY_T(WARNING, 1.0) << "Blocking message due to backpressure " << "(write_seq: " << write_seq << ", slowest_seq: " << slowest_seq << ")";
        return false;
      }
    }

    const auto push_result = ring_buffer_->try_push(data);
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
  std::chrono::duration<int64_t, std::micro> consumer_busy_spin_duration_;

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
      auto start = std::chrono::steady_clock::now();

      while (!stop_polling_) {
        const uint64_t read_seq = segment_->subscribers[subscriber_index_].read_seq.load(std::memory_order_acquire);

        auto* header = segment_mapping_->header_at(read_seq);

        const shm::RingBuffer::PopResult pop_result = ring_buffer_->try_pop(read_seq);

        switch (pop_result.result) {
          case shm::RingBuffer::ReadResult::OK: {
            // Call the subscriber callback with the message data
            const size_t callback_count = subscriber_callback_count_.load();
            for (size_t i = 0; i < callback_count; ++i) {
              subscriber_callbacks_[i](pop_result.data);
            }

            // Increment sequence and update last read timestamp before trying to read the next message
            segment_->subscribers[subscriber_index_].read_seq.fetch_add(1);
            // Reset start timer so that we can busy spin for the next message...
            start = std::chrono::steady_clock::now();
            segment_->subscribers[subscriber_index_].last_read_timestamp = start.time_since_epoch().count();
            continue;
          }
          case shm::RingBuffer::ReadResult::NOT_COMMITTED_YET:
          case shm::RingBuffer::ReadResult::EMPTY: {
            // Busy spin for configured duration then sleep
            const auto now = std::chrono::steady_clock::now();

            if (now - start > consumer_busy_spin_duration_) {
              if (read_seq < segment_->write_seq.load(std::memory_order_acquire)) {
                // Producer advanced, retry instead of sleeping
                continue;
              }

              std::atomic_uint32_t& slot_state = segment_mapping_->header_at(read_seq)->state;
              // TODO: handle futex errors
              shm::futex_wait(&slot_state, pop_result.slot_state);
            }
            break;
          }
          case shm::RingBuffer::ReadResult::TOO_FAR_AHEAD: {
            // Busy spin for configured duration then sleep
            const auto now = std::chrono::steady_clock::now();

            if (now - start > consumer_busy_spin_duration_) {
              if (read_seq < segment_->write_seq.load(std::memory_order_acquire)) {
                // Producer advanced, retry instead of sleeping
                continue;
              }

              // TODO: handle futex errors
              shm::futex_wait(&segment_->write_seq_futex, pop_result.write_seq_futex);
            }
            break;
          }
          case shm::RingBuffer::ReadResult::OVERWRITTEN: {
            // Overrun occurred, move read pointer to latest message
            const uint64_t current_read_seq = read_seq;
            const uint64_t new_read_seq = segment_->write_seq.load(std::memory_order_acquire);

            segment_->subscribers[subscriber_index_].read_seq.store(new_read_seq, std::memory_order_release);
            // TODO: add logging/reporting for subscriber being slow and messages being dropped
            segment_->subscribers[subscriber_index_].dropped_count.fetch_add(new_read_seq - current_read_seq);
            break;
          }
          default:
            break;
        }
      }
    });
  }
};

std::unique_ptr<Bus> createBus(const std::string &name, const Config &config) {
  return std::make_unique<BusImpl>(name, config);
}
} // namespace redline