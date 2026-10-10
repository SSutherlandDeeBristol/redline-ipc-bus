#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "redline/bus.hpp"
#include "redline/utils.hpp"
#include "clock.hpp"
#include "histogram.hpp"
#include "shm/publisher_registry.hpp"
#include "shm/ring_buffer.hpp"
#include "shm/segment.hpp"
#include "shm/subscriber_registry.hpp"

namespace redline {
struct SubscriptionSettings {
  bool zero_copy = false;
  bool latency_metrics = true;
  OnLapped on_lapped = OnLapped::RESUME_AT_OLDEST;
  int64_t spin_duration_ns = 0;
  int histogram_significant_figures = 3;
};

// One registry entry and read position on the bus. Backs Bus::subscribe(), the callbacks sharing a Bus' poll thread,
// and each own_thread callback.
class SubscriptionImpl final : public Subscription {
public:
  SubscriptionImpl(std::shared_ptr<shm::SegmentMapping> mapping, const SubscriptionSettings& settings,
                   std::unique_ptr<shm::SubscriberLease> lease);

  size_t poll(const Callback& callback, size_t max_messages) override { return poll_with(callback, max_messages); }
  bool wait(std::chrono::microseconds timeout) override;
  SubscriberMetricsSnapshot get_metrics() const override;
  void reset_metrics() override;

  // Delivers up to max_messages ready messages to `deliver`, which is called with each Message
  template <typename Deliver>
  size_t poll_with(Deliver&& deliver, size_t max_messages);

  // Waits until a message is ready to poll, the deadline (CLOCK_MONOTONIC_RAW ns) passes, or *stop becomes true.
  // Spins for consumer_busy_spin_duration_us before sleeping on the bus' futex.
  bool wait_until(int64_t deadline_ns, const std::atomic_bool* stop);

private:
  std::shared_ptr<shm::SegmentMapping> mapping_;
  shm::SHMSegment* segment_;
  shm::RingBuffer ring_buffer_;
  shm::PublisherRegistry publisher_registry_;
  std::unique_ptr<shm::SubscriberLease> lease_;
  shm::SubscriberEntry& entry_;
  std::string name_;
  uint64_t slot_count_;
  SubscriptionSettings settings_;

  Histogram histogram_;

  // Only touched by the polling thread
  uint64_t read_seq_;
  int64_t last_heartbeat_ns_;
  int64_t stalled_since_ns_ = 0;  // when we first found nothing to read at read_seq_, 0 while messages are flowing
  std::vector<uint8_t> buffer_;   // copy mode: messages are copied here before being handed to callbacks

  bool ready() const noexcept {
    return entry_.state.load(std::memory_order_relaxed) != uint32_t(shm::SubscriberState::OK) ||
           ring_buffer_.is_ready(read_seq_);
  }

  void check_stalled(int64_t now) noexcept;
  void heartbeat(int64_t now) noexcept;
  void record_metrics(uint64_t lag, int64_t publish_timestamp_ns) noexcept;
  void add_dropped(uint64_t count) noexcept;
  void skip_overwritten() noexcept;
  bool rejoin();
  void sleep_until_ready(int64_t timeout_ns, const std::atomic_bool* stop);
};

template <typename Deliver>
size_t SubscriptionImpl::poll_with(Deliver&& deliver, const size_t max_messages) {
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
    const auto outcome = settings_.zero_copy ? ring_buffer_.peek(seq, data) : ring_buffer_.read(seq, buffer_.data());

    if (outcome.result == shm::RingBuffer::ReadResult::NOT_READY) {
      check_stalled(raw_now_ns());
      break;
    }
    stalled_since_ns_ = 0;
    if (outcome.result == shm::RingBuffer::ReadResult::OVERWRITTEN) {
      skip_overwritten();
      continue;
    }
    if (outcome.result == shm::RingBuffer::ReadResult::SKIPPED || outcome.result == shm::RingBuffer::ReadResult::LOST) {
      // SKIPPED is an aborted claim: nothing was published, so it is neither delivered nor dropped. LOST is a
      // message whose publisher died before committing it.
      if (outcome.result == shm::RingBuffer::ReadResult::LOST) {
        add_dropped(1);
      }
      read_seq_ = seq + 1;
      entry_.read_seq.store(seq + 1, std::memory_order_release);
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

    if (settings_.zero_copy) {
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
}
