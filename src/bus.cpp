#include "redline/bus.hpp"

#include <algorithm>
#include <array>
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
#include <utility>
#include <vector>

#include "log.hpp"

#include "redline/config.hpp"
#include "redline/utils.hpp"
#include "cpu.hpp"
#include "histogram.hpp"
#include "shm/futex.hpp"
#include "shm/layout.hpp"
#include "shm/publisher_registry.hpp"
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
// How long a reserved sequence can stay unfinished before a waiting subscriber or publisher checks whether its
// publisher has died. Recovery never touches a live publisher's sequence, so this only bounds how often we look.
constexpr int64_t STALL_RECOVERY_NS = 1'000'000;

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
  SubscriptionImpl(std::shared_ptr<shm::SegmentMapping> mapping, const Config& config, const bool zero_copy,
                   std::unique_ptr<shm::SubscriberLease> lease)
      : mapping_(std::move(mapping)),
        segment_(mapping_->segment()),
        ring_buffer_(mapping_),
        publisher_registry_(mapping_),
        lease_(std::move(lease)),
        entry_(lease_->entry()),
        name_(mapping_->name()),
        slot_count_(segment_->slot_count),
        zero_copy_(zero_copy),
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
        check_stalled(now_benchmark_ns());
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
      check_stalled(now);
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
  shm::PublisherRegistry publisher_registry_;
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
  int64_t stalled_since_ns_ = 0; // when we first found nothing to read at read_seq_, 0 while messages are flowing
  std::vector<uint8_t> buffer_; // copy mode: messages are copied here before being handed to callbacks

  bool ready() const noexcept {
    return entry_.state.load(std::memory_order_relaxed) != uint32_t(shm::SubscriberState::OK) ||
           ring_buffer_.is_ready(read_seq_);
  }

  // Called while read_seq_ isn't ready. If it has been reserved but stayed unfinished for a while, its publisher may
  // have died mid-publish, so ask for recovery (which leaves it alone if its publisher is alive).
  void check_stalled(const int64_t now) noexcept {
    if (stalled_since_ns_ == 0) {
      stalled_since_ns_ = now;
      return;
    }
    if (now - stalled_since_ns_ < STALL_RECOVERY_NS) {
      return;
    }
    stalled_since_ns_ = now;
    // Beyond write_seq we are just waiting for new messages; only checked occasionally, since publishers write it constantly
    if (read_seq_ < segment_->write_seq.load(std::memory_order_seq_cst)) {
      publisher_registry_.recover();
    }
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

class BusImpl : public Bus, private detail::ClaimOwner {
public:
  BusImpl(const std::string& name, const Config& config) : name_(name), config_(config) {
    segment_mapping_ = shm::SegmentMapping::open_or_create(segment_name(name), config);
    segment_ = segment_mapping_->segment();
    slot_count_ = segment_->slot_count;
    ring_buffer_ = std::make_unique<shm::RingBuffer>(segment_mapping_);
    subscriber_registry_ = std::make_unique<shm::SubscriberRegistry>(segment_mapping_);
    publisher_registry_ = std::make_unique<shm::PublisherRegistry>(segment_mapping_);
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

    if (count > slot_count_) {
      return ErrorCode::INVALID_ARGUMENT;
    }

    ErrorCode error = ErrorCode::OK;
    PublisherSlot* publisher = acquire_publisher(error);
    if (!publisher) {
      return error;
    }

    const int64_t timestamp_ns = now_ns();
    uint64_t first_seq;
    if (!reserve(count, timeout_us, first_seq, *publisher->lease)) {
      release_publisher(publisher);
      return ErrorCode::BACKPRESSURE_TIMEOUT;
    }

    // No payload means a later lap already took the slot, which subscribers will account for as a drop
    for (uint64_t i = 0; i < count; ++i) {
      if (uint8_t* payload = begin_slot(first_seq + i)) {
        ring_buffer_->fill_and_commit(first_seq + i, payload, messages[i], timestamp_ns);
      }
    }
    publisher->lease->done();
    release_publisher(publisher);
    wake_if_sleeping();
    return ErrorCode::OK;
  }

  ErrorCode claim(Claim& claim, const int64_t timeout_us) override {
    claim.abort();
    // The claim keeps the publisher lease until it is committed or aborted, so its record covers the open sequence
    ErrorCode error = ErrorCode::OK;
    PublisherSlot* publisher = acquire_publisher(error);
    if (!publisher) {
      return error;
    }
    while (true) {
      uint64_t seq;
      if (!reserve(1, timeout_us, seq, *publisher->lease)) {
        release_publisher(publisher);
        return ErrorCode::BACKPRESSURE_TIMEOUT;
      }
      if (uint8_t* data = begin_slot(seq)) {
        claim = Claim(this, seq, std::span<uint8_t>(data, segment_->slot_size_bytes), publisher);
        return ErrorCode::OK;
      }
      // Producers lapped each other and a later lap already took this slot, so this sequence is lost to subscribers
      // (they count it as dropped). Take the next one.
    }
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

    if (!valid(options, !options.own_thread)) {
      return ErrorCode::INVALID_ARGUMENT;
    }

    if (options.own_thread) {
      std::unique_ptr<SubscriptionImpl> subscription;
      if (const auto ec = try_subscribe(subscription, options); ec != ErrorCode::OK) {
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
      if (const auto ec = try_subscribe(shared_subscription_, options); ec != ErrorCode::OK) {
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

  std::unique_ptr<Subscription> subscribe(const SubscriberOptions& options) override {
    if (options.own_thread || !valid(options, false)) {
      throw std::invalid_argument("invalid SubscriberOptions for subscribe(): own_thread is for add_subscriber, and "
                                  "zero_copy_reads requires RELIABLE");
    }
    return make_subscription(options);
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
  // A publisher lease, and whether a publish or Claim in this process is using it. Each in-flight publish needs a lease
  // of its own (a lease records one reservation at a time), so they are created as needed and kept until the Bus is
  // destroyed: their number tracks the most publishes this Bus ever had in flight at once.
  struct alignas(shm::CACHE_LINE_SIZE) PublisherSlot {
    std::atomic_bool in_use{false};
    std::unique_ptr<shm::PublisherLease> lease; // set before the slot is counted, never changed after
  };
  static constexpr size_t PUBLISHER_SLOTS_PER_CHUNK = 64;
  static constexpr size_t MAX_PUBLISHER_CHUNKS = shm::MAX_REGISTRY_ENTRIES / PUBLISHER_SLOTS_PER_CHUNK;
  struct PublisherSlotChunk {
    std::array<PublisherSlot, PUBLISHER_SLOTS_PER_CHUNK> slots;
  };

  void commit_claim(const uint64_t sequence, const uint32_t size, void* context) noexcept override {
    ring_buffer_->commit(sequence, size, now_ns());
    finish_claim(static_cast<PublisherSlot*>(context));
  }

  void abort_claim(const uint64_t sequence, void* context) noexcept override {
    ring_buffer_->abort(sequence, now_ns());
    finish_claim(static_cast<PublisherSlot*>(context)); // subscribers waiting on this sequence can move past it
  }

  void finish_claim(PublisherSlot* publisher) noexcept {
    publisher->lease->done();
    release_publisher(publisher);
    wake_if_sleeping();
  }

  // Claims seq's slot. If an older lap's publisher is still writing it, waits, and if that goes on for a while checks
  // whether it has died. Returns null if a later lap already took the slot.
  uint8_t* begin_slot(const uint64_t seq) noexcept {
    int64_t waiting_since_ns = 0;
    while (true) {
      const auto begin = ring_buffer_->try_begin_write(seq);
      if (begin.payload || begin.lapped) {
        return begin.payload;
      }
      const int64_t now = now_benchmark_ns();
      if (waiting_since_ns == 0) {
        waiting_since_ns = now;
      } else if (now - waiting_since_ns > STALL_RECOVERY_NS) {
        publisher_registry_->recover();
        waiting_since_ns = now;
      }
      cpu_relax();
    }
  }

  PublisherSlot& publisher_slot(const size_t i) noexcept {
    return publisher_chunks_[i / PUBLISHER_SLOTS_PER_CHUNK].load(std::memory_order_acquire)->slots[i % PUBLISHER_SLOTS_PER_CHUNK];
  }

  PublisherSlot* acquire_publisher(ErrorCode& error) {
    thread_local size_t hint = 0; // where this thread last found a free slot, so threads tend to keep to their own
    const size_t count = publisher_slot_count_.load(std::memory_order_acquire);
    for (size_t k = 0; k < count; ++k) {
      const size_t i = (hint + k) % count;
      PublisherSlot& slot = publisher_slot(i);
      if (!slot.in_use.load(std::memory_order_relaxed) && !slot.in_use.exchange(true, std::memory_order_acquire)) {
        hint = i;
        return &slot;
      }
    }

    // All busy: register another lease
    std::lock_guard lk(publisher_slots_mutex_);
    const size_t n = publisher_slot_count_.load(std::memory_order_relaxed);
    if (n == MAX_PUBLISHER_CHUNKS * PUBLISHER_SLOTS_PER_CHUNK) {
      error = ErrorCode::PUBLISHER_REGISTRY_FULL;
      return nullptr;
    }
    std::unique_ptr<shm::PublisherLease> lease;
    try {
      lease = publisher_registry_->register_publisher();
    } catch (const std::exception& e) {
      REDLINE_LOG(ERROR) << "Failed to register publisher on " << name_ << ": " << e.what();
      error = ErrorCode::UNKNOWN_ERROR;
      return nullptr;
    }
    if (!lease) {
      error = ErrorCode::PUBLISHER_REGISTRY_FULL;
      return nullptr;
    }
    if (n % PUBLISHER_SLOTS_PER_CHUNK == 0) {
      publisher_chunk_storage_.push_back(std::make_unique<PublisherSlotChunk>());
      publisher_chunks_[n / PUBLISHER_SLOTS_PER_CHUNK].store(publisher_chunk_storage_.back().get(), std::memory_order_release);
    }
    PublisherSlot& slot = publisher_slot(n);
    slot.lease = std::move(lease);
    slot.in_use.store(true, std::memory_order_relaxed);
    publisher_slot_count_.store(n + 1, std::memory_order_release);
    hint = n;
    return &slot;
  }

  static void release_publisher(PublisherSlot* publisher) noexcept {
    publisher->in_use.store(false, std::memory_order_release);
  }

  // Only pay for a syscall when a subscriber is actually asleep. The fence pairs with the one in sleep_until_ready:
  // either we see its waiter count, or it sees our commit.
  void wake_if_sleeping() noexcept {
    std::atomic_thread_fence(std::memory_order_seq_cst);
    if (segment_->waiters.load(std::memory_order_relaxed) != 0) {
      wake_sleepers(segment_);
    }
  }

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
  std::unique_ptr<shm::PublisherRegistry> publisher_registry_;

  std::array<std::atomic<PublisherSlotChunk*>, MAX_PUBLISHER_CHUNKS> publisher_chunks_{};
  std::atomic_size_t publisher_slot_count_ = 0;
  std::mutex publisher_slots_mutex_;
  std::vector<std::unique_ptr<PublisherSlotChunk>> publisher_chunk_storage_; // guarded by publisher_slots_mutex_

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

  // Shared-thread callbacks always use the Config; the overrides are for subscribers with a position of their own
  bool valid(const SubscriberOptions& options, const bool shared_thread) const noexcept {
    if (shared_thread && (options.backpressure_mode || options.zero_copy_reads)) {
      return false;
    }
    return !(options.zero_copy_reads.value_or(config_.zero_copy_reads) &&
             options.backpressure_mode.value_or(config_.backpressure_mode) != BackPressureMode::RELIABLE);
  }

  // Returns nullptr if the registry is full. Options must already be valid.
  std::unique_ptr<SubscriptionImpl> make_subscription(const SubscriberOptions& options) {
    const bool reliable = options.backpressure_mode.value_or(config_.backpressure_mode) == BackPressureMode::RELIABLE;
    const bool zero_copy = options.zero_copy_reads.value_or(config_.zero_copy_reads);
    const uint32_t flags = (reliable ? shm::SUBSCRIBER_RELIABLE : 0) | (zero_copy ? shm::SUBSCRIBER_NO_EVICT : 0);

    auto lease = subscriber_registry_->register_subscriber(flags);
    if (!lease) {
      return nullptr;
    }
    return std::make_unique<SubscriptionImpl>(segment_mapping_, config_, zero_copy, std::move(lease));
  }

  ErrorCode try_subscribe(std::unique_ptr<SubscriptionImpl>& subscription, const SubscriberOptions& options) {
    try {
      subscription = make_subscription(options);
    } catch (const std::exception& e) {
      REDLINE_LOG(ERROR) << "Failed to register subscriber on " << name_ << ": " << e.what();
      return ErrorCode::UNKNOWN_ERROR;
    }
    return subscription ? ErrorCode::OK : ErrorCode::SUBSCRIBER_REGISTRY_FULL;
  }

  template <typename Deliver>
  void run(SubscriptionImpl& subscription, Deliver&& deliver) {
    while (!kill_.load(std::memory_order_relaxed)) {
      if (subscription.poll_with(deliver, POLL_BATCH) == 0) {
        subscription.wait_until(std::numeric_limits<int64_t>::max(), &kill_);
      }
    }
  }

  // Reserves `count` consecutive sequences once every live RELIABLE subscriber has consumed the messages currently in
  // their slots (immediately, if there are none). The compare-exchange makes the capacity check and the reservation one
  // step, so concurrent producers can't overrun a subscriber between them.
  // The lease records each attempt before it is made, so recovery can tell whose sequences are whose.
  bool reserve(const uint64_t count, const int64_t timeout_us, uint64_t& first_seq, shm::PublisherLease& lease) {
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
            lease.done();
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

      lease.reserving(write_seq, count);
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

  // Checks for subscribers that have died or (if configured) stalled, and for publishers that died mid-publish leaving
  // subscribers stuck, at most every REAP_INTERVAL_NS per process
  void maybe_reap() noexcept {
    const int64_t now = now_ns();
    int64_t last = last_reap_ns_.load(std::memory_order_relaxed);
    if (now - last < REAP_INTERVAL_NS || !last_reap_ns_.compare_exchange_strong(last, now)) {
      return;
    }
    if (subscriber_registry_->reap(now, config_.subscriber_timeout_us * 1000) > 0) {
      cached_min_read_seq_.store(0, std::memory_order_release); // force a rescan
    }
    publisher_registry_->recover();
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

Claim::Claim(Claim&& other) noexcept
    : owner_(std::exchange(other.owner_, nullptr)), sequence_(other.sequence_), data_(std::exchange(other.data_, {})),
      context_(std::exchange(other.context_, nullptr)) {}

Claim& Claim::operator=(Claim&& other) noexcept {
  if (this != &other) {
    abort();
    owner_ = std::exchange(other.owner_, nullptr);
    sequence_ = other.sequence_;
    data_ = std::exchange(other.data_, {});
    context_ = std::exchange(other.context_, nullptr);
  }
  return *this;
}

Claim::~Claim() {
  abort();
}

ErrorCode Claim::commit(const size_t size) noexcept {
  if (!owner_) {
    return ErrorCode::INVALID_ARGUMENT;
  }
  if (size > data_.size()) {
    return ErrorCode::MESSAGE_TOO_LARGE;
  }
  std::exchange(owner_, nullptr)->commit_claim(sequence_, uint32_t(size), std::exchange(context_, nullptr));
  data_ = {};
  return ErrorCode::OK;
}

void Claim::abort() noexcept {
  if (owner_) {
    std::exchange(owner_, nullptr)->abort_claim(sequence_, std::exchange(context_, nullptr));
    data_ = {};
  }
}

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
