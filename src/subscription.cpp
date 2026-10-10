#include "subscription.hpp"

#include <algorithm>
#include <unistd.h>

#include "cpu.hpp"
#include "log.hpp"
#include "shm/futex.hpp"

namespace redline {
namespace {
constexpr int64_t SLEEP_TIMEOUT_NS = 50'000'000; // upper bound on how long a sleeping poll thread takes to notice shutdown
constexpr int64_t HEARTBEAT_INTERVAL_NS = 1'000'000;
}

SubscriptionImpl::SubscriptionImpl(std::shared_ptr<shm::SegmentMapping> mapping, const SubscriptionSettings& settings,
                                   std::unique_ptr<shm::SubscriberLease> lease)
    : mapping_(std::move(mapping)),
      segment_(mapping_->segment()),
      ring_buffer_(mapping_),
      publisher_registry_(mapping_),
      lease_(std::move(lease)),
      entry_(lease_->entry()),
      name_(mapping_->name()),
      slot_count_(segment_->slot_count),
      settings_(settings),
      histogram_(settings.histogram_significant_figures),
      read_seq_(entry_.read_seq.load(std::memory_order_acquire)),
      last_heartbeat_ns_(now_ns()) {
  if (!settings_.zero_copy) {
    buffer_.resize(segment_->slot_size_bytes);
  }
}

bool SubscriptionImpl::wait(const std::chrono::microseconds timeout) {
  return wait_until(raw_now_ns() + timeout.count() * 1000, nullptr);
}

SubscriberMetricsSnapshot SubscriptionImpl::get_metrics() const {
  SubscriberMetricsSnapshot snap;
  const uint64_t read_seq = entry_.read_seq.load(std::memory_order_relaxed);
  const uint64_t write_seq = segment_->write_seq.load(std::memory_order_relaxed);

  snap.messages_consumed = entry_.messages_consumed.load(std::memory_order_relaxed);
  snap.dropped_messages = entry_.dropped_count.load(std::memory_order_relaxed);
  snap.current_lag = write_seq > read_seq ? write_seq - read_seq : 0;
  snap.max_lag = entry_.max_lag.load(std::memory_order_relaxed);
  snap.last_latency_ns = entry_.last_latency_ns.load(std::memory_order_relaxed);

  if (settings_.latency_metrics) {
    snap.p50_latency_ns = histogram_.percentile(50.0);
    snap.p90_latency_ns = histogram_.percentile(90.0);
    snap.p99_latency_ns = histogram_.percentile(99.0);
    snap.p999_latency_ns = histogram_.percentile(99.9);
  }
  return snap;
}

void SubscriptionImpl::reset_metrics() {
  histogram_.reset();
  entry_.max_lag.store(0, std::memory_order_relaxed);
}

bool SubscriptionImpl::wait_until(const int64_t deadline_ns, const std::atomic_bool* stop) {
  const int64_t start_ns = raw_now_ns();
  while (true) {
    if (ready()) {
      return true;
    }
    if (stop && stop->load(std::memory_order_relaxed)) {
      return false;
    }
    const int64_t now = raw_now_ns();
    if (now >= deadline_ns) {
      return false;
    }
    check_stalled(now);
    if (now - start_ns < settings_.spin_duration_ns) {
      cpu_relax();
      continue;
    }
    heartbeat(now_ns());
    sleep_until_ready(std::min(deadline_ns - now, SLEEP_TIMEOUT_NS), stop);
  }
}

// Called while read_seq_ isn't ready. If it has been reserved but stayed unfinished for a while, its publisher may
// have died mid-publish, so ask for recovery (which leaves it alone if its publisher is alive).
void SubscriptionImpl::check_stalled(const int64_t now) noexcept {
  if (stalled_since_ns_ == 0) {
    stalled_since_ns_ = now;
    return;
  }
  if (now - stalled_since_ns_ < shm::STALL_RECOVERY_NS) {
    return;
  }
  stalled_since_ns_ = now;
  // Beyond write_seq we are just waiting for new messages; only checked occasionally, since publishers write it constantly
  if (read_seq_ < segment_->write_seq.load(std::memory_order_seq_cst)) {
    publisher_registry_.recover();
  }
}

void SubscriptionImpl::heartbeat(const int64_t now) noexcept {
  entry_.heartbeat_ns.store(now, std::memory_order_relaxed);
  last_heartbeat_ns_ = now;
}

void SubscriptionImpl::record_metrics(const uint64_t lag, const int64_t publish_timestamp_ns) noexcept {
  // Single writer, so plain stores instead of locked read-modify-writes
  entry_.messages_consumed.store(entry_.messages_consumed.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
  if (lag > entry_.max_lag.load(std::memory_order_relaxed)) {
    entry_.max_lag.store(lag, std::memory_order_relaxed);
  }

  // The heartbeat only needs to be accurate to a few milliseconds, so without latency metrics it uses the coarse
  // clock, which costs a few nanoseconds rather than a few tens
  const int64_t now = settings_.latency_metrics ? now_ns() : coarse_now_ns();
  if (settings_.latency_metrics) {
    const int64_t latency_ns = now - publish_timestamp_ns;
    entry_.last_latency_ns.store(latency_ns, std::memory_order_relaxed);
    histogram_.record(latency_ns);
  }
  if (now - last_heartbeat_ns_ > HEARTBEAT_INTERVAL_NS) {
    heartbeat(now);
  }
}

void SubscriptionImpl::add_dropped(const uint64_t count) noexcept {
  entry_.dropped_count.store(entry_.dropped_count.load(std::memory_order_relaxed) + count, std::memory_order_relaxed);
}

// Lapped by producers: skip to the oldest message that can still be in the ring, or the newest
void SubscriptionImpl::skip_overwritten() noexcept {
  const uint64_t write_seq = segment_->write_seq.load(std::memory_order_acquire);
  const uint64_t resume_at = settings_.on_lapped == OnLapped::RESUME_AT_NEWEST
                                 ? (write_seq > 0 ? write_seq - 1 : 0)
                                 : (write_seq >= slot_count_ ? write_seq - slot_count_ + 1 : 0);
  const uint64_t new_read_seq = std::max(read_seq_ + 1, resume_at);
  add_dropped(new_read_seq - read_seq_);
  entry_.read_seq.store(new_read_seq, std::memory_order_release);
  read_seq_ = new_read_seq;
}

// Called when our entry is no longer OK: we were evicted for blocking publishers (DEAD). Rejoins at the head and
// counts everything skipped as dropped. EMPTY can't normally happen, since nobody reclaims an entry whose lease we
// hold, but if it does the lease means nobody else can have claimed it, so take it back. Returns false if the entry
// is in some other state, i.e. there is nothing to poll.
bool SubscriptionImpl::rejoin() {
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

void SubscriptionImpl::sleep_until_ready(const int64_t timeout_ns, const std::atomic_bool* stop) {
  segment_->waiters.fetch_add(1, std::memory_order_seq_cst);
  // Pairs with the fence in wake_if_sleeping: either the publisher sees our waiter count, or we see its commit
  std::atomic_thread_fence(std::memory_order_seq_cst);
  const uint32_t futex_value = segment_->commit_futex.load(std::memory_order_seq_cst);
  if (!ready() && !(stop && stop->load())) {
    shm::futex_wait(&segment_->commit_futex, futex_value, std::chrono::microseconds(timeout_ns / 1000));
  }
  segment_->waiters.fetch_sub(1, std::memory_order_seq_cst);
}
}
