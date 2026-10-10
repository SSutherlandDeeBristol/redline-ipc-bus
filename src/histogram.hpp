#pragma once

#include <algorithm>
#include <atomic>
#include <bit>
#include <cmath>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>

namespace redline {
/*
 * Log-linear histogram of non-negative int64_t values (e.g. latencies in ns), in the style of HdrHistogram.
 *
 * Values below 2^sub_bucket_bits are recorded exactly; above that each power of two is split into 2^(sub_bucket_bits-1)
 * linear sub-buckets, which bounds the relative error to 10^-significant_figures. Values above MAX_VALUE are clamped.
 *
 * record() is wait-free and must only be called from one thread. percentile(), total_count() and reset() may be called
 * from any thread concurrently with record().
 */
class Histogram {
public:
  static constexpr int MAX_VALUE_BITS = 40; // ~18 minutes in ns
  static constexpr int64_t MAX_VALUE = (int64_t(1) << MAX_VALUE_BITS) - 1;

  explicit Histogram(int significant_figures = 3) {
    if (significant_figures < 1 || significant_figures > 5) {
      throw std::invalid_argument("significant_figures must be 1..5");
    }
    // Need 2 * 10^sf sub-buckets per power of two for 10^-sf relative precision
    const auto needed = static_cast<uint64_t>(2 * std::pow(10, significant_figures));
    sub_bucket_bits_ = static_cast<int>(std::bit_width(needed - 1));
    half_count_ = uint64_t(1) << (sub_bucket_bits_ - 1);
    bucket_count_ = static_cast<size_t>(index_for(MAX_VALUE) + 1);
    counts_ = std::make_unique<std::atomic_uint64_t[]>(bucket_count_);
    baseline_.assign(bucket_count_, 0);
  }

  void record(int64_t value) noexcept {
    auto& count = counts_[index_for(value)];
    // Single writer, so a plain load + store is enough and avoids a locked RMW
    count.store(count.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
  }

  // Value at or below which p% of the values recorded since the last reset fall (upper bound of its bucket), or 0 if empty
  int64_t percentile(double p) const {
    if (p < 0.0 || p > 100.0) {
      throw std::invalid_argument("percentile must be 0..100");
    }

    std::lock_guard lk(baseline_mutex_);
    const std::vector<uint64_t> snapshot = snapshot_locked();
    uint64_t total = 0;
    for (const uint64_t c : snapshot) total += c;
    if (total == 0) return 0;

    // The epsilon stops floating point error pushing an exact rank up by one (0.999 * 1000 = 999.0000000000001)
    const double exact_rank = p / 100.0 * static_cast<double>(total);
    const auto rank = std::clamp<uint64_t>(static_cast<uint64_t>(std::ceil(exact_rank - 1e-9 * exact_rank)), 1, total);
    uint64_t cumulative = 0;
    for (size_t i = 0; i < snapshot.size(); ++i) {
      cumulative += snapshot[i];
      if (cumulative >= rank) {
        return highest_equivalent_value(i);
      }
    }
    return MAX_VALUE;
  }

  uint64_t total_count() const {
    std::lock_guard lk(baseline_mutex_);
    uint64_t total = 0;
    for (const uint64_t c : snapshot_locked()) total += c;
    return total;
  }

  // Forget everything recorded so far. Implemented as a baseline subtraction so the writer never needs to synchronise.
  void reset() {
    std::lock_guard lk(baseline_mutex_);
    for (size_t i = 0; i < bucket_count_; ++i) {
      baseline_[i] = counts_[i].load(std::memory_order_relaxed);
    }
  }

  // Exposed for testing precision
  int64_t highest_equivalent_value(size_t index) const noexcept {
    if (index < 2 * half_count_) return static_cast<int64_t>(index);
    const uint64_t shift = (index >> (sub_bucket_bits_ - 1)) - 1;
    const uint64_t sub_bucket = index - shift * half_count_;
    return static_cast<int64_t>(((sub_bucket + 1) << shift) - 1);
  }

  size_t index_for(int64_t value) const noexcept {
    const uint64_t v = static_cast<uint64_t>(std::clamp<int64_t>(value, 0, MAX_VALUE));
    const int bits = static_cast<int>(std::bit_width(v));
    if (bits <= sub_bucket_bits_) return static_cast<size_t>(v);
    const int shift = bits - sub_bucket_bits_;
    return static_cast<size_t>(uint64_t(shift) * half_count_ + (v >> shift));
  }

private:
  int sub_bucket_bits_;
  uint64_t half_count_;
  size_t bucket_count_;
  std::unique_ptr<std::atomic_uint64_t[]> counts_;

  mutable std::mutex baseline_mutex_;
  std::vector<uint64_t> baseline_;

  std::vector<uint64_t> snapshot_locked() const {
    std::vector<uint64_t> snapshot(bucket_count_);
    for (size_t i = 0; i < bucket_count_; ++i) {
      snapshot[i] = counts_[i].load(std::memory_order_relaxed) - baseline_[i];
    }
    return snapshot;
  }
};
}
