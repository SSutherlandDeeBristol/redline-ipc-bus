#pragma once

#include <cmath>
#include <cstdint>
#include <stdexcept>
#include <vector>

/*
 * histogram for int64_t latency values (nanoseconds)
 * Single-writer lock-free, header-only, configurable number of buckets and significant figures
 */
class Histogram {
public:
    // Constructor
    // lowest_value: smallest expected latency in ns
    // highest_value: largest expected latency in ns
    // num_buckets: number of buckets to divide range into
    // significant_figures: accuracy within each bucket (1–5)
    explicit Histogram(
        int64_t lowest_value = 1,
        int64_t highest_value = 1'000'000'000LL,
        int num_buckets = 2048,
        int significant_figures = 3)
        : lowest_value_(lowest_value),
          highest_value_(highest_value),
          num_buckets_(num_buckets),
          significant_figures_(significant_figures),
          counts_(num_buckets, 0),
          total_count_(0)
    {
        if (lowest_value <= 0 || highest_value <= 0)
            throw std::invalid_argument("lowest_value and highest_value must be positive");
        if (lowest_value >= highest_value)
            throw std::invalid_argument("lowest_value must be < highest_value");
        if (num_buckets <= 0)
            throw std::invalid_argument("num_buckets must be > 0");
        if (significant_figures < 1 || significant_figures > 5)
            throw std::invalid_argument("significant_figures must be 1..5");

        log_range_ = std::log(static_cast<double>(highest_value_) / lowest_value_);
    }

    // Record a new latency value (ns)
    void record_value(int64_t value) noexcept {
        if (value < 0) {
            value = 0; // handle negative values
        }
        int index = counts_index(value);

        if (index < 0) {
            index = 0;
        } else if (index >= num_buckets_) {
            index = num_buckets_ - 1;
        }

        counts_[index]++;
        total_count_++;
    }

    // Reset histogram counts
    void reset() noexcept {
        std::fill(counts_.begin(), counts_.end(), 0);
        total_count_ = 0;
    }

    // Compute approximate percentile
    int64_t percentile(double p) const {
        if (total_count_ == 0) return 0;
        if (p < 0.0 || p > 100.0)
            throw std::invalid_argument("percentile must be 0..100");

        auto target = static_cast<int64_t>((p / 100.0) * total_count_);
        int64_t cumulative = 0;

        for (size_t i = 0; i < counts_.size(); ++i) {
            cumulative += counts_[i];
            if (cumulative >= target) {
                return value_from_index(i);
            }
        }
        return highest_value_;
    }

    uint64_t total_count() const { return total_count_; }

private:
    int64_t lowest_value_;
    int64_t highest_value_;
    int num_buckets_;
    int significant_figures_;
    std::vector<int64_t> counts_;
    uint64_t total_count_;
    double log_range_; // precompute log(highest/lowest) for bucket mapping

    // Map a value to bucket index (logarithmic)
    int counts_index(int64_t value) const noexcept {
        if (value <= lowest_value_) return 0;
        if (value >= highest_value_) return num_buckets_ - 1;

        double fraction = std::log(static_cast<double>(value) / lowest_value_) / log_range_;
        return static_cast<int>(fraction * num_buckets_);
    }

    // Map bucket index back to approximate latency value
    int64_t value_from_index(int index) const noexcept {
        double fraction = static_cast<double>(index) / num_buckets_;
        double value = lowest_value_ * std::exp(fraction * log_range_);
        if (value > static_cast<double>(highest_value_)) value = highest_value_;
        return static_cast<int64_t>(value);
    }
};