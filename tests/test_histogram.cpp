#include <thread>

#include <gtest/gtest.h>

#include "histogram.hpp"

using redline::Histogram;

TEST(Histogram, EmptyReturnsZero) {
  Histogram h;
  EXPECT_EQ(h.total_count(), 0u);
  EXPECT_EQ(h.percentile(50), 0);
  EXPECT_EQ(h.percentile(100), 0);
}

TEST(Histogram, RejectsInvalidArguments) {
  EXPECT_THROW(Histogram(0), std::invalid_argument);
  EXPECT_THROW(Histogram(6), std::invalid_argument);
  Histogram h;
  EXPECT_THROW(h.percentile(-1), std::invalid_argument);
  EXPECT_THROW(h.percentile(100.1), std::invalid_argument);
}

TEST(Histogram, SmallValuesAreExact) {
  Histogram h;
  for (int v = 1; v <= 1000; ++v) h.record(v);
  EXPECT_EQ(h.total_count(), 1000u);
  EXPECT_EQ(h.percentile(0), 1);
  EXPECT_EQ(h.percentile(0.1), 1);
  EXPECT_EQ(h.percentile(50), 500);
  EXPECT_EQ(h.percentile(99.9), 999);
  EXPECT_EQ(h.percentile(100), 1000);
}

TEST(Histogram, BucketsAreContiguousAndMonotonic) {
  Histogram h(3);
  size_t previous = h.index_for(0);
  for (int64_t v = 1; v < 10'000'000; v += (v < 100'000 ? 1 : 997)) {
    const size_t index = h.index_for(v);
    ASSERT_GE(index, previous) << v;
    previous = index;
  }
  // Every bucket's upper bound maps back into that bucket, and the next value starts the next bucket
  for (size_t i = 0; i < h.index_for(Histogram::MAX_VALUE); ++i) {
    const int64_t upper = h.highest_equivalent_value(i);
    ASSERT_EQ(h.index_for(upper), i);
    ASSERT_EQ(h.index_for(upper + 1), i + 1);
  }
}

class HistogramPrecision : public ::testing::TestWithParam<int> {};

TEST_P(HistogramPrecision, RelativeErrorWithinSignificantFigures) {
  const int significant_figures = GetParam();
  Histogram h(significant_figures);
  const double max_error = std::pow(10.0, -significant_figures);
  for (double v = 1; v < 1e12; v *= 1.37) {
    const auto value = static_cast<int64_t>(v);
    const int64_t reported = h.highest_equivalent_value(h.index_for(value));
    ASSERT_GE(reported, value);
    ASSERT_LE(double(reported - value), double(value) * max_error) << "value " << value;
  }
}

INSTANTIATE_TEST_SUITE_P(AllPrecisions, HistogramPrecision, ::testing::Values(1, 2, 3, 4));

TEST(Histogram, PercentilesOfUniformDistribution) {
  Histogram h(3);
  for (int64_t v = 1; v <= 1'000'000; ++v) h.record(v * 10);
  EXPECT_NEAR(double(h.percentile(50)), 5'000'000, 5'000'000 * 1e-3);
  EXPECT_NEAR(double(h.percentile(90)), 9'000'000, 9'000'000 * 1e-3);
  EXPECT_NEAR(double(h.percentile(99)), 9'900'000, 9'900'000 * 1e-3);
  EXPECT_NEAR(double(h.percentile(99.9)), 9'990'000, 9'990'000 * 1e-3);
}

TEST(Histogram, ClampsOutOfRangeValues) {
  Histogram h;
  h.record(-5);
  EXPECT_EQ(h.percentile(100), 0);
  h.record(int64_t(1) << 60);
  EXPECT_EQ(h.percentile(100), Histogram::MAX_VALUE);
}

TEST(Histogram, ResetForgetsEarlierValues) {
  Histogram h;
  for (int i = 0; i < 100; ++i) h.record(1'000'000);
  h.reset();
  EXPECT_EQ(h.total_count(), 0u);
  EXPECT_EQ(h.percentile(50), 0);
  h.record(10);
  EXPECT_EQ(h.total_count(), 1u);
  EXPECT_EQ(h.percentile(100), 10);
}

// One writer and concurrent readers/resetters; mostly here so ThreadSanitizer can check it
TEST(Histogram, ConcurrentReadsWhileRecording) {
  Histogram h;
  std::atomic_bool stop{false};
  std::thread writer([&] {
    int64_t v = 0;
    while (!stop) h.record(++v % 100'000);
  });
  for (int i = 0; i < 200; ++i) {
    const int64_t p50 = h.percentile(50);
    const int64_t p99 = h.percentile(99);
    EXPECT_LE(p50, p99);
    if (i % 50 == 0) h.reset();
  }
  stop = true;
  writer.join();
}
