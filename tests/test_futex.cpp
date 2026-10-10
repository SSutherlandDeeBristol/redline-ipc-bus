#include <atomic>
#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "shm/futex.hpp"

using namespace redline::shm;
using namespace std::chrono_literals;

TEST(Futex, WaitReturnsWouldBlockWhenValueDiffers) {
  std::atomic_uint32_t word{1};
  EXPECT_EQ(futex_wait(&word, 0, 1s), FutexResult::WOULD_BLOCK);
}

TEST(Futex, WaitTimesOut) {
  std::atomic_uint32_t word{0};
  const auto start = std::chrono::steady_clock::now();
  EXPECT_EQ(futex_wait(&word, 0, 20ms), FutexResult::TIMEOUT);
  EXPECT_GE(std::chrono::steady_clock::now() - start, 15ms);
}

TEST(Futex, WaitHandlesTimeoutsOverOneSecond) {
  // Regression guard for the timespec conversion: 1.5s must not be passed as an invalid tv_nsec
  std::atomic_uint32_t word{1};
  EXPECT_EQ(futex_wait(&word, 0, 1500ms), FutexResult::WOULD_BLOCK);
}

TEST(Futex, WakeReleasesWaiter) {
  std::atomic_uint32_t word{0};
  std::atomic_bool woke{false};
  std::thread waiter([&] {
    while (word.load() == 0) {
      futex_wait(&word, 0, 5s);
    }
    woke = true;
  });
  std::this_thread::sleep_for(20ms);
  word.store(1);
  EXPECT_EQ(futex_wake(&word, 1), FutexResult::OK);
  waiter.join();
  EXPECT_TRUE(woke);
}
