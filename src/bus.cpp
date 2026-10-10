#include "redline/bus.hpp"

#include <cerrno>
#include <cstring>
#include <limits>
#include <list>
#include <mutex>
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "log.hpp"
#include "producer.hpp"
#include "subscription.hpp"
#include "wake.hpp"
#include "shm/segment.hpp"
#include "shm/subscriber_registry.hpp"

namespace redline {
namespace {
constexpr size_t POLL_BATCH = 256; // messages a poll thread delivers between checks for shutdown

std::unexpected<Error> invalid(const std::string& reason) {
  REDLINE_LOG(ERROR) << reason;
  return std::unexpected(Error::INVALID_ARGUMENT);
}

Result<std::string> segment_name(const std::string& name) {
  if (name.empty() || name.find('/') != std::string::npos || name.size() > 200) {
    return invalid("Bus name must be 1-200 characters and must not contain '/', got '" + name + "'");
  }
  return "/redline." + name;
}

void consume_safely(const Callback& callback, const Message& message, const size_t index) noexcept {
  try {
    callback(message);
  } catch (const std::exception& e) {
    REDLINE_LOG(ERROR) << "Subscriber callback " << index << " threw exception: " << e.what();
  } catch (...) {
    REDLINE_LOG(ERROR) << "Subscriber callback " << index << " threw a non-std::exception";
  }
}

void pin_current_thread(const int cpu, const std::string& bus_name) noexcept {
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(static_cast<size_t>(cpu), &set);
  if (const int error = pthread_setaffinity_np(pthread_self(), sizeof(set), &set); error != 0) {
    REDLINE_LOG(WARNING) << "Failed to pin a poll thread on " << bus_name << " to CPU " << cpu << ": " << std::strerror(error);
  }
}

bool valid_cpu(const int cpu) noexcept {
  return cpu >= 0 && cpu < CPU_SETSIZE;
}
}

class BusImpl final : public Bus {
public:
  BusImpl(std::shared_ptr<shm::SegmentMapping> mapping, const BusOptions& options)
      : mapping_(std::move(mapping)),
        segment_(mapping_->segment()),
        options_(options),
        producer_(mapping_, options.subscriber_timeout_us),
        subscriber_registry_(mapping_) {}

  Result<> publish(const std::span<const uint8_t> data, const int64_t timeout_us) override {
    return producer_.publish_batch(std::span<const std::span<const uint8_t>>(&data, 1), timeout_us);
  }

  Result<> publish_batch(const std::span<const std::span<const uint8_t>> messages, const int64_t timeout_us) override {
    return producer_.publish_batch(messages, timeout_us);
  }

  Result<Claim> claim(const int64_t timeout_us) override {
    return producer_.claim(timeout_us);
  }

  Result<> add_subscriber(Callback callback, const SubscriberOptions& options) override {
    if (!callback) {
      return invalid("add_subscriber was given an empty callback");
    }

    std::lock_guard lk(subscriber_creation_mutex_);

    if (auto ok = check(options, options.own_thread ? Use::OWN_THREAD : Use::SHARED_THREAD); !ok) {
      return ok;
    }

    if (options.own_thread) {
      auto subscription = make_subscription(options);
      if (!subscription) {
        return std::unexpected(subscription.error());
      }
      auto& own = own_thread_subscribers_.emplace_back();
      own.subscription = std::move(*subscription);
      auto* sub = own.subscription.get();
      if (auto started = start_thread(own.thread, options.cpu, [this, sub, callback = std::move(callback)] {
            run(*sub, [&](const Message& message) { consume_safely(callback, message, 0); });
          }); !started) {
        own_thread_subscribers_.pop_back();
        return started;
      }
      return {};
    }

    // The first callback registers this Bus as a subscriber; nothing is recorded unless that succeeds
    const bool first = !shared_subscription_;
    if (first) {
      auto subscription = make_subscription(options);
      if (!subscription) {
        return std::unexpected(subscription.error());
      }
      shared_subscription_ = std::move(*subscription);
    }

    // Copy on write, so the poll thread can keep using the list it has while we publish a new one
    auto previous = callbacks_;
    auto callbacks = std::make_shared<Callbacks>(callbacks_ ? *callbacks_ : Callbacks{});
    callbacks->push_back(std::make_shared<const Callback>(std::move(callback)));
    callbacks_ = std::move(callbacks);
    callbacks_version_.fetch_add(1, std::memory_order_release);

    if (first) {
      // The thread can't read callbacks_ until we release the mutex, by which time it's set
      if (auto started = start_thread(poll_thread_, options_.poll_thread_cpu, [this] { run_shared(); }); !started) {
        callbacks_ = std::move(previous);
        shared_subscription_.reset();
        return started;
      }
      shared_subscription_ptr_.store(shared_subscription_.get(), std::memory_order_release);
    }
    return {};
  }

  Result<std::unique_ptr<Subscription>> subscribe(const SubscriberOptions& options) override {
    if (auto ok = check(options, Use::SUBSCRIBE); !ok) {
      return std::unexpected(ok.error());
    }
    auto subscription = make_subscription(options);
    if (!subscription) {
      return std::unexpected(subscription.error());
    }
    return std::unique_ptr<Subscription>(std::move(*subscription));
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

  const BusLayout& layout() const noexcept override {
    return mapping_->layout();
  }

  ~BusImpl() override {
    kill_.store(true);
    // Wake our poll threads if they are asleep (other sleeping subscribers just go back to sleep). The sleep timeout
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
  enum class Use { SHARED_THREAD, OWN_THREAD, SUBSCRIBE };

  struct OwnThreadSubscriber {
    std::unique_ptr<SubscriptionImpl> subscription;
    std::thread thread;
  };

  std::shared_ptr<shm::SegmentMapping> mapping_;
  shm::SHMSegment* segment_;
  BusOptions options_;

  Producer producer_;
  shm::SubscriberRegistry subscriber_registry_;

  // Shared by the callbacks on poll_thread_. Created with the first callback and kept until the Bus is destroyed.
  std::unique_ptr<SubscriptionImpl> shared_subscription_;
  std::atomic<SubscriptionImpl*> shared_subscription_ptr_ = nullptr; // for reading metrics without the mutex

  using Callbacks = std::vector<std::shared_ptr<const Callback>>;
  std::mutex subscriber_creation_mutex_;
  // Callbacks run by poll_thread_, replaced wholesale whenever one is added. callbacks_version_ changes with it, so the
  // poll thread only takes the mutex to pick up a new list, not for every message.
  std::shared_ptr<const Callbacks> callbacks_; // guarded by subscriber_creation_mutex_
  std::atomic_uint64_t callbacks_version_ = 0;

  std::list<OwnThreadSubscriber> own_thread_subscribers_; // guarded by subscriber_creation_mutex_

  std::atomic_bool kill_{false};
  std::thread poll_thread_;

  // Shared-thread callbacks always use the BusOptions; the overrides are for subscribers with a position of their own
  Result<> check(const SubscriberOptions& options, const Use use) const {
    if (use == Use::SHARED_THREAD && (options.backpressure_mode || options.on_lapped || options.zero_copy_reads ||
                                      options.latency_metrics || options.cpu)) {
      return invalid("Callbacks sharing the Bus' poll thread use its BusOptions; set own_thread to override them");
    }
    if (use == Use::SUBSCRIBE && (options.own_thread || options.cpu)) {
      return invalid("own_thread and cpu are for add_subscriber, not subscribe");
    }
    const BackPressureMode mode = options.backpressure_mode.value_or(options_.backpressure_mode);
    if (mode != BackPressureMode::RELIABLE && mode != BackPressureMode::BEST_EFFORT) {
      return invalid("backpressure_mode is not a valid BackPressureMode");
    }
    const OnLapped on_lapped = options.on_lapped.value_or(options_.on_lapped);
    if (on_lapped != OnLapped::RESUME_AT_OLDEST && on_lapped != OnLapped::RESUME_AT_NEWEST) {
      return invalid("on_lapped is not a valid OnLapped");
    }
    if (options.zero_copy_reads.value_or(options_.zero_copy_reads) && mode != BackPressureMode::RELIABLE) {
      return invalid("zero_copy_reads requires RELIABLE mode, since BEST_EFFORT publishers may overwrite a message mid-read");
    }
    if (options.cpu && !valid_cpu(*options.cpu)) {
      return invalid("cpu is not a valid CPU number");
    }
    return {};
  }

  // Options must already have been checked
  Result<std::unique_ptr<SubscriptionImpl>> make_subscription(const SubscriberOptions& options) {
    const SubscriptionSettings settings{
      .zero_copy = options.zero_copy_reads.value_or(options_.zero_copy_reads),
      .latency_metrics = options.latency_metrics.value_or(options_.latency_metrics),
      .on_lapped = options.on_lapped.value_or(options_.on_lapped),
      .spin_duration_ns = options_.consumer_busy_spin_duration_us * 1000,
      .histogram_significant_figures = options_.latency_histogram_significant_figures,
    };
    const bool reliable = options.backpressure_mode.value_or(options_.backpressure_mode) == BackPressureMode::RELIABLE;
    const uint32_t flags = (reliable ? shm::SUBSCRIBER_RELIABLE : 0) | (settings.zero_copy ? shm::SUBSCRIBER_NO_EVICT : 0);

    auto lease = subscriber_registry_.register_subscriber(flags);
    if (!lease) {
      return std::unexpected(lease.error());
    }
    return std::make_unique<SubscriptionImpl>(mapping_, settings, std::move(*lease));
  }

  template <typename Body>
  Result<> start_thread(std::thread& thread, const std::optional<int> cpu, Body&& body) {
    try {
      thread = std::thread([this, cpu, body = std::forward<Body>(body)]() mutable {
        if (cpu) {
          pin_current_thread(*cpu, mapping_->name());
        }
        body();
      });
    } catch (const std::system_error& e) {
      REDLINE_LOG(ERROR) << "Failed to start a poll thread on " << mapping_->name() << ": " << e.what();
      return std::unexpected(Error::SYSTEM_ERROR);
    }
    return {};
  }

  template <typename Deliver>
  void run(SubscriptionImpl& subscription, Deliver&& deliver) {
    while (!kill_.load(std::memory_order_relaxed)) {
      if (subscription.poll_with(deliver, POLL_BATCH) == 0) {
        subscription.wait_until(std::numeric_limits<int64_t>::max(), &kill_);
      }
    }
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

Result<std::unique_ptr<Bus>> create_bus(const std::string& name, const BusLayout& layout, const BusOptions& options) {
  const auto path = segment_name(name);
  if (!path) {
    return std::unexpected(path.error());
  }
  auto mapping = shm::SegmentMapping::open_or_create(*path, layout, options);
  if (!mapping) {
    return std::unexpected(mapping.error());
  }
  return std::make_unique<BusImpl>(std::move(*mapping), options);
}

Result<std::unique_ptr<Bus>> open_bus(const std::string& name, const BusOptions& options) {
  const auto path = segment_name(name);
  if (!path) {
    return std::unexpected(path.error());
  }
  auto mapping = shm::SegmentMapping::open(*path, options);
  if (!mapping) {
    return std::unexpected(mapping.error());
  }
  return std::make_unique<BusImpl>(std::move(*mapping), options);
}

Result<> destroy_bus(const std::string& name) {
  const auto path = segment_name(name);
  if (!path) {
    return std::unexpected(path.error());
  }
  if (shm_unlink(path->c_str()) == 0) {
    return {};
  }
  if (errno == ENOENT) {
    return std::unexpected(Error::NOT_FOUND);
  }
  REDLINE_LOG(ERROR) << "Failed to remove shared memory segment at " << *path << ": " << std::strerror(errno);
  return std::unexpected(Error::SYSTEM_ERROR);
}

std::string_view to_string(const Error error) noexcept {
  switch (error) {
    case Error::BACKPRESSURE_TIMEOUT: return "BACKPRESSURE_TIMEOUT";
    case Error::MESSAGE_TOO_LARGE: return "MESSAGE_TOO_LARGE";
    case Error::SUBSCRIBER_REGISTRY_FULL: return "SUBSCRIBER_REGISTRY_FULL";
    case Error::PUBLISHER_REGISTRY_FULL: return "PUBLISHER_REGISTRY_FULL";
    case Error::INVALID_ARGUMENT: return "INVALID_ARGUMENT";
    case Error::NOT_FOUND: return "NOT_FOUND";
    case Error::INCOMPATIBLE: return "INCOMPATIBLE";
    case Error::SYSTEM_ERROR: return "SYSTEM_ERROR";
  }
  return "UNKNOWN";
}
}
