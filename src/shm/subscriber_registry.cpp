#include "subscriber_registry.hpp"

#include <chrono>

#include "layout.hpp"

namespace redline::shm {
SubscriberRegistry::SubscriberRegistry(std::shared_ptr<SegmentMapping> segment)
: segment_(std::move(segment)), shm_segment_(segment_->segment()) {}

SubscribeResult SubscriberRegistry::register_subscriber(uint64_t &out_index) {
    out_index = 0;
    for (; out_index < shm_segment_->subscribers.size(); ++out_index) {
        auto expected = uint32_t(shm::SubscriberState::EMPTY);
        if (shm_segment_->subscribers[out_index].state.compare_exchange_strong(
                expected, static_cast<uint32_t>(SubscriberState::INITIALISING))) {
            // Claimed this spot
            break;
        }
    }

    if (out_index == shm_segment_->subscribers.size()) {
        return SubscribeResult::REGISTRY_FULL;
    }

    auto& subscriber = shm_segment_->subscribers[out_index];
    subscriber.pid = static_cast<uint64_t>(getpid());
    subscriber.read_seq = shm_segment_->write_seq.load();
    subscriber.last_read_timestamp = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    subscriber.state.store(uint32_t(SubscriberState::OK));

    return SubscribeResult::OK;
}

void SubscriberRegistry::remove_subscriber(uint64_t index) {
    shm_segment_->subscribers[index].state.store(uint32_t(SubscriberState::EMPTY));
}
} // namespace redline::shm