#include <utility>

#include "redline/bus.hpp"

namespace redline {
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

Result<> Claim::commit(const size_t size) noexcept {
  if (!owner_) {
    return std::unexpected(Error::INVALID_ARGUMENT);
  }
  if (size > data_.size()) {
    return std::unexpected(Error::MESSAGE_TOO_LARGE);
  }
  std::exchange(owner_, nullptr)->commit_claim(sequence_, uint32_t(size), std::exchange(context_, nullptr));
  data_ = {};
  return {};
}

void Claim::abort() noexcept {
  if (owner_) {
    std::exchange(owner_, nullptr)->abort_claim(sequence_, std::exchange(context_, nullptr));
    data_ = {};
  }
}
}
