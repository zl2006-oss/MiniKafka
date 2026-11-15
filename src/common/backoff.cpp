#include "minikafka/common/backoff.h"

namespace minikafka {
namespace common {

Backoff::Backoff() : Backoff(100u, 1000u) {}

Backoff::Backoff(std::uint64_t initial_ms, std::uint64_t max_ms)
    : initial_ms_(initial_ms),
      max_ms_(max_ms < initial_ms ? initial_ms : max_ms),
      next_ms_(initial_ms),
      attempts_(0u) {}

std::uint64_t Backoff::NextDelayMs() {
  const std::uint64_t delay = next_ms_;
  ++attempts_;

  if (next_ms_ >= max_ms_ || next_ms_ > max_ms_ / 2u) {
    next_ms_ = max_ms_;
  } else {
    next_ms_ *= 2u;
  }
  return delay;
}

void Backoff::Reset() {
  next_ms_ = initial_ms_;
  attempts_ = 0u;
}

}  // namespace common
}  // namespace minikafka
