#include "minikafka/common/clock.h"

namespace minikafka {
namespace common {

Clock::TimePoint Clock::Now() {
  return std::chrono::steady_clock::now();
}

std::uint64_t Clock::NowMillis() {
  const std::chrono::milliseconds elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          Now().time_since_epoch());
  return static_cast<std::uint64_t>(elapsed.count());
}

std::uint64_t Clock::ElapsedMillis(TimePoint start) {
  return ElapsedMillis(start, Now());
}

std::uint64_t Clock::ElapsedMillis(TimePoint start, TimePoint end) {
  if (end < start) {
    return 0;
  }
  const std::chrono::milliseconds elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  return static_cast<std::uint64_t>(elapsed.count());
}

}  // namespace common
}  // namespace minikafka
