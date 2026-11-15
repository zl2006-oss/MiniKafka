#ifndef MINIKAFKA_COMMON_CLOCK_H_
#define MINIKAFKA_COMMON_CLOCK_H_

#include <chrono>
#include <cstdint>

namespace minikafka {
namespace common {

// 单调时钟：只用于超时/退避测量，不表示墙上时间。
class Clock {
 public:
  using TimePoint = std::chrono::steady_clock::time_point;

  static TimePoint Now();
  static std::uint64_t NowMillis();
  static std::uint64_t ElapsedMillis(TimePoint start);
  static std::uint64_t ElapsedMillis(TimePoint start, TimePoint end);
};

}  // namespace common
}  // namespace minikafka

#endif  // MINIKAFKA_COMMON_CLOCK_H_
