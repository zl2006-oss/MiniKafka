#ifndef MINIKAFKA_COMMON_BACKOFF_H_
#define MINIKAFKA_COMMON_BACKOFF_H_

#include <cstdint>

namespace minikafka {
namespace common {

// 指数退避（Q-P3）：默认 100ms 起、×2、上限 1000ms，无抖动。
class Backoff {
 public:
  Backoff();
  Backoff(std::uint64_t initial_ms, std::uint64_t max_ms);

  // 返回本次应等待的时长，然后推进到下一次（首次返回 initial_ms）。
  std::uint64_t NextDelayMs();

  void Reset();

  std::uint32_t attempts() const { return attempts_; }

 private:
  std::uint64_t initial_ms_;
  std::uint64_t max_ms_;
  std::uint64_t next_ms_;
  std::uint32_t attempts_;
};

}  // namespace common
}  // namespace minikafka

#endif  // MINIKAFKA_COMMON_BACKOFF_H_
