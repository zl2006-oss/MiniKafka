#ifndef MINIKAFKA_BENCH_STATS_H_
#define MINIKAFKA_BENCH_STATS_H_

#include <cstddef>
#include <cstdio>
#include <string>
#include <vector>

namespace minikafka {
namespace bench {

// 固定的三位小数输出，避免压测结果因 iostream 本地化而漂移。
inline std::string Fixed(double value) {
  char buffer[32];
  std::snprintf(buffer, sizeof(buffer), "%.3f", value);
  return std::string(buffer);
}

// 输入必须已升序排序。ratio ∈ [0,1]；空样本返回 0。
inline double Percentile(const std::vector<double>& sorted, double ratio) {
  if (sorted.empty()) {
    return 0.0;
  }
  const double index = ratio * static_cast<double>(sorted.size() - 1u);
  const std::size_t rounded = static_cast<std::size_t>(index + 0.5);
  return sorted[rounded < sorted.size() ? rounded : sorted.size() - 1u];
}

}  // namespace bench
}  // namespace minikafka

#endif  // MINIKAFKA_BENCH_STATS_H_
