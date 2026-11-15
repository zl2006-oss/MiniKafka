#ifndef MINIKAFKA_BENCH_BENCH_H_
#define MINIKAFKA_BENCH_BENCH_H_

#include <string>
#include <vector>

namespace minikafka {
namespace bench {

// M8 压测子命令（R5/R26/03 §3.4）：多 Producer 并发纯写，输出吞吐与 ack 延迟分位。
int RunBench(const std::vector<std::string>& tokens);
void PrintBenchUsage();

}  // namespace bench
}  // namespace minikafka

#endif  // MINIKAFKA_BENCH_BENCH_H_
