#ifndef MINIKAFKA_COMMON_DEFAULTS_H_
#define MINIKAFKA_COMMON_DEFAULTS_H_

namespace minikafka {
namespace common {

// 03 §3.4 B4 已确认的固定默认值；M7/M5 直接引用，避免各处重复字面量。
constexpr int kDefaultBrokerPort = 9092;
constexpr char kDefaultDataDir[] = "./data";
constexpr int kDefaultMinFreeSpaceMb = 256;
constexpr long long kDefaultSegmentBytes = 268435456LL;
constexpr long long kMinSegmentBytes = 2097152LL;
constexpr char kDefaultLogLevel[] = "warn";

constexpr int kDefaultRetries = 3;
constexpr int kDefaultRetryBackoffMs = 100;
constexpr int kDefaultRetryTimeoutMs = 5000;
constexpr char kDefaultFrom[] = "earliest";

constexpr int kDefaultAutoCommitIntervalMs = 5000;
constexpr int kDefaultReplayPartition = 0;
constexpr long long kDefaultReplayOffset = 0LL;

constexpr char kDefaultBrokerAddress[] = "127.0.0.1:9092";
constexpr char kDefaultBenchMode[] = "throughput";
constexpr int kDefaultProducers = 4;
constexpr int kDefaultPayloadBytes = 100;
constexpr int kDefaultKeyBytes = 8;

constexpr int kMinPartitions = 1;
constexpr int kMaxPartitions = 16;

}  // namespace common
}  // namespace minikafka

#endif  // MINIKAFKA_COMMON_DEFAULTS_H_
