#ifndef MINIKAFKA_BROKER_SERVICE_H_
#define MINIKAFKA_BROKER_SERVICE_H_

#include <cstdint>

#include "minikafka/broker/partition_manager.h"
#include "minikafka/protocol/messages.h"

namespace minikafka {
namespace broker {

// 单响应 Fetch/Replay 的硬上限（05 §3.5）：4 MiB 或 500 条。
constexpr std::uint32_t kMaxFetchRecords = 500u;
constexpr std::uint32_t kDefaultFetchBytes = 1024u * 1024u;

// 读写路径编排（M5 职责）：校验 → 磁盘预检 → 追加/读取 → 组逐条结果。
// 所有方法都作用于某个确定分区，必须在"该分区所属 Storage Worker"上执行，
// 因此本类自身不加锁；跨分区并发由 worker 划分保证（AD-05/AD-13）。
class PartitionService {
 public:
  explicit PartitionService(PartitionManager* manager) : manager_(manager) {}

  // R14/R16/R17/R23 + AD-14：逐条独立结果，同分区首个失败后续 not_attempted。
  protocol::ProduceResponse Produce(const protocol::ProduceRequest& request);

  // 05 §3.5：立即返回，不做长轮询；offset == log_end_offset 返回空批。
  protocol::FetchResponse Fetch(const protocol::FetchRequest& request);

  // R12 / 05 §3.8：只读重放，与 Fetch 同构，不触碰任何 group 状态。
  protocol::FetchResponse Replay(const protocol::ReplayRequest& request);

 private:
  // 从 offset 起顺序读，受 max_bytes / max_count 约束；至少返回一条能容纳的记录。
  common::Status ReadRecords(storage::PartitionStore* store,
                             std::uint64_t offset, std::uint32_t max_bytes,
                             std::uint32_t max_count,
                             protocol::FetchResponse* response) const;

  PartitionManager* manager_;
};

}  // namespace broker
}  // namespace minikafka

#endif  // MINIKAFKA_BROKER_SERVICE_H_
