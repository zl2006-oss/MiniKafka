#ifndef MINIKAFKA_GROUP_ASSIGNMENT_H_
#define MINIKAFKA_GROUP_ASSIGNMENT_H_

#include <cstdint>
#include <vector>

#include "minikafka/common/status.h"

namespace minikafka {
namespace group {

// 05 §3.6：consumer_count 上限 16，与分区上限一致。
constexpr std::uint32_t kMaxConsumerCount = 16u;

// Q-P2=A 静态槽位校验：0 <= consumer_index < consumer_count，1 <= count <= 16。
common::Status ValidateConsumerSlot(std::uint32_t consumer_index,
                                    std::uint32_t consumer_count);

// Q-D2 的"可替换分配策略边界"：只抽策略签名，不引入心跳/会话/成员状态。
// P1 动态 rebalance 只需另实现一个策略，不改协调器与协议。
class AssignmentStrategy {
 public:
  virtual ~AssignmentStrategy() = default;

  // 成功时 *partitions 为升序分配结果；非法槽位返回 kInvalidConsumerSlot。
  virtual common::Status Assign(std::uint32_t consumer_index,
                                std::uint32_t consumer_count,
                                std::uint32_t partition_count,
                                std::vector<std::uint32_t>* partitions) const = 0;
};

// MVP 策略：partition % consumer_count == consumer_index。
// 纯函数、无成员状态，保证互不重叠且完全覆盖。
class StaticAssignmentStrategy final : public AssignmentStrategy {
 public:
  common::Status Assign(std::uint32_t consumer_index,
                        std::uint32_t consumer_count,
                        std::uint32_t partition_count,
                        std::vector<std::uint32_t>* partitions) const override;
};

// 便捷入口：等价于 StaticAssignmentStrategy().Assign(...)。
common::Status ComputeAssignment(std::uint32_t consumer_index,
                                 std::uint32_t consumer_count,
                                 std::uint32_t partition_count,
                                 std::vector<std::uint32_t>* partitions);

}  // namespace group
}  // namespace minikafka

#endif  // MINIKAFKA_GROUP_ASSIGNMENT_H_
