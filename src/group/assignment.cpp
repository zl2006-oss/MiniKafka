#include "minikafka/group/assignment.h"

#include "minikafka/common/defaults.h"

namespace minikafka {
namespace group {

using common::Status;
using common::StatusCode;

common::Status ValidateConsumerSlot(std::uint32_t consumer_index,
                                    std::uint32_t consumer_count) {
  if (consumer_count < 1u || consumer_count > kMaxConsumerCount) {
    return Status(StatusCode::kInvalidConsumerSlot,
                  "consumer_count must be in 1..16");
  }
  if (consumer_index >= consumer_count) {
    return Status(StatusCode::kInvalidConsumerSlot,
                  "consumer_index must be less than consumer_count");
  }
  return Status();
}

common::Status StaticAssignmentStrategy::Assign(
    std::uint32_t consumer_index, std::uint32_t consumer_count,
    std::uint32_t partition_count, std::vector<std::uint32_t>* partitions) const {
  if (partitions == nullptr) {
    return Status(StatusCode::kInternalError, "null assignment output");
  }
  const Status slot_status =
      ValidateConsumerSlot(consumer_index, consumer_count);
  if (!slot_status.ok()) {
    return slot_status;
  }
  if (partition_count < static_cast<std::uint32_t>(common::kMinPartitions) ||
      partition_count >
          static_cast<std::uint32_t>(common::kMaxPartitions)) {
    return Status(StatusCode::kInvalidPartitions,
                  "partition_count must be in 1..16");
  }

  partitions->clear();
  // 升序枚举保证 05 §3.6 的"按 partition 升序"与 entry_count == assigned_count。
  for (std::uint32_t partition = 0; partition < partition_count; ++partition) {
    if (partition % consumer_count == consumer_index) {
      partitions->push_back(partition);
    }
  }
  return Status();
}

common::Status ComputeAssignment(std::uint32_t consumer_index,
                                 std::uint32_t consumer_count,
                                 std::uint32_t partition_count,
                                 std::vector<std::uint32_t>* partitions) {
  const StaticAssignmentStrategy strategy;
  return strategy.Assign(consumer_index, consumer_count, partition_count,
                         partitions);
}

}  // namespace group
}  // namespace minikafka
