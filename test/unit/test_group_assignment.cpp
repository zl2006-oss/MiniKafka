#include "test_common.h"

#include <cstddef>
#include <cstdint>
#include <vector>

#include "minikafka/common/status.h"
#include "minikafka/group/assignment.h"

namespace {

using minikafka::common::Status;
using minikafka::common::StatusCode;
using minikafka::group::ComputeAssignment;
using minikafka::group::ValidateConsumerSlot;

bool Equal(const std::vector<std::uint32_t>& actual,
           const std::vector<std::uint32_t>& expected) {
  if (actual.size() != expected.size()) {
    return false;
  }
  for (std::size_t i = 0; i < actual.size(); ++i) {
    if (actual[i] != expected[i]) {
      return false;
    }
  }
  return true;
}

// Q-P2=A：partition % consumer_count == consumer_index。
void TestBasicRule() {
  std::vector<std::uint32_t> partitions;
  MK_EXPECT_TRUE(ComputeAssignment(0u, 1u, 3u, &partitions).ok());
  MK_EXPECT_TRUE(Equal(partitions, {0u, 1u, 2u}));

  MK_EXPECT_TRUE(ComputeAssignment(0u, 3u, 7u, &partitions).ok());
  MK_EXPECT_TRUE(Equal(partitions, {0u, 3u, 6u}));

  MK_EXPECT_TRUE(ComputeAssignment(1u, 3u, 7u, &partitions).ok());
  MK_EXPECT_TRUE(Equal(partitions, {1u, 4u}));

  MK_EXPECT_TRUE(ComputeAssignment(2u, 3u, 7u, &partitions).ok());
  MK_EXPECT_TRUE(Equal(partitions, {2u, 5u}));

  // 槽位多于分区：多余槽位拿到空分配，不报错。
  MK_EXPECT_TRUE(ComputeAssignment(4u, 5u, 3u, &partitions).ok());
  MK_EXPECT_TRUE(partitions.empty());
}

// 互不重叠且完全覆盖：对所有合法的 (n, partition_count) 组合逐一验证。
void TestDisjointAndComplete() {
  for (std::uint32_t consumer_count = 1u; consumer_count <= 16u;
       ++consumer_count) {
    for (std::uint32_t partition_count = 1u; partition_count <= 16u;
         ++partition_count) {
      std::vector<std::uint32_t> owner(partition_count, 0xFFFFFFFFu);
      std::uint32_t total = 0u;
      for (std::uint32_t index = 0u; index < consumer_count; ++index) {
        std::vector<std::uint32_t> assigned;
        MK_EXPECT_TRUE(
            ComputeAssignment(index, consumer_count, partition_count,
                              &assigned)
                .ok());
        std::uint32_t previous = 0u;
        bool first = true;
        for (std::size_t i = 0; i < assigned.size(); ++i) {
          const std::uint32_t partition = assigned[i];
          MK_EXPECT_TRUE(partition < partition_count);
          MK_EXPECT_EQ(partition % consumer_count, index);
          // 同一次分配内严格升序
          MK_EXPECT_TRUE(first || partition > previous);
          previous = partition;
          first = false;
          // 不允许被两个槽位同时认领
          MK_EXPECT_EQ(owner[partition], 0xFFFFFFFFu);
          owner[partition] = index;
          ++total;
        }
      }
      // 完全覆盖
      MK_EXPECT_EQ(total, partition_count);
      for (std::uint32_t partition = 0u; partition < partition_count;
           ++partition) {
        MK_EXPECT_TRUE(owner[partition] != 0xFFFFFFFFu);
      }
    }
  }
}

void TestInvalidSlots() {
  std::vector<std::uint32_t> partitions;

  MK_EXPECT_EQ(ValidateConsumerSlot(1u, 1u).code(),
               StatusCode::kInvalidConsumerSlot);
  MK_EXPECT_EQ(ValidateConsumerSlot(0u, 0u).code(),
               StatusCode::kInvalidConsumerSlot);
  MK_EXPECT_EQ(ValidateConsumerSlot(0u, 17u).code(),
               StatusCode::kInvalidConsumerSlot);
  MK_EXPECT_TRUE(ValidateConsumerSlot(0u, 1u).ok());
  MK_EXPECT_TRUE(ValidateConsumerSlot(15u, 16u).ok());

  MK_EXPECT_EQ(ComputeAssignment(5u, 5u, 4u, &partitions).code(),
               StatusCode::kInvalidConsumerSlot);
  MK_EXPECT_EQ(ComputeAssignment(0u, 0u, 4u, &partitions).code(),
               StatusCode::kInvalidConsumerSlot);
  MK_EXPECT_EQ(ComputeAssignment(0u, 20u, 4u, &partitions).code(),
               StatusCode::kInvalidConsumerSlot);
  MK_EXPECT_EQ(ComputeAssignment(0u, 1u, 0u, &partitions).code(),
               StatusCode::kInvalidPartitions);
  MK_EXPECT_EQ(ComputeAssignment(0u, 1u, 17u, &partitions).code(),
               StatusCode::kInvalidPartitions);
  MK_EXPECT_EQ(ComputeAssignment(0u, 1u, 3u, nullptr).code(),
               StatusCode::kInternalError);
}

}  // namespace

void TestGroupAssignment() {
  TestBasicRule();
  TestDisjointAndComplete();
  TestInvalidSlots();
}
