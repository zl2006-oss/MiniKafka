#include "test_common.h"

#include <cstdint>
#include <string>

#include "minikafka/common/status.h"
#include "minikafka/storage/paths.h"

namespace {

using minikafka::common::Status;
using minikafka::common::StatusCode;
using minikafka::storage::FormatBaseOffset;
using minikafka::storage::GroupTopicDirectory;
using minikafka::storage::IsValidGroupName;
using minikafka::storage::IsValidTopicName;
using minikafka::storage::MetaPath;
using minikafka::storage::PartitionDirectory;
using minikafka::storage::SegmentIndexPath;
using minikafka::storage::SegmentLogPath;
using minikafka::storage::TopicDirectory;
using minikafka::storage::ValidateGroupName;
using minikafka::storage::ValidatePartition;
using minikafka::storage::ValidateTopicName;

void ExpectStatus(const Status& status, StatusCode code) {
  MK_EXPECT_FALSE(status.ok());
  MK_EXPECT_EQ(status.code(), code);
}

}  // namespace

void TestStoragePath() {
  MK_EXPECT_TRUE(IsValidTopicName("orders"));
  MK_EXPECT_TRUE(IsValidTopicName("a"));
  MK_EXPECT_TRUE(IsValidTopicName("a-b_c9"));
  MK_EXPECT_TRUE(IsValidTopicName(std::string(64u, 'a')));
  MK_EXPECT_FALSE(IsValidTopicName(""));
  MK_EXPECT_FALSE(IsValidTopicName("."));
  MK_EXPECT_FALSE(IsValidTopicName(".."));
  MK_EXPECT_FALSE(IsValidTopicName("/abs"));
  MK_EXPECT_FALSE(IsValidTopicName("a/b"));
  MK_EXPECT_FALSE(IsValidTopicName("a\\b"));
  MK_EXPECT_FALSE(IsValidTopicName("Orders"));
  MK_EXPECT_FALSE(IsValidTopicName("a.b"));
  MK_EXPECT_FALSE(IsValidTopicName("a b"));
  MK_EXPECT_FALSE(IsValidTopicName(std::string(65u, 'a')));
  MK_EXPECT_FALSE(IsValidTopicName(std::string("\xC3\xA9", 2)));

  MK_EXPECT_TRUE(IsValidGroupName("g1"));
  MK_EXPECT_TRUE(IsValidGroupName("group-_9"));
  MK_EXPECT_FALSE(IsValidGroupName(""));
  MK_EXPECT_FALSE(IsValidGroupName(".."));
  MK_EXPECT_FALSE(IsValidGroupName("g/1"));

  ExpectStatus(ValidateTopicName("Bad"), StatusCode::kInvalidTopicName);
  ExpectStatus(ValidateGroupName("Bad"), StatusCode::kInvalidGroupName);

  MK_EXPECT_TRUE(ValidatePartition(0u, 1u).ok());
  MK_EXPECT_TRUE(ValidatePartition(15u, 16u).ok());
  ExpectStatus(ValidatePartition(0u, 0u), StatusCode::kInvalidPartitions);
  ExpectStatus(ValidatePartition(0u, 17u), StatusCode::kInvalidPartitions);
  ExpectStatus(ValidatePartition(4u, 4u), StatusCode::kInvalidPartition);
  ExpectStatus(ValidatePartition(16u, 16u), StatusCode::kInvalidPartition);

  MK_EXPECT_EQ(FormatBaseOffset(0u),
               std::string("00000000000000000000"));
  MK_EXPECT_EQ(FormatBaseOffset(12345u),
               std::string("00000000000000012345"));
  MK_EXPECT_EQ(FormatBaseOffset(18446744073709551615ULL),
               std::string("18446744073709551615"));

  std::string path;
  MK_EXPECT_TRUE(TopicDirectory("/data", "orders", &path).ok());
  MK_EXPECT_EQ(path, std::string("/data/topics/orders"));

  MK_EXPECT_TRUE(PartitionDirectory("/data", "orders", 3u, 4u, &path).ok());
  MK_EXPECT_EQ(path, std::string("/data/topics/orders/3"));

  MK_EXPECT_TRUE(MetaPath("/data", "orders", &path).ok());
  MK_EXPECT_EQ(path, std::string("/data/meta/orders.meta"));

  MK_EXPECT_TRUE(GroupTopicDirectory("/data", "g1", "orders", &path).ok());
  MK_EXPECT_EQ(path, std::string("/data/groups/g1/orders"));

  MK_EXPECT_TRUE(
      SegmentLogPath("/data", "orders", 0u, 4u, 0u, &path).ok());
  MK_EXPECT_EQ(
      path, std::string("/data/topics/orders/0/00000000000000000000.log"));

  MK_EXPECT_TRUE(
      SegmentLogPath("/data", "orders", 0u, 4u, 12345u, &path).ok());
  MK_EXPECT_EQ(
      path, std::string("/data/topics/orders/0/00000000000000012345.log"));

  MK_EXPECT_TRUE(
      SegmentIndexPath("/data", "orders", 0u, 4u, 0u, &path).ok());
  MK_EXPECT_EQ(path,
               std::string(
                   "/data/topics/orders/0/00000000000000000000.index"));

  MK_EXPECT_TRUE(TopicDirectory("/data/", "orders", &path).ok());
  MK_EXPECT_EQ(path, std::string("/data/topics/orders"));

  MK_EXPECT_TRUE(TopicDirectory("./data", "orders", &path).ok());
  MK_EXPECT_EQ(path, std::string("./data/topics/orders"));

  path = "sentinel";
  ExpectStatus(TopicDirectory("/data", "Bad", &path),
               StatusCode::kInvalidTopicName);
  MK_EXPECT_EQ(path, std::string("sentinel"));

  ExpectStatus(TopicDirectory("/data", "../etc", &path),
               StatusCode::kInvalidTopicName);
  ExpectStatus(PartitionDirectory("/data", "orders", 4u, 4u, &path),
               StatusCode::kInvalidPartition);
  ExpectStatus(PartitionDirectory("/data", "orders", 0u, 0u, &path),
               StatusCode::kInvalidPartitions);
  ExpectStatus(GroupTopicDirectory("/data", "Bad", "orders", &path),
               StatusCode::kInvalidGroupName);
  ExpectStatus(GroupTopicDirectory("/data", "g1", "Bad", &path),
               StatusCode::kInvalidTopicName);
  ExpectStatus(MetaPath("", "orders", &path),
               StatusCode::kInternalError);
  ExpectStatus(SegmentLogPath("", "orders", 0u, 1u, 0u, &path),
               StatusCode::kInternalError);
  ExpectStatus(TopicDirectory("/data", "orders", nullptr),
               StatusCode::kInternalError);
  ExpectStatus(SegmentIndexPath("/data", "orders", 0u, 1u, 0u, nullptr),
               StatusCode::kInternalError);
}
