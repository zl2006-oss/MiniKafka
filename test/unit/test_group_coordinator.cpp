#include "test_common.h"
#include "test_storage_util.h"

#include <cstdio>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "minikafka/common/status.h"
#include "minikafka/group/coordinator.h"
#include "minikafka/storage/offset_file.h"

namespace {

using minikafka::common::Status;
using minikafka::common::StatusCode;
using minikafka::common::StatusOr;
using minikafka::group::CommittedOffset;
using minikafka::group::FetchOutcome;
using minikafka::group::GroupCoordinator;
using minikafka::storage::OffsetEntry;

std::string SlotPath(const std::string& data_dir, const std::string& group,
                     const std::string& topic, const char* file) {
  return data_dir + "/groups/" + group + "/" + topic + "/" + file;
}

bool OverwriteWithGarbage(const std::string& path) {
  std::FILE* file = std::fopen(path.c_str(), "wb");
  if (file == nullptr) {
    return false;
  }
  const char garbage[8] = {0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x00};
  const std::size_t written = std::fwrite(garbage, 1u, sizeof(garbage), file);
  std::fclose(file);
  return written == sizeof(garbage);
}

OffsetEntry MakeUpdate(std::uint32_t partition, std::uint64_t offset) {
  OffsetEntry entry;
  entry.partition = partition;
  entry.committed_offset = offset;
  return entry;
}

// 05 §3.6：新 group 拿到全量分配，且 has_committed=false。
void TestFetchNewGroup() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  std::vector<std::string> warnings;
  GroupCoordinator coordinator(
      dir.path(), [&warnings](const std::string& detail) {
        warnings.push_back(detail);
      });

  StatusOr<FetchOutcome> outcome =
      coordinator.Fetch("g1", "orders", 0u, 1u, 3u);
  MK_EXPECT_TRUE(outcome.ok());
  if (!outcome.ok()) {
    return;
  }
  MK_EXPECT_EQ(outcome.value().assigned_partitions.size(), 3u);
  MK_EXPECT_EQ(outcome.value().entries.size(), 3u);
  for (std::size_t i = 0; i < outcome.value().entries.size(); ++i) {
    MK_EXPECT_EQ(outcome.value().entries[i].partition,
                 static_cast<std::uint32_t>(i));
    MK_EXPECT_FALSE(outcome.value().entries[i].has_committed);
    MK_EXPECT_EQ(outcome.value().entries[i].committed_offset, 0u);
  }
  MK_EXPECT_TRUE(warnings.empty());
  MK_EXPECT_TRUE(coordinator.knows("g1", "orders"));
  MK_EXPECT_EQ(coordinator.tracked_topics(), 1u);

  // 3 个消费者分 7 个分区：0 号槽位拿 0,3,6。
  StatusOr<FetchOutcome> second =
      coordinator.Fetch("g1", "orders", 0u, 3u, 7u);
  MK_EXPECT_TRUE(second.ok());
  if (second.ok()) {
    MK_EXPECT_EQ(second.value().assigned_partitions.size(), 3u);
    if (second.value().assigned_partitions.size() == 3u) {
      MK_EXPECT_EQ(second.value().assigned_partitions[0], 0u);
      MK_EXPECT_EQ(second.value().assigned_partitions[2], 6u);
    }
  }
}

// 05 §3.7：提交后能读回；同分区取 max、不回退。
void TestCommitAndFetch() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  GroupCoordinator coordinator(dir.path());

  std::vector<OffsetEntry> updates;
  updates.push_back(MakeUpdate(0u, 5u));
  updates.push_back(MakeUpdate(2u, 7u));
  StatusOr<std::uint32_t> applied =
      coordinator.Commit("g1", "orders", updates, 3u);
  MK_EXPECT_TRUE(applied.ok());
  if (applied.ok()) {
    MK_EXPECT_EQ(applied.value(), 2u);
  }

  StatusOr<FetchOutcome> outcome =
      coordinator.Fetch("g1", "orders", 0u, 1u, 3u);
  MK_EXPECT_TRUE(outcome.ok());
  if (!outcome.ok()) {
    return;
  }
  MK_EXPECT_EQ(outcome.value().entries.size(), 3u);
  if (outcome.value().entries.size() == 3u) {
    MK_EXPECT_TRUE(outcome.value().entries[0].has_committed);
    MK_EXPECT_EQ(outcome.value().entries[0].committed_offset, 5u);
    MK_EXPECT_FALSE(outcome.value().entries[1].has_committed);
    MK_EXPECT_TRUE(outcome.value().entries[2].has_committed);
    MK_EXPECT_EQ(outcome.value().entries[2].committed_offset, 7u);
  }

  // 回退提交：新值小于现有值时保留现有值。
  std::vector<OffsetEntry> stale;
  stale.push_back(MakeUpdate(0u, 3u));
  MK_EXPECT_TRUE(coordinator.Commit("g1", "orders", stale, 3u).ok());
  StatusOr<FetchOutcome> after =
      coordinator.Fetch("g1", "orders", 0u, 1u, 3u);
  MK_EXPECT_TRUE(after.ok());
  if (after.ok() && after.value().entries.size() == 3u) {
    MK_EXPECT_EQ(after.value().entries[0].committed_offset, 5u);
  }

  // 空提交：不落盘、不报错。
  std::vector<OffsetEntry> none;
  StatusOr<std::uint32_t> zero =
      coordinator.Commit("g2", "orders", none, 3u);
  MK_EXPECT_TRUE(zero.ok());
  if (zero.ok()) {
    MK_EXPECT_EQ(zero.value(), 0u);
  }
  MK_EXPECT_FALSE(coordinator.knows("g2", "orders"));
}

// R28：位点跨重启保留（双槽文件）。
void TestPersistenceAcrossRestart() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  {
    GroupCoordinator coordinator(dir.path());
    std::vector<OffsetEntry> updates;
    updates.push_back(MakeUpdate(1u, 42u));
    MK_EXPECT_TRUE(coordinator.Commit("g1", "orders", updates, 4u).ok());
  }

  GroupCoordinator restarted(dir.path());
  StatusOr<FetchOutcome> outcome =
      restarted.Fetch("g1", "orders", 1u, 2u, 4u);
  MK_EXPECT_TRUE(outcome.ok());
  if (!outcome.ok()) {
    return;
  }
  MK_EXPECT_EQ(outcome.value().assigned_partitions.size(), 2u);
  if (outcome.value().assigned_partitions.size() == 2u) {
    MK_EXPECT_EQ(outcome.value().assigned_partitions[0], 1u);
    MK_EXPECT_EQ(outcome.value().assigned_partitions[1], 3u);
    MK_EXPECT_TRUE(outcome.value().entries[0].has_committed);
    MK_EXPECT_EQ(outcome.value().entries[0].committed_offset, 42u);
    MK_EXPECT_FALSE(outcome.value().entries[1].has_committed);
  }
}

// 双槽都损坏 → 降级为"无已提交位点"并告警，不报错退出（R28）。
void TestDegradedOffsets() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  {
    GroupCoordinator coordinator(dir.path());
    std::vector<OffsetEntry> updates;
    updates.push_back(MakeUpdate(0u, 9u));
    MK_EXPECT_TRUE(coordinator.Commit("g1", "orders", updates, 2u).ok());
  }

  MK_EXPECT_TRUE(
      OverwriteWithGarbage(SlotPath(dir.path(), "g1", "orders", "offsets.a")));
  MK_EXPECT_TRUE(
      OverwriteWithGarbage(SlotPath(dir.path(), "g1", "orders", "offsets.b")));

  std::vector<std::string> warnings;
  GroupCoordinator coordinator(
      dir.path(), [&warnings](const std::string& detail) {
        warnings.push_back(detail);
      });
  StatusOr<FetchOutcome> outcome =
      coordinator.Fetch("g1", "orders", 0u, 1u, 2u);
  MK_EXPECT_TRUE(outcome.ok());
  if (!outcome.ok()) {
    return;
  }
  MK_EXPECT_EQ(outcome.value().entries.size(), 2u);
  for (std::size_t i = 0; i < outcome.value().entries.size(); ++i) {
    MK_EXPECT_FALSE(outcome.value().entries[i].has_committed);
  }
  MK_EXPECT_FALSE(warnings.empty());
}

// S-13：任一分区越界 → 整体不应用；非法名称各有专用错误码。
void TestValidation() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  GroupCoordinator coordinator(dir.path());

  std::vector<OffsetEntry> mixed;
  mixed.push_back(MakeUpdate(0u, 5u));
  mixed.push_back(MakeUpdate(9u, 6u));
  StatusOr<std::uint32_t> rejected =
      coordinator.Commit("g1", "orders", mixed, 3u);
  MK_EXPECT_FALSE(rejected.ok());
  MK_EXPECT_EQ(rejected.status().code(), StatusCode::kInvalidPartition);

  // 整体不应用：0 号分区不应留下 5。
  StatusOr<FetchOutcome> outcome =
      coordinator.Fetch("g1", "orders", 0u, 1u, 3u);
  MK_EXPECT_TRUE(outcome.ok());
  if (outcome.ok() && outcome.value().entries.size() == 3u) {
    MK_EXPECT_FALSE(outcome.value().entries[0].has_committed);
  }

  StatusOr<FetchOutcome> bad_group =
      coordinator.Fetch("Bad Group", "orders", 0u, 1u, 3u);
  MK_EXPECT_FALSE(bad_group.ok());
  MK_EXPECT_EQ(bad_group.status().code(), StatusCode::kInvalidGroupName);

  StatusOr<FetchOutcome> bad_topic =
      coordinator.Fetch("g1", "Bad/Topic", 0u, 1u, 3u);
  MK_EXPECT_FALSE(bad_topic.ok());
  MK_EXPECT_EQ(bad_topic.status().code(), StatusCode::kInvalidTopicName);

  StatusOr<FetchOutcome> bad_slot =
      coordinator.Fetch("g1", "orders", 2u, 2u, 3u);
  MK_EXPECT_FALSE(bad_slot.ok());
  MK_EXPECT_EQ(bad_slot.status().code(), StatusCode::kInvalidConsumerSlot);

  std::vector<OffsetEntry> single;
  single.push_back(MakeUpdate(0u, 1u));
  StatusOr<std::uint32_t> zero_participants =
      coordinator.Commit("g1", "orders", single, 0u);
  MK_EXPECT_FALSE(zero_participants.ok());
  MK_EXPECT_EQ(zero_participants.status().code(),
               StatusCode::kInvalidPartitions);
}

}  // namespace

void TestGroupCoordinator() {
  TestFetchNewGroup();
  TestCommitAndFetch();
  TestPersistenceAcrossRestart();
  TestDegradedOffsets();
  TestValidation();
}
