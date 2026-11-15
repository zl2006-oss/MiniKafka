#include "test_common.h"
#include "test_storage_util.h"

#include <sys/stat.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "minikafka/common/status.h"
#include "minikafka/storage/record.h"
#include "minikafka/storage/recovery.h"
#include "minikafka/storage/segment.h"
#include "minikafka/storage/storage_worker.h"

namespace {

using minikafka::common::Status;
using minikafka::common::StatusCode;
using minikafka::common::StatusOr;
using minikafka::storage::kFlagKeyPresent;
using minikafka::storage::PartitionState;
using minikafka::storage::PartitionStore;
using minikafka::storage::Record;
using minikafka::storage::Segment;
using minikafka::storage::StorageWorker;
using minikafka::storage::StorageWorkerPool;

const std::uint64_t kTestSegmentBytes = 64u * 1024u;
const std::uint64_t kTestDiskThreshold = 1u;

Record MakeRecord(const std::string& key, const std::string& value) {
  Record record;
  record.flags = kFlagKeyPresent;
  record.key = key;
  record.value = value;
  return record;
}

StatusOr<PartitionStore> OpenStore(const std::string& dir,
                                   std::uint64_t segment_bytes,
                                   std::uint64_t disk_threshold) {
  return PartitionStore::Open(dir, segment_bytes, disk_threshold);
}

void TestPartitionStoreAppendAndRead() {
  mk_test::storage_test::TempDir root;
  MK_EXPECT_TRUE(root.ok());
  if (!root.ok()) {
    return;
  }
  const std::string dir = root.path() + "/p0";

  StatusOr<PartitionStore> opened =
      OpenStore(dir, kTestSegmentBytes, kTestDiskThreshold);
  MK_EXPECT_TRUE(opened.ok());
  if (!opened.ok()) {
    return;
  }
  PartitionStore store = std::move(opened).value();
  MK_EXPECT_TRUE(store.ready());
  MK_EXPECT_TRUE(store.state() == PartitionState::kReady);
  MK_EXPECT_EQ(store.next_offset(), 0u);
  MK_EXPECT_EQ(store.segment_count(), 1u);
  MK_EXPECT_FALSE(store.low_space());
  MK_EXPECT_EQ(store.truncated_bytes(), 0u);

  std::uint64_t offset = 0u;
  MK_EXPECT_TRUE(
      store.Append(MakeRecord("k0", "v0"), &offset).ok());
  MK_EXPECT_EQ(offset, 0u);
  MK_EXPECT_TRUE(
      store.Append(MakeRecord("k1", "v1"), &offset).ok());
  MK_EXPECT_EQ(offset, 1u);
  MK_EXPECT_EQ(store.next_offset(), 2u);

  StatusOr<Record> first = store.ReadAt(0u);
  MK_EXPECT_TRUE(first.ok());
  if (first.ok()) {
    MK_EXPECT_EQ(first.value().key, std::string("k0"));
    MK_EXPECT_EQ(first.value().value, std::string("v0"));
  }
  StatusOr<Record> second = store.ReadAt(1u);
  MK_EXPECT_TRUE(second.ok());
  if (second.ok()) {
    MK_EXPECT_EQ(second.value().value, std::string("v1"));
  }
  MK_EXPECT_FALSE(store.ReadAt(2u).ok());
  MK_EXPECT_TRUE(store.RefreshDiskSpace().ok());
  MK_EXPECT_FALSE(store.low_space());
}

// 0004 §5.2：重启后 next_offset 必须从扫描结果继续，不能回退到 0。
void TestPartitionStoreReopenKeepsOffset() {
  mk_test::storage_test::TempDir root;
  MK_EXPECT_TRUE(root.ok());
  if (!root.ok()) {
    return;
  }
  const std::string dir = root.path() + "/p0";

  {
    StatusOr<PartitionStore> opened =
        OpenStore(dir, kTestSegmentBytes, kTestDiskThreshold);
    MK_EXPECT_TRUE(opened.ok());
    if (!opened.ok()) {
      return;
    }
    PartitionStore store = std::move(opened).value();
    for (int i = 0; i < 3; ++i) {
      std::uint64_t offset = 0u;
      MK_EXPECT_TRUE(
          store.Append(MakeRecord("key", "value"), &offset).ok());
      MK_EXPECT_EQ(offset, static_cast<std::uint64_t>(i));
    }
    MK_EXPECT_EQ(store.next_offset(), 3u);
  }

  StatusOr<PartitionStore> reopened =
      OpenStore(dir, kTestSegmentBytes, kTestDiskThreshold);
  MK_EXPECT_TRUE(reopened.ok());
  if (!reopened.ok()) {
    return;
  }
  PartitionStore store = std::move(reopened).value();
  MK_EXPECT_TRUE(store.ready());
  MK_EXPECT_EQ(store.next_offset(), 3u);

  std::uint64_t offset = 0u;
  MK_EXPECT_TRUE(store.Append(MakeRecord("key", "value"), &offset).ok());
  MK_EXPECT_EQ(offset, 3u);
  MK_EXPECT_EQ(store.next_offset(), 4u);
}

// 500 KB 级分段：写满即滚动，跨段读取仍然正确。
void TestPartitionStoreRollsAcrossSegments() {
  mk_test::storage_test::TempDir root;
  MK_EXPECT_TRUE(root.ok());
  if (!root.ok()) {
    return;
  }
  const std::string dir = root.path() + "/p0";
  const std::uint64_t segment_bytes = 512u;

  StatusOr<PartitionStore> opened =
      OpenStore(dir, segment_bytes, kTestDiskThreshold);
  MK_EXPECT_TRUE(opened.ok());
  if (!opened.ok()) {
    return;
  }
  PartitionStore store = std::move(opened).value();

  const int kRecordCount = 40;
  for (int i = 0; i < kRecordCount; ++i) {
    const std::string value = "v" + std::to_string(i);
    std::uint64_t offset = 0u;
    const Status status = store.Append(MakeRecord("k", value), &offset);
    MK_EXPECT_TRUE(status.ok());
    if (!status.ok()) {
      return;
    }
    MK_EXPECT_EQ(offset, static_cast<std::uint64_t>(i));
  }
  MK_EXPECT_EQ(store.next_offset(),
               static_cast<std::uint64_t>(kRecordCount));
  MK_EXPECT_TRUE(store.segment_count() >= 2u);

  for (int i = 0; i < kRecordCount; ++i) {
    StatusOr<Record> record =
        store.ReadAt(static_cast<std::uint64_t>(i));
    MK_EXPECT_TRUE(record.ok());
    if (record.ok()) {
      MK_EXPECT_EQ(record.value().value, "v" + std::to_string(i));
    }
  }

  // 重开后段数与 offset 必须与写入时一致。
  const std::size_t before = store.segment_count();
  const std::uint64_t offset_before = store.next_offset();
  StatusOr<PartitionStore> reopened =
      OpenStore(dir, segment_bytes, kTestDiskThreshold);
  MK_EXPECT_TRUE(reopened.ok());
  if (reopened.ok()) {
    MK_EXPECT_EQ(reopened.value().segment_count(), before);
    MK_EXPECT_EQ(reopened.value().next_offset(), offset_before);
    StatusOr<Record> tail =
        reopened.value().ReadAt(offset_before - 1u);
    MK_EXPECT_TRUE(tail.ok());
    if (tail.ok()) {
      MK_EXPECT_EQ(tail.value().value,
                   "v" + std::to_string(kRecordCount - 1));
    }
  }
}

// R16：可用空间低于阈值 → disk_full，不写任何字节，读仍然可用。
void TestPartitionStoreDiskFullRejectsWrite() {
  mk_test::storage_test::TempDir root;
  MK_EXPECT_TRUE(root.ok());
  if (!root.ok()) {
    return;
  }
  const std::string dir = root.path() + "/p0";

  StatusOr<PartitionStore> opened =
      OpenStore(dir, kTestSegmentBytes, ~static_cast<std::uint64_t>(0));
  MK_EXPECT_TRUE(opened.ok());
  if (!opened.ok()) {
    return;
  }
  PartitionStore store = std::move(opened).value();
  MK_EXPECT_TRUE(store.ready());
  MK_EXPECT_TRUE(store.low_space());

  std::uint64_t offset = 12345u;
  const Status status = store.Append(MakeRecord("k", "v"), &offset);
  MK_EXPECT_FALSE(status.ok());
  MK_EXPECT_EQ(status.code(), StatusCode::kDiskFull);
  MK_EXPECT_EQ(offset, 12345u);
  MK_EXPECT_EQ(store.next_offset(), 0u);

  // 写入被拒但读取路径不受影响（空分区 → 越界，而非 storage_unavailable）。
  const StatusOr<Record> read = store.ReadAt(0u);
  MK_EXPECT_FALSE(read.ok());
  MK_EXPECT_EQ(read.status().code(), StatusCode::kOffsetOutOfRange);
}

// 基址断链：分区保持不可用，Append 拒绝，且不影响其他分区。
void TestPartitionStoreUnavailable() {
  mk_test::storage_test::TempDir root;
  MK_EXPECT_TRUE(root.ok());
  if (!root.ok()) {
    return;
  }
  const std::string dir = root.path() + "/p0";
  MK_EXPECT_TRUE(::mkdir(dir.c_str(), 0755) == 0);

  StatusOr<Segment> created =
      Segment::Create(dir, 9u, kTestSegmentBytes);
  MK_EXPECT_TRUE(created.ok());
  if (!created.ok()) {
    return;
  }
  (void)created.value().Seal();

  StatusOr<PartitionStore> opened =
      OpenStore(dir, kTestSegmentBytes, kTestDiskThreshold);
  MK_EXPECT_TRUE(opened.ok());
  if (!opened.ok()) {
    return;
  }
  PartitionStore store = std::move(opened).value();
  MK_EXPECT_FALSE(store.ready());
  MK_EXPECT_TRUE(store.state() == PartitionState::kUnavailable);
  MK_EXPECT_FALSE(store.warning().empty());
  MK_EXPECT_EQ(store.segment_count(), 0u);

  std::uint64_t offset = 0u;
  const Status status = store.Append(MakeRecord("k", "v"), &offset);
  MK_EXPECT_FALSE(status.ok());
  MK_EXPECT_EQ(status.code(), StatusCode::kStorageUnavailable);

  const StatusOr<Record> read = store.ReadAt(0u);
  MK_EXPECT_FALSE(read.ok());
  MK_EXPECT_EQ(read.status().code(), StatusCode::kStorageUnavailable);
}

// AD-05：同一分区恒定映射到同一 worker，与分区顺序、重复调用无关。
void TestWorkerAffinity() {
  StorageWorkerPool pool(4u);
  MK_EXPECT_EQ(pool.worker_count(), 4u);
  for (std::uint32_t partition = 0; partition < 16u; ++partition) {
    const std::size_t first = pool.WorkerIndexFor(partition);
    MK_EXPECT_EQ(first, static_cast<std::size_t>(partition % 4u));
    MK_EXPECT_EQ(pool.WorkerIndexFor(partition), first);
    MK_EXPECT_TRUE(first < pool.worker_count());
  }

  // worker_count=0 视为 1，避免除零；所有分区落在 0 号。
  StorageWorkerPool single(0u);
  MK_EXPECT_EQ(single.worker_count(), 1u);
  MK_EXPECT_EQ(single.WorkerIndexFor(0u), 0u);
  MK_EXPECT_EQ(single.WorkerIndexFor(7u), 0u);
}

void TestWorkerAddAndFind() {
  mk_test::storage_test::TempDir root;
  MK_EXPECT_TRUE(root.ok());
  if (!root.ok()) {
    return;
  }

  StorageWorker worker(0u);
  StatusOr<PartitionStore> first =
      OpenStore(root.path() + "/p3", kTestSegmentBytes, kTestDiskThreshold);
  MK_EXPECT_TRUE(first.ok());
  if (!first.ok()) {
    return;
  }
  worker.AddPartition(
      3u, std::unique_ptr<PartitionStore>(
              new PartitionStore(std::move(first).value())));
  MK_EXPECT_EQ(worker.partition_count(), 1u);
  MK_EXPECT_EQ(worker.partitions().size(), 1u);
  MK_EXPECT_EQ(worker.partitions()[0], 3u);
  MK_EXPECT_TRUE(worker.Find(3u) != nullptr);
  MK_EXPECT_TRUE(worker.Find(4u) == nullptr);
  MK_EXPECT_EQ(worker.index(), 0u);

  // 重复 AddPartition 只替换，不产生重复条目。
  StatusOr<PartitionStore> again =
      OpenStore(root.path() + "/p3", kTestSegmentBytes, kTestDiskThreshold);
  MK_EXPECT_TRUE(again.ok());
  if (again.ok()) {
    worker.AddPartition(
        3u, std::unique_ptr<PartitionStore>(
                new PartitionStore(std::move(again).value())));
  }
  MK_EXPECT_EQ(worker.partition_count(), 1u);
  MK_EXPECT_EQ(worker.partitions().size(), 1u);
}

}  // namespace

void TestStorageWorker() {
  TestPartitionStoreAppendAndRead();
  TestPartitionStoreReopenKeepsOffset();
  TestPartitionStoreRollsAcrossSegments();
  TestPartitionStoreDiskFullRejectsWrite();
  TestPartitionStoreUnavailable();
  TestWorkerAffinity();
  TestWorkerAddAndFind();
}
