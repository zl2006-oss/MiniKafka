#include "test_common.h"
#include "test_storage_util.h"

#include <sys/stat.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <string>
#include <utility>

#include "minikafka/common/status.h"
#include "minikafka/storage/paths.h"
#include "minikafka/storage/record.h"
#include "minikafka/storage/recovery.h"
#include "minikafka/storage/segment.h"

namespace {

using minikafka::common::Status;
using minikafka::common::StatusCode;
using minikafka::common::StatusOr;
using minikafka::storage::AvailableBytes;
using minikafka::storage::CheckDiskSpace;
using minikafka::storage::kFlagKeyPresent;
using minikafka::storage::PartitionState;
using minikafka::storage::Record;
using minikafka::storage::RecoverPartition;
using minikafka::storage::RecoveredPartition;
using minikafka::storage::Segment;

const std::uint64_t kTestSegmentBytes = 64u * 1024u;

std::string SegmentPath(const std::string& dir, std::uint64_t base_offset) {
  return dir + "/" + minikafka::storage::FormatBaseOffset(base_offset) +
         ".log";
}

std::uint64_t FileSize(const std::string& path) {
  struct stat st;
  if (::stat(path.c_str(), &st) != 0 || st.st_size < 0) {
    return 0u;
  }
  return static_cast<std::uint64_t>(st.st_size);
}

bool AppendRawBytes(const std::string& path, const std::string& bytes) {
  std::FILE* file = std::fopen(path.c_str(), "ab");
  if (file == nullptr) {
    return false;
  }
  const std::size_t written = std::fwrite(bytes.data(), 1u, bytes.size(), file);
  const int closed = std::fclose(file);
  return written == bytes.size() && closed == 0;
}

Record MakeRecord(const std::string& value) {
  Record record;
  record.flags = kFlagKeyPresent;
  record.key = "k";
  record.value = value;
  return record;
}

// 04 §5.2 第 3 步：空目录 → next_offset=0，并落地 base=0 的空 active 段。
void TestRecoverEmptyDirectory() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }

  StatusOr<RecoveredPartition> recovered =
      RecoverPartition(dir.path(), kTestSegmentBytes);
  MK_EXPECT_TRUE(recovered.ok());
  if (!recovered.ok()) {
    return;
  }
  MK_EXPECT_TRUE(recovered.value().state == PartitionState::kReady);
  MK_EXPECT_EQ(recovered.value().next_offset, 0u);
  MK_EXPECT_EQ(recovered.value().segment_count, 1u);
  MK_EXPECT_EQ(recovered.value().truncated_bytes, 0u);
  MK_EXPECT_TRUE(FileSize(SegmentPath(dir.path(), 0u)) == 0u);
}

// R7：active 段尾部残缺只截断，正常记录必须保留。
void TestRecoverTruncatesActiveTail() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }

  StatusOr<Segment> created =
      Segment::Create(dir.path(), 0u, kTestSegmentBytes);
  MK_EXPECT_TRUE(created.ok());
  if (!created.ok()) {
    return;
  }
  Segment segment = std::move(created).value();
  MK_EXPECT_TRUE(segment.Append(MakeRecord("v0")).ok());
  MK_EXPECT_TRUE(segment.Append(MakeRecord("v1")).ok());
  const std::uint64_t valid_size = segment.size();
  MK_EXPECT_TRUE(valid_size > 0u);
  (void)segment.Seal();

  const std::string log_path = SegmentPath(dir.path(), 0u);
  MK_EXPECT_TRUE(AppendRawBytes(log_path, std::string(7, '\x7f')));
  MK_EXPECT_EQ(FileSize(log_path), valid_size + 7u);

  StatusOr<RecoveredPartition> recovered =
      RecoverPartition(dir.path(), kTestSegmentBytes);
  MK_EXPECT_TRUE(recovered.ok());
  if (!recovered.ok()) {
    return;
  }
  MK_EXPECT_TRUE(recovered.value().state == PartitionState::kReady);
  MK_EXPECT_EQ(recovered.value().next_offset, 2u);
  MK_EXPECT_EQ(recovered.value().truncated_bytes, 7u);
  // 截断后文件长度回到最后一条校验通过的记录末尾。
  MK_EXPECT_EQ(FileSize(log_path), valid_size);

  // 截断后的分段仍可正常打开并读回两条记录。
  StatusOr<Segment> reopened =
      Segment::Open(dir.path(), 0u, 2u, kTestSegmentBytes);
  MK_EXPECT_TRUE(reopened.ok());
  if (!reopened.ok()) {
    return;
  }
  StatusOr<Record> first = reopened.value().ReadAt(0u);
  MK_EXPECT_TRUE(first.ok());
  if (first.ok()) {
    MK_EXPECT_EQ(first.value().value, std::string("v0"));
  }
  StatusOr<Record> second = reopened.value().ReadAt(1u);
  MK_EXPECT_TRUE(second.ok());
  if (second.ok()) {
    MK_EXPECT_EQ(second.value().value, std::string("v1"));
  }
}

// S-11：首段 base 必须为 0，否则整分区不可用且不改文件。
void TestRecoverFirstBaseNotZero() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }

  StatusOr<Segment> created =
      Segment::Create(dir.path(), 5u, kTestSegmentBytes);
  MK_EXPECT_TRUE(created.ok());
  if (!created.ok()) {
    return;
  }
  const std::uint64_t before = FileSize(SegmentPath(dir.path(), 5u));

  StatusOr<RecoveredPartition> recovered =
      RecoverPartition(dir.path(), kTestSegmentBytes);
  MK_EXPECT_TRUE(recovered.ok());
  if (!recovered.ok()) {
    return;
  }
  MK_EXPECT_TRUE(recovered.value().state == PartitionState::kUnavailable);
  MK_EXPECT_EQ(recovered.value().segment_count, 1u);
  MK_EXPECT_FALSE(recovered.value().warning.empty());
  MK_EXPECT_TRUE(recovered.value().warning.find("base chain broken") !=
                 std::string::npos);
  MK_EXPECT_EQ(FileSize(SegmentPath(dir.path(), 5u)), before);
}

// 相邻段必须前段 next_offset 衔接：0 段只有 1 条记录，下一段 base 却是 3。
void TestRecoverBaseChainBroken() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }

  StatusOr<Segment> first =
      Segment::Create(dir.path(), 0u, kTestSegmentBytes);
  MK_EXPECT_TRUE(first.ok());
  if (!first.ok()) {
    return;
  }
  MK_EXPECT_TRUE(first.value().Append(MakeRecord("v0")).ok());
  (void)first.value().Seal();

  StatusOr<Segment> second =
      Segment::Create(dir.path(), 3u, kTestSegmentBytes);
  MK_EXPECT_TRUE(second.ok());

  StatusOr<RecoveredPartition> recovered =
      RecoverPartition(dir.path(), kTestSegmentBytes);
  MK_EXPECT_TRUE(recovered.ok());
  if (!recovered.ok()) {
    return;
  }
  MK_EXPECT_TRUE(recovered.value().state == PartitionState::kUnavailable);
  MK_EXPECT_EQ(recovered.value().segment_count, 2u);
  MK_EXPECT_TRUE(recovered.value().warning.find("base chain broken") !=
                 std::string::npos);
}

// 封存段只读校验：尾部残缺判定损坏，且绝不截断该文件。
void TestRecoverSealedCorruptIsNotModified() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }

  StatusOr<Segment> sealed_segment =
      Segment::Create(dir.path(), 0u, kTestSegmentBytes);
  MK_EXPECT_TRUE(sealed_segment.ok());
  if (!sealed_segment.ok()) {
    return;
  }
  MK_EXPECT_TRUE(sealed_segment.value().Append(MakeRecord("v0")).ok());
  (void)sealed_segment.value().Seal();

  StatusOr<Segment> active_segment =
      Segment::Create(dir.path(), 1u, kTestSegmentBytes);
  MK_EXPECT_TRUE(active_segment.ok());
  if (!active_segment.ok()) {
    return;
  }
  MK_EXPECT_TRUE(active_segment.value().Append(MakeRecord("v1")).ok());
  (void)active_segment.value().Seal();

  const std::string log_path = SegmentPath(dir.path(), 0u);
  const std::uint64_t intact_size = FileSize(log_path);
  MK_EXPECT_TRUE(AppendRawBytes(log_path, std::string(9, '\x2a')));
  const std::uint64_t corrupt_size = FileSize(log_path);
  MK_EXPECT_EQ(corrupt_size, intact_size + 9u);

  StatusOr<RecoveredPartition> recovered =
      RecoverPartition(dir.path(), kTestSegmentBytes);
  MK_EXPECT_TRUE(recovered.ok());
  if (!recovered.ok()) {
    return;
  }
  MK_EXPECT_TRUE(recovered.value().state == PartitionState::kUnavailable);
  MK_EXPECT_TRUE(recovered.value().warning.find("sealed segment is corrupt") !=
                 std::string::npos);
  MK_EXPECT_EQ(recovered.value().truncated_bytes, 0u);
  MK_EXPECT_EQ(FileSize(log_path), corrupt_size);
}

// 只有 0 字节 active 段时，next_offset 保持 0（不能凭空前进）。
void TestRecoverEmptyActiveSegment() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }

  StatusOr<Segment> created =
      Segment::Create(dir.path(), 0u, kTestSegmentBytes);
  MK_EXPECT_TRUE(created.ok());
  if (!created.ok()) {
    return;
  }
  MK_EXPECT_EQ(created.value().size(), 0u);
  (void)created.value().Seal();

  StatusOr<RecoveredPartition> recovered =
      RecoverPartition(dir.path(), kTestSegmentBytes);
  MK_EXPECT_TRUE(recovered.ok());
  if (!recovered.ok()) {
    return;
  }
  MK_EXPECT_TRUE(recovered.value().state == PartitionState::kReady);
  MK_EXPECT_EQ(recovered.value().next_offset, 0u);
  MK_EXPECT_EQ(recovered.value().segment_count, 1u);
  MK_EXPECT_EQ(recovered.value().truncated_bytes, 0u);
}

// R16 / Q-P11：可用空间查询与写前预检。
void TestDiskSpaceChecks() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }

  StatusOr<std::uint64_t> available = AvailableBytes(dir.path());
  MK_EXPECT_TRUE(available.ok());
  if (available.ok()) {
    MK_EXPECT_TRUE(available.value() > 0u);
  }

  const Status small = CheckDiskSpace(dir.path(), 1u);
  MK_EXPECT_TRUE(small.ok());

  const Status huge = CheckDiskSpace(dir.path(), ~static_cast<std::uint64_t>(0));
  MK_EXPECT_FALSE(huge.ok());
  MK_EXPECT_EQ(huge.code(), StatusCode::kDiskFull);

  const Status bad_path = CheckDiskSpace("/definitely/not/here", 1u);
  MK_EXPECT_FALSE(bad_path.ok());
}

}  // namespace

void TestStorageRecovery() {
  TestRecoverEmptyDirectory();
  TestRecoverTruncatesActiveTail();
  TestRecoverFirstBaseNotZero();
  TestRecoverBaseChainBroken();
  TestRecoverSealedCorruptIsNotModified();
  TestRecoverEmptyActiveSegment();
  TestDiskSpaceChecks();
}
