#include "test_common.h"
#include "test_storage_util.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "minikafka/common/status.h"
#include "minikafka/storage/record.h"
#include "minikafka/storage/segment.h"
#include "minikafka/storage/segment_set.h"

namespace {

using minikafka::common::Status;
using minikafka::common::StatusCode;
using minikafka::storage::kFlagKeyPresent;
using minikafka::storage::Record;
using minikafka::storage::Segment;
using minikafka::storage::SegmentSet;

const std::uint64_t kTestSegmentBytes = 64u * 1024u;

void ExpectStatus(const Status& status, StatusCode code) {
  MK_EXPECT_FALSE(status.ok());
  MK_EXPECT_EQ(status.code(), code);
}

Record MakeRecord(const std::string& value) {
  Record record;
  record.flags = kFlagKeyPresent;
  record.key = "k";
  record.value = value;
  return record;
}

void TestAppendReadAndSeal() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }

  minikafka::common::StatusOr<Segment> created =
      Segment::Create(dir.path(), 0u, kTestSegmentBytes);
  MK_EXPECT_TRUE(created.ok());
  if (!created.ok()) {
    return;
  }
  Segment segment = std::move(created).value();
  MK_EXPECT_EQ(segment.base_offset(), 0u);
  MK_EXPECT_EQ(segment.next_offset(), 0u);
  MK_EXPECT_TRUE(segment.path().find("00000000000000000000.log") !=
                 std::string::npos);
  MK_EXPECT_TRUE(segment.index_path().find("00000000000000000000.index") !=
                 std::string::npos);

  const Record first = MakeRecord("v");
  minikafka::common::StatusOr<std::uint64_t> offset = segment.Append(first);
  MK_EXPECT_TRUE(offset.ok());
  MK_EXPECT_EQ(offset.value(), 0u);
  MK_EXPECT_EQ(segment.size(), static_cast<std::uint64_t>(26));
  MK_EXPECT_EQ(segment.next_offset(), 1u);
  MK_EXPECT_EQ(segment.index_entry_count(), 1u);

  minikafka::common::StatusOr<Record> read = segment.ReadAt(0u);
  MK_EXPECT_TRUE(read.ok());
  MK_EXPECT_EQ(read.value().flags, kFlagKeyPresent);
  MK_EXPECT_EQ(read.value().key, std::string("k"));
  MK_EXPECT_EQ(read.value().value, std::string("v"));

  const Record second = MakeRecord("v2");
  offset = segment.Append(second);
  MK_EXPECT_TRUE(offset.ok());
  MK_EXPECT_EQ(offset.value(), 1u);
  read = segment.ReadAt(1u);
  MK_EXPECT_TRUE(read.ok());
  MK_EXPECT_EQ(read.value().value, std::string("v2"));
  read = segment.ReadAt(0u);
  MK_EXPECT_TRUE(read.ok());
  MK_EXPECT_EQ(read.value().value, std::string("v"));

  MK_EXPECT_TRUE(segment.Seal().ok());
  MK_EXPECT_TRUE(segment.sealed());
  MK_EXPECT_TRUE(segment.Seal().ok());
  minikafka::common::StatusOr<std::uint64_t> rejected =
      segment.Append(MakeRecord("after-seal"));
  MK_EXPECT_FALSE(rejected.ok());
  ExpectStatus(rejected.status(), StatusCode::kInternalError);
  MK_EXPECT_TRUE(segment.ReadAt(1u).ok());
}

void TestOversizeRecordRejected() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  minikafka::common::StatusOr<Segment> created =
      Segment::Create(dir.path(), 0u, kTestSegmentBytes);
  MK_EXPECT_TRUE(created.ok());
  if (!created.ok()) {
    return;
  }
  Segment segment = std::move(created).value();

  Record record;
  record.flags = kFlagKeyPresent;
  record.value.assign(70u * 1024u, 'x');
  minikafka::common::StatusOr<std::uint64_t> offset = segment.Append(record);
  MK_EXPECT_FALSE(offset.ok());
  ExpectStatus(offset.status(), StatusCode::kMessageTooLarge);
  MK_EXPECT_EQ(segment.size(), 0u);
  MK_EXPECT_EQ(segment.next_offset(), 0u);
}

void TestRollKeepsOldSegmentReadable() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  minikafka::common::StatusOr<Segment> created =
      Segment::Create(dir.path(), 0u, kTestSegmentBytes);
  MK_EXPECT_TRUE(created.ok());
  if (!created.ok()) {
    return;
  }
  Segment old_segment = std::move(created).value();
  MK_EXPECT_TRUE(old_segment.Append(MakeRecord("old")).ok());

  minikafka::common::StatusOr<Segment> rolled = old_segment.Roll();
  MK_EXPECT_TRUE(rolled.ok());
  if (!rolled.ok()) {
    return;
  }
  Segment active = std::move(rolled).value();
  MK_EXPECT_TRUE(old_segment.sealed());
  MK_EXPECT_EQ(active.base_offset(), 1u);
  MK_EXPECT_EQ(active.next_offset(), 1u);
  MK_EXPECT_TRUE(active.path().find("00000000000000000001.log") !=
                 std::string::npos);

  minikafka::common::StatusOr<std::uint64_t> offset =
      active.Append(MakeRecord("new"));
  MK_EXPECT_TRUE(offset.ok());
  MK_EXPECT_EQ(offset.value(), 1u);

  minikafka::common::StatusOr<Record> old_read = old_segment.ReadAt(0u);
  MK_EXPECT_TRUE(old_read.ok());
  MK_EXPECT_EQ(old_read.value().value, std::string("old"));
  minikafka::common::StatusOr<Record> new_read = active.ReadAt(1u);
  MK_EXPECT_TRUE(new_read.ok());
  MK_EXPECT_EQ(new_read.value().value, std::string("new"));
}

void TestSegmentSetRollsAndReadsAcrossSegments() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  minikafka::common::StatusOr<SegmentSet> created =
      SegmentSet::Create(dir.path(), 0u, kTestSegmentBytes);
  MK_EXPECT_TRUE(created.ok());
  if (!created.ok()) {
    return;
  }
  SegmentSet set = std::move(created).value();

  Record record;
  record.flags = kFlagKeyPresent;
  record.value.assign(20u * 1024u, 'z');

  for (std::uint64_t i = 0; i < 6u; ++i) {
    std::uint64_t offset = 999u;
    const Status status = set.Append(record, &offset);
    MK_EXPECT_TRUE(status.ok());
    MK_EXPECT_EQ(offset, i);
  }
  MK_EXPECT_EQ(set.next_offset(), 6u);
  MK_EXPECT_EQ(set.segment_count(), static_cast<std::size_t>(2));
  MK_EXPECT_EQ(set.active_segment().base_offset(), 3u);

  const std::vector<std::uint64_t> bases = set.base_offsets();
  MK_EXPECT_EQ(bases.size(), static_cast<std::size_t>(2));
  if (bases.size() == 2u) {
    MK_EXPECT_EQ(bases[0], 0u);
    MK_EXPECT_EQ(bases[1], 3u);
  }

  for (std::uint64_t i = 0; i < 6u; ++i) {
    minikafka::common::StatusOr<Record> read = set.ReadAt(i);
    MK_EXPECT_TRUE(read.ok());
    MK_EXPECT_EQ(read.value().value.size(),
                 static_cast<std::size_t>(20u * 1024u));
  }
  minikafka::common::StatusOr<Record> missing = set.ReadAt(6u);
  MK_EXPECT_FALSE(missing.ok());
  ExpectStatus(missing.status(), StatusCode::kOffsetOutOfRange);
}

void TestCheckpointGranularity() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  minikafka::common::StatusOr<Segment> created =
      Segment::Create(dir.path(), 0u, kTestSegmentBytes);
  MK_EXPECT_TRUE(created.ok());
  if (!created.ok()) {
    return;
  }
  Segment segment = std::move(created).value();

  Record record;
  record.flags = kFlagKeyPresent;
  record.value.assign(1024u, 'c');
  const std::size_t record_size = 1024u + 24u;
  const std::size_t record_count = 40u;

  std::size_t expected_entries = 0;
  std::size_t last_position = 0;
  std::size_t position = 0;
  for (std::size_t i = 0; i < record_count; ++i) {
    minikafka::common::StatusOr<std::uint64_t> offset =
        segment.Append(record);
    MK_EXPECT_TRUE(offset.ok());
    MK_EXPECT_EQ(offset.value(), static_cast<std::uint64_t>(i));
    if (i == 0u || position - last_position >= 4096u) {
      ++expected_entries;
      last_position = position;
    }
    position += record_size;
  }

  MK_EXPECT_EQ(segment.size(),
               static_cast<std::uint64_t>(record_count * record_size));
  MK_EXPECT_TRUE(segment.index_valid());
  MK_EXPECT_EQ(segment.index_entry_count(), expected_entries);
  const std::vector<minikafka::storage::IndexEntry>& entries =
      segment.index().entries();
  MK_EXPECT_EQ(entries.size(), expected_entries);
  if (!entries.empty()) {
    MK_EXPECT_EQ(entries.front().offset, 0u);
    MK_EXPECT_EQ(entries.front().position, 0u);
  }
}

}  // namespace

void TestStorageSegment() {
  TestAppendReadAndSeal();
  TestOversizeRecordRejected();
  TestRollKeepsOldSegmentReadable();
  TestSegmentSetRollsAndReadsAcrossSegments();
  TestCheckpointGranularity();
}
