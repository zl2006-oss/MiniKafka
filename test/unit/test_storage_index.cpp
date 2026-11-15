#include "test_common.h"
#include "test_storage_util.h"

#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iterator>
#include <string>
#include <utility>
#include <vector>

#include "minikafka/common/status.h"
#include "minikafka/storage/record.h"
#include "minikafka/storage/segment.h"
#include "minikafka/storage/sparse_index.h"

namespace {

using minikafka::common::Status;
using minikafka::common::StatusCode;
using minikafka::storage::IndexEntry;
using minikafka::storage::kFlagKeyPresent;
using minikafka::storage::Record;
using minikafka::storage::Segment;
using minikafka::storage::SparseIndex;

const std::uint64_t kTestSegmentBytes = 64u * 1024u;

IndexEntry MakeEntry(std::uint64_t offset, std::uint32_t position) {
  IndexEntry entry;
  entry.offset = offset;
  entry.position = position;
  return entry;
}

Record MakeRecord(const std::string& value) {
  Record record;
  record.flags = kFlagKeyPresent;
  record.key = "k";
  record.value = value;
  return record;
}

std::string ReadFile(const std::string& path) {
  std::ifstream file(path.c_str(), std::ios::binary);
  return std::string(std::istreambuf_iterator<char>(file),
                     std::istreambuf_iterator<char>());
}

void WriteFile(const std::string& path, const std::string& data) {
  std::ofstream file(path.c_str(), std::ios::binary | std::ios::trunc);
  file.write(data.data(), static_cast<std::streamsize>(data.size()));
}

void AppendU32(std::string* out, std::uint32_t value) {
  out->push_back(static_cast<char>((value >> 24) & 0xFFu));
  out->push_back(static_cast<char>((value >> 16) & 0xFFu));
  out->push_back(static_cast<char>((value >> 8) & 0xFFu));
  out->push_back(static_cast<char>(value & 0xFFu));
}

void AppendU64(std::string* out, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xFFu));
  }
}

std::uint32_t ReadU32(const std::string& data, std::size_t offset) {
  return (static_cast<std::uint32_t>(
              static_cast<unsigned char>(data[offset])) << 24) |
         (static_cast<std::uint32_t>(
              static_cast<unsigned char>(data[offset + 1u])) << 16) |
         (static_cast<std::uint32_t>(
              static_cast<unsigned char>(data[offset + 2u])) << 8) |
         static_cast<std::uint32_t>(
             static_cast<unsigned char>(data[offset + 3u]));
}

std::uint64_t ReadU64(const std::string& data, std::size_t offset) {
  std::uint64_t value = 0;
  for (std::size_t i = 0; i < 8u; ++i) {
    value = (value << 8) |
            static_cast<std::uint64_t>(
                static_cast<unsigned char>(data[offset + i]));
  }
  return value;
}

std::string RawEntry(std::uint64_t offset, std::uint32_t position) {
  std::string data;
  AppendU64(&data, offset);
  AppendU32(&data, position);
  return data;
}

void ExpectSameEntries(const std::vector<IndexEntry>& actual,
                       const std::vector<IndexEntry>& expected) {
  MK_EXPECT_EQ(actual.size(), expected.size());
  const std::size_t count =
      actual.size() < expected.size() ? actual.size() : expected.size();
  for (std::size_t i = 0; i < count; ++i) {
    MK_EXPECT_EQ(actual[i].offset, expected[i].offset);
    MK_EXPECT_EQ(actual[i].position, expected[i].position);
  }
}

void TestSparseIndexAppendAndLookup() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  const std::string path = dir.path() + "/test.index";
  SparseIndex index(path);
  index.ResetForNew();
  MK_EXPECT_TRUE(index.valid());
  MK_EXPECT_TRUE(index.AppendEntry(5u, 0u).ok());
  MK_EXPECT_TRUE(index.AppendEntry(10u, 5000u).ok());
  MK_EXPECT_EQ(index.entry_count(), static_cast<std::size_t>(2));

  IndexEntry entry;
  MK_EXPECT_FALSE(index.FindCheckpoint(0u, &entry));
  MK_EXPECT_TRUE(index.FindCheckpoint(5u, &entry));
  MK_EXPECT_EQ(entry.offset, 5u);
  MK_EXPECT_EQ(entry.position, 0u);
  MK_EXPECT_TRUE(index.FindCheckpoint(7u, &entry));
  MK_EXPECT_EQ(entry.offset, 5u);
  MK_EXPECT_TRUE(index.FindCheckpoint(10u, &entry));
  MK_EXPECT_EQ(entry.offset, 10u);
  MK_EXPECT_EQ(entry.position, 5000u);
  MK_EXPECT_TRUE(index.FindCheckpoint(100u, &entry));
  MK_EXPECT_EQ(entry.offset, 10u);

  const std::string data = ReadFile(path);
  MK_EXPECT_EQ(data.size(), static_cast<std::size_t>(24));
  if (data.size() == 24u) {
    MK_EXPECT_EQ(ReadU64(data, 0u), 5u);
    MK_EXPECT_EQ(ReadU32(data, 8u), 0u);
    MK_EXPECT_EQ(ReadU64(data, 12u), 10u);
    MK_EXPECT_EQ(ReadU32(data, 20u), 5000u);
  }

  SparseIndex loaded(path);
  loaded.Load(10000u, 5u);
  MK_EXPECT_TRUE(loaded.valid());
  MK_EXPECT_EQ(loaded.entry_count(), static_cast<std::size_t>(2));
  if (loaded.entry_count() == 2u) {
    MK_EXPECT_EQ(loaded.entries()[0].offset, 5u);
    MK_EXPECT_EQ(loaded.entries()[0].position, 0u);
    MK_EXPECT_EQ(loaded.entries()[1].offset, 10u);
    MK_EXPECT_EQ(loaded.entries()[1].position, 5000u);
  }
}

void TestSparseIndexStoreAndLoad() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  const std::string path = dir.path() + "/stored.index";
  SparseIndex index(path);

  std::vector<IndexEntry> entries;
  entries.push_back(MakeEntry(0u, 0u));
  entries.push_back(MakeEntry(100u, 4096u));
  entries.push_back(MakeEntry(200u, 8192u));
  const Status stored = index.Store(entries, 10000u, 0u);
  MK_EXPECT_TRUE(stored.ok());
  MK_EXPECT_TRUE(index.valid());
  MK_EXPECT_EQ(index.entry_count(), static_cast<std::size_t>(3));

  IndexEntry entry;
  MK_EXPECT_TRUE(index.FindCheckpoint(150u, &entry));
  MK_EXPECT_EQ(entry.offset, 100u);
  MK_EXPECT_EQ(entry.position, 4096u);
  MK_EXPECT_TRUE(index.FindCheckpoint(200u, &entry));
  MK_EXPECT_EQ(entry.offset, 200u);

  MK_EXPECT_TRUE(::access((path + ".tmp").c_str(), F_OK) != 0);
  const std::string data = ReadFile(path);
  MK_EXPECT_EQ(data.size(), static_cast<std::size_t>(36));
  if (data.size() == 36u) {
    MK_EXPECT_EQ(ReadU64(data, 0u), 0u);
    MK_EXPECT_EQ(ReadU32(data, 8u), 0u);
    MK_EXPECT_EQ(ReadU64(data, 12u), 100u);
    MK_EXPECT_EQ(ReadU32(data, 20u), 4096u);
    MK_EXPECT_EQ(ReadU64(data, 24u), 200u);
    MK_EXPECT_EQ(ReadU32(data, 32u), 8192u);
  }

  SparseIndex loaded(path);
  loaded.Load(10000u, 0u);
  MK_EXPECT_TRUE(loaded.valid());
  ExpectSameEntries(loaded.entries(), entries);
}

void TestSparseIndexInvalidFileDegrades() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }

  {
    SparseIndex missing(dir.path() + "/missing.index");
    missing.Load(1000u, 0u);
    MK_EXPECT_FALSE(missing.valid());
  }

  {
    const std::string path = dir.path() + "/bad_length.index";
    WriteFile(path, "abcde");
    SparseIndex index(path);
    index.Load(1000u, 0u);
    MK_EXPECT_FALSE(index.valid());
  }

  {
    const std::string path = dir.path() + "/bad_order.index";
    WriteFile(path, RawEntry(10u, 0u) + RawEntry(5u, 4096u));
    SparseIndex index(path);
    index.Load(10000u, 10u);
    MK_EXPECT_FALSE(index.valid());
  }

  {
    const std::string path = dir.path() + "/bad_first_position.index";
    WriteFile(path, RawEntry(0u, 12u) + RawEntry(1u, 4096u));
    SparseIndex index(path);
    index.Load(10000u, 0u);
    MK_EXPECT_FALSE(index.valid());
  }

  {
    const std::string path = dir.path() + "/bad_position.index";
    WriteFile(path, RawEntry(0u, 0u) + RawEntry(1u, 1000u));
    SparseIndex index(path);
    index.Load(1000u, 0u);
    MK_EXPECT_FALSE(index.valid());
  }

  {
    const std::string path = dir.path() + "/bad_base.index";
    WriteFile(path, RawEntry(0u, 0u));
    SparseIndex index(path);
    index.Load(10000u, 5u);
    MK_EXPECT_FALSE(index.valid());
  }

  {
    const std::string path = dir.path() + "/nonempty_empty_segment.index";
    WriteFile(path, RawEntry(0u, 0u));
    SparseIndex index(path);
    index.Load(0u, 0u);
    MK_EXPECT_FALSE(index.valid());
  }
}

void TestSegmentIndexRebuildAfterDelete() {
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
  record.value.assign(1024u, 'r');
  const std::uint64_t record_count = 40u;
  for (std::uint64_t i = 0; i < record_count; ++i) {
    MK_EXPECT_TRUE(segment.Append(record).ok());
  }

  const std::vector<IndexEntry> original_entries =
      segment.index().entries();
  MK_EXPECT_FALSE(original_entries.empty());
  const std::string index_path = segment.index_path();
  MK_EXPECT_EQ(::unlink(index_path.c_str()), 0);

  minikafka::common::StatusOr<Segment> opened_result =
      Segment::Open(dir.path(), 0u, record_count, kTestSegmentBytes);
  MK_EXPECT_TRUE(opened_result.ok());
  if (!opened_result.ok()) {
    return;
  }
  Segment opened = std::move(opened_result).value();
  MK_EXPECT_FALSE(opened.index_valid());
  MK_EXPECT_EQ(opened.index_entry_count(), static_cast<std::size_t>(0));

  minikafka::common::StatusOr<Record> read = opened.ReadAt(39u);
  MK_EXPECT_TRUE(read.ok());
  if (read.ok()) {
    MK_EXPECT_EQ(read.value().value.size(), static_cast<std::size_t>(1024));
  }

  const Status rebuilt = opened.RebuildIndex();
  MK_EXPECT_TRUE(rebuilt.ok());
  MK_EXPECT_TRUE(opened.index_valid());
  ExpectSameEntries(opened.index().entries(), original_entries);
  MK_EXPECT_TRUE(::access((index_path + ".tmp").c_str(), F_OK) != 0);
  MK_EXPECT_TRUE(opened.ReadAt(0u).ok());
  MK_EXPECT_TRUE(opened.ReadAt(39u).ok());
}

void TestSegmentIndexRebuildAfterCorruption() {
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
  for (std::uint64_t i = 0; i < 3u; ++i) {
    MK_EXPECT_TRUE(segment.Append(MakeRecord("corrupt-index")).ok());
  }
  const std::vector<IndexEntry> original_entries =
      segment.index().entries();
  const std::string index_path = segment.index_path();
  WriteFile(index_path, "garbage");

  minikafka::common::StatusOr<Segment> opened_result =
      Segment::Open(dir.path(), 0u, 3u, kTestSegmentBytes);
  MK_EXPECT_TRUE(opened_result.ok());
  if (!opened_result.ok()) {
    return;
  }
  Segment opened = std::move(opened_result).value();
  MK_EXPECT_FALSE(opened.index_valid());
  MK_EXPECT_TRUE(opened.ReadAt(2u).ok());

  MK_EXPECT_TRUE(opened.RebuildIndex().ok());
  MK_EXPECT_TRUE(opened.index_valid());
  ExpectSameEntries(opened.index().entries(), original_entries);
  MK_EXPECT_TRUE(::access((index_path + ".tmp").c_str(), F_OK) != 0);
  MK_EXPECT_TRUE(opened.ReadAt(2u).ok());
}

}  // namespace

void TestStorageIndex() {
  TestSparseIndexAppendAndLookup();
  TestSparseIndexStoreAndLoad();
  TestSparseIndexInvalidFileDegrades();
  TestSegmentIndexRebuildAfterDelete();
  TestSegmentIndexRebuildAfterCorruption();
}
