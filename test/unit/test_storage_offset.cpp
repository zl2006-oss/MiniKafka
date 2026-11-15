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

#include "minikafka/common/crc32.h"
#include "minikafka/common/status.h"
#include "minikafka/storage/offset_file.h"

namespace {

using minikafka::common::Status;
using minikafka::common::StatusCode;
using minikafka::common::StatusOr;
using minikafka::storage::kOffsetEntrySize;
using minikafka::storage::kOffsetFileFooterSize;
using minikafka::storage::kOffsetFileHeaderSize;
using minikafka::storage::kOffsetFileMagic;
using minikafka::storage::kOffsetFileVersion;
using minikafka::storage::OffsetEntry;
using minikafka::storage::OffsetLoadResult;
using minikafka::storage::OffsetSnapshot;
using minikafka::storage::OffsetStore;

OffsetEntry MakeEntry(std::uint32_t partition,
                      std::uint64_t committed_offset) {
  OffsetEntry entry;
  entry.partition = partition;
  entry.committed_offset = committed_offset;
  return entry;
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

void AppendU16(std::string* out, std::uint16_t value) {
  out->push_back(static_cast<char>((value >> 8) & 0xFFu));
  out->push_back(static_cast<char>(value & 0xFFu));
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

std::string BuildRawOffsetFile(std::uint64_t generation,
                               const std::vector<OffsetEntry>& entries) {
  std::string data;
  AppendU32(&data, kOffsetFileMagic);
  AppendU16(&data, kOffsetFileVersion);
  AppendU16(&data, static_cast<std::uint16_t>(kOffsetFileHeaderSize));
  AppendU64(&data, generation);
  AppendU32(&data, static_cast<std::uint32_t>(entries.size()));
  AppendU32(&data, 0u);
  for (std::size_t i = 0; i < entries.size(); ++i) {
    AppendU32(&data, entries[i].partition);
    AppendU64(&data, entries[i].committed_offset);
  }
  AppendU32(&data, minikafka::common::Crc32(data));
  return data;
}

void ExpectStatus(const Status& status, StatusCode code) {
  MK_EXPECT_FALSE(status.ok());
  MK_EXPECT_EQ(status.code(), code);
}

bool ReadSlot(const std::string& path, OffsetSnapshot* snapshot) {
  const std::string data = ReadFile(path);
  return minikafka::storage::DecodeOffsetSnapshot(data.data(), data.size(),
                                                  snapshot)
      .ok();
}

const OffsetEntry* FindEntry(const OffsetSnapshot& snapshot,
                             std::uint32_t partition) {
  for (std::size_t i = 0; i < snapshot.entries.size(); ++i) {
    if (snapshot.entries[i].partition == partition) {
      return &snapshot.entries[i];
    }
  }
  return nullptr;
}

bool CommitEntries(OffsetStore* store,
                   const std::vector<OffsetEntry>& updates,
                   OffsetSnapshot* snapshot) {
  const Status status = store->Commit(updates, snapshot);
  MK_EXPECT_TRUE(status.ok());
  return status.ok();
}

void TestOffsetCodecRoundTripAndInvalidEntries() {
  OffsetSnapshot snapshot;
  snapshot.generation = 7u;
  snapshot.entries.push_back(MakeEntry(0u, 0u));
  snapshot.entries.push_back(MakeEntry(3u, 42u));
  snapshot.entries.push_back(MakeEntry(15u, 100u));

  std::string bytes;
  Status status = minikafka::storage::EncodeOffsetSnapshot(snapshot, &bytes);
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_EQ(bytes.size(),
               kOffsetFileHeaderSize + 3u * kOffsetEntrySize +
                   kOffsetFileFooterSize);

  OffsetSnapshot decoded;
  status = minikafka::storage::DecodeOffsetSnapshot(bytes.data(), bytes.size(),
                                                    &decoded);
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_EQ(decoded.generation, 7u);
  MK_EXPECT_EQ(decoded.entries.size(), static_cast<std::size_t>(3));
  if (decoded.entries.size() == 3u) {
    MK_EXPECT_EQ(decoded.entries[0].partition, 0u);
    MK_EXPECT_EQ(decoded.entries[0].committed_offset, 0u);
    MK_EXPECT_EQ(decoded.entries[1].partition, 3u);
    MK_EXPECT_EQ(decoded.entries[1].committed_offset, 42u);
    MK_EXPECT_EQ(decoded.entries[2].partition, 15u);
    MK_EXPECT_EQ(decoded.entries[2].committed_offset, 100u);
  }

  std::string corrupt = bytes;
  corrupt[corrupt.size() - 1u] ^= static_cast<char>(0xFF);
  MK_EXPECT_FALSE(minikafka::storage::DecodeOffsetSnapshot(
                      corrupt.data(), corrupt.size(), &decoded)
                      .ok());

  OffsetSnapshot bad_order;
  bad_order.generation = 1u;
  bad_order.entries.push_back(MakeEntry(1u, 0u));
  bad_order.entries.push_back(MakeEntry(0u, 0u));
  std::string bad_bytes;
  ExpectStatus(
      minikafka::storage::EncodeOffsetSnapshot(bad_order, &bad_bytes),
      StatusCode::kPayloadDecodeError);

  OffsetSnapshot duplicate;
  duplicate.generation = 1u;
  duplicate.entries.push_back(MakeEntry(0u, 1u));
  duplicate.entries.push_back(MakeEntry(0u, 2u));
  ExpectStatus(
      minikafka::storage::EncodeOffsetSnapshot(duplicate, &bad_bytes),
      StatusCode::kPayloadDecodeError);

  OffsetSnapshot out_of_range;
  out_of_range.generation = 1u;
  out_of_range.entries.push_back(MakeEntry(16u, 1u));
  ExpectStatus(
      minikafka::storage::EncodeOffsetSnapshot(out_of_range, &bad_bytes),
      StatusCode::kInvalidPartition);

  std::vector<OffsetEntry> raw_entries;
  raw_entries.push_back(MakeEntry(1u, 0u));
  raw_entries.push_back(MakeEntry(0u, 0u));
  const std::string raw_bad_order =
      BuildRawOffsetFile(1u, raw_entries);
  MK_EXPECT_FALSE(minikafka::storage::DecodeOffsetSnapshot(
                      raw_bad_order.data(), raw_bad_order.size(), &decoded)
                      .ok());

  raw_entries.clear();
  raw_entries.push_back(MakeEntry(0u, 1u));
  raw_entries.push_back(MakeEntry(0u, 2u));
  const std::string raw_duplicate =
      BuildRawOffsetFile(1u, raw_entries);
  MK_EXPECT_FALSE(minikafka::storage::DecodeOffsetSnapshot(
                      raw_duplicate.data(), raw_duplicate.size(), &decoded)
                      .ok());

  raw_entries.clear();
  raw_entries.push_back(MakeEntry(16u, 0u));
  const std::string raw_out_of_range =
      BuildRawOffsetFile(1u, raw_entries);
  MK_EXPECT_FALSE(minikafka::storage::DecodeOffsetSnapshot(
                      raw_out_of_range.data(), raw_out_of_range.size(),
                      &decoded)
                      .ok());
}

void TestDoubleSlotRotation() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  StatusOr<OffsetStore> opened =
      OffsetStore::Open(dir.path(), "g1", "orders");
  MK_EXPECT_TRUE(opened.ok());
  if (!opened.ok()) {
    return;
  }
  OffsetStore store = std::move(opened).value();

  OffsetSnapshot snapshot;
  std::vector<OffsetEntry> updates;
  updates.push_back(MakeEntry(0u, 5u));
  MK_EXPECT_TRUE(CommitEntries(&store, updates, &snapshot));
  MK_EXPECT_EQ(snapshot.generation, 1u);
  MK_EXPECT_TRUE(::access(store.slot_a_path().c_str(), F_OK) == 0);
  MK_EXPECT_TRUE(::access(store.slot_b_path().c_str(), F_OK) != 0);

  OffsetSnapshot slot_a;
  OffsetSnapshot slot_b;
  MK_EXPECT_TRUE(ReadSlot(store.slot_a_path(), &slot_a));
  MK_EXPECT_EQ(slot_a.generation, 1u);

  updates[0].committed_offset = 8u;
  MK_EXPECT_TRUE(CommitEntries(&store, updates, &snapshot));
  MK_EXPECT_EQ(snapshot.generation, 2u);
  MK_EXPECT_TRUE(::access(store.slot_b_path().c_str(), F_OK) == 0);
  MK_EXPECT_TRUE(ReadSlot(store.slot_a_path(), &slot_a));
  MK_EXPECT_TRUE(ReadSlot(store.slot_b_path(), &slot_b));
  MK_EXPECT_EQ(slot_a.generation, 1u);
  MK_EXPECT_EQ(slot_b.generation, 2u);

  updates[0].committed_offset = 10u;
  MK_EXPECT_TRUE(CommitEntries(&store, updates, &snapshot));
  MK_EXPECT_EQ(snapshot.generation, 3u);
  MK_EXPECT_TRUE(ReadSlot(store.slot_a_path(), &slot_a));
  MK_EXPECT_TRUE(ReadSlot(store.slot_b_path(), &slot_b));
  MK_EXPECT_EQ(slot_a.generation, 3u);
  MK_EXPECT_EQ(slot_b.generation, 2u);
  MK_EXPECT_TRUE(::access(store.tmp_path().c_str(), F_OK) != 0);

  StatusOr<OffsetLoadResult> loaded = store.Load();
  MK_EXPECT_TRUE(loaded.ok());
  if (loaded.ok()) {
    MK_EXPECT_TRUE(loaded.value().found);
    MK_EXPECT_FALSE(loaded.value().degraded);
    MK_EXPECT_EQ(loaded.value().snapshot.generation, 3u);
    const OffsetEntry* entry = FindEntry(loaded.value().snapshot, 0u);
    MK_EXPECT_TRUE(entry != nullptr);
    if (entry != nullptr) {
      MK_EXPECT_EQ(entry->committed_offset, 10u);
    }
  }
}

void TestTmpOnlyCrashKeepsOldSnapshot() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  StatusOr<OffsetStore> opened =
      OffsetStore::Open(dir.path(), "g1", "orders");
  MK_EXPECT_TRUE(opened.ok());
  if (!opened.ok()) {
    return;
  }
  OffsetStore store = std::move(opened).value();

  OffsetSnapshot snapshot;
  std::vector<OffsetEntry> updates;
  updates.push_back(MakeEntry(0u, 5u));
  MK_EXPECT_TRUE(CommitEntries(&store, updates, &snapshot));

  OffsetSnapshot pending;
  pending.generation = 2u;
  pending.entries.push_back(MakeEntry(0u, 99u));
  std::string pending_bytes;
  MK_EXPECT_TRUE(
      minikafka::storage::EncodeOffsetSnapshot(pending, &pending_bytes).ok());
  WriteFile(store.tmp_path(), pending_bytes);

  StatusOr<OffsetLoadResult> loaded = store.Load();
  MK_EXPECT_TRUE(loaded.ok());
  if (!loaded.ok()) {
    return;
  }
  MK_EXPECT_TRUE(loaded.value().found);
  MK_EXPECT_EQ(loaded.value().snapshot.generation, 1u);
  const OffsetEntry* entry = FindEntry(loaded.value().snapshot, 0u);
  MK_EXPECT_TRUE(entry != nullptr);
  if (entry != nullptr) {
    MK_EXPECT_EQ(entry->committed_offset, 5u);
  }

  updates[0].committed_offset = 11u;
  MK_EXPECT_TRUE(CommitEntries(&store, updates, &snapshot));
  MK_EXPECT_EQ(snapshot.generation, 2u);
  MK_EXPECT_TRUE(::access(store.tmp_path().c_str(), F_OK) != 0);
}

void TestSingleSlotCorruptionFallsBack() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  StatusOr<OffsetStore> opened =
      OffsetStore::Open(dir.path(), "g1", "orders");
  MK_EXPECT_TRUE(opened.ok());
  if (!opened.ok()) {
    return;
  }
  OffsetStore store = std::move(opened).value();

  OffsetSnapshot snapshot;
  std::vector<OffsetEntry> updates;
  updates.push_back(MakeEntry(0u, 5u));
  MK_EXPECT_TRUE(CommitEntries(&store, updates, &snapshot));
  updates[0].committed_offset = 8u;
  MK_EXPECT_TRUE(CommitEntries(&store, updates, &snapshot));

  std::string corrupt_slot = ReadFile(store.slot_b_path());
  MK_EXPECT_FALSE(corrupt_slot.empty());
  if (!corrupt_slot.empty()) {
    corrupt_slot[corrupt_slot.size() - 1u] ^= static_cast<char>(0xFF);
    WriteFile(store.slot_b_path(), corrupt_slot);
  }

  StatusOr<OffsetLoadResult> loaded = store.Load();
  MK_EXPECT_TRUE(loaded.ok());
  if (!loaded.ok()) {
    return;
  }
  MK_EXPECT_TRUE(loaded.value().found);
  MK_EXPECT_TRUE(loaded.value().degraded);
  MK_EXPECT_FALSE(loaded.value().warning.empty());
  MK_EXPECT_EQ(loaded.value().snapshot.generation, 1u);
  const OffsetEntry* entry = FindEntry(loaded.value().snapshot, 0u);
  MK_EXPECT_TRUE(entry != nullptr);
  if (entry != nullptr) {
    MK_EXPECT_EQ(entry->committed_offset, 5u);
  }
}

void TestBothSlotsCorruptReturnsNoOffsets() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  StatusOr<OffsetStore> opened =
      OffsetStore::Open(dir.path(), "g1", "orders");
  MK_EXPECT_TRUE(opened.ok());
  if (!opened.ok()) {
    return;
  }
  OffsetStore store = std::move(opened).value();

  OffsetSnapshot snapshot;
  std::vector<OffsetEntry> updates;
  updates.push_back(MakeEntry(0u, 5u));
  MK_EXPECT_TRUE(CommitEntries(&store, updates, &snapshot));

  WriteFile(store.slot_a_path(), "broken-a");
  WriteFile(store.slot_b_path(), "broken-b");

  StatusOr<OffsetLoadResult> loaded = store.Load();
  MK_EXPECT_TRUE(loaded.ok());
  if (!loaded.ok()) {
    return;
  }
  MK_EXPECT_FALSE(loaded.value().found);
  MK_EXPECT_TRUE(loaded.value().degraded);
  MK_EXPECT_FALSE(loaded.value().warning.empty());
  MK_EXPECT_TRUE(loaded.value().snapshot.entries.empty());
}

void TestCommitDoesNotRegress() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  StatusOr<OffsetStore> opened =
      OffsetStore::Open(dir.path(), "g1", "orders");
  MK_EXPECT_TRUE(opened.ok());
  if (!opened.ok()) {
    return;
  }
  OffsetStore store = std::move(opened).value();

  OffsetSnapshot snapshot;
  std::vector<OffsetEntry> updates;
  updates.push_back(MakeEntry(0u, 10u));
  updates.push_back(MakeEntry(1u, 2u));
  MK_EXPECT_TRUE(CommitEntries(&store, updates, &snapshot));
  MK_EXPECT_EQ(snapshot.generation, 1u);

  updates.clear();
  updates.push_back(MakeEntry(0u, 7u));
  updates.push_back(MakeEntry(1u, 5u));
  MK_EXPECT_TRUE(CommitEntries(&store, updates, &snapshot));
  MK_EXPECT_EQ(snapshot.generation, 2u);

  const OffsetEntry* zero = FindEntry(snapshot, 0u);
  const OffsetEntry* one = FindEntry(snapshot, 1u);
  MK_EXPECT_TRUE(zero != nullptr);
  MK_EXPECT_TRUE(one != nullptr);
  if (zero != nullptr) {
    MK_EXPECT_EQ(zero->committed_offset, 10u);
  }
  if (one != nullptr) {
    MK_EXPECT_EQ(one->committed_offset, 5u);
  }

  StatusOr<OffsetLoadResult> loaded = store.Load();
  MK_EXPECT_TRUE(loaded.ok());
  if (loaded.ok()) {
    const OffsetEntry* loaded_zero =
        FindEntry(loaded.value().snapshot, 0u);
    MK_EXPECT_TRUE(loaded_zero != nullptr);
    if (loaded_zero != nullptr) {
      MK_EXPECT_EQ(loaded_zero->committed_offset, 10u);
    }
  }
}

void TestNoOffsetsBeforeCommit() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  StatusOr<OffsetStore> opened =
      OffsetStore::Open(dir.path(), "g1", "orders");
  MK_EXPECT_TRUE(opened.ok());
  if (!opened.ok()) {
    return;
  }
  OffsetStore store = std::move(opened).value();

  StatusOr<OffsetLoadResult> loaded = store.Load();
  MK_EXPECT_TRUE(loaded.ok());
  if (loaded.ok()) {
    MK_EXPECT_FALSE(loaded.value().found);
    MK_EXPECT_FALSE(loaded.value().degraded);
    MK_EXPECT_TRUE(loaded.value().warning.empty());
  }
}

}  // namespace

void TestStorageOffset() {
  TestOffsetCodecRoundTripAndInvalidEntries();
  TestDoubleSlotRotation();
  TestTmpOnlyCrashKeepsOldSnapshot();
  TestSingleSlotCorruptionFallsBack();
  TestBothSlotsCorruptReturnsNoOffsets();
  TestCommitDoesNotRegress();
  TestNoOffsetsBeforeCommit();
}
