#include "minikafka/storage/offset_file.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "byte_codec.h"
#include "file_util.h"
#include "minikafka/common/crc32.h"
#include "minikafka/storage/paths.h"

namespace minikafka {
namespace storage {

using common::Status;
using common::StatusCode;
using common::StatusOr;

namespace {

const std::uint64_t kOffsetFileMinSize =
    static_cast<std::uint64_t>(kOffsetFileHeaderSize) +
    static_cast<std::uint64_t>(kOffsetFileFooterSize);

struct SlotInfo {
  bool exists = false;
  bool valid = false;
  OffsetSnapshot snapshot;
  std::string error;
};

bool ValidateEntryOrder(const std::vector<OffsetEntry>& entries,
                        Status* error) {
  for (std::size_t i = 0; i < entries.size(); ++i) {
    if (entries[i].partition >= kMaxOffsetPartitions) {
      *error = Status(StatusCode::kInvalidPartition,
                      "offset partition is out of range");
      return false;
    }
    if (i != 0u && entries[i].partition <= entries[i - 1u].partition) {
      *error = Status(StatusCode::kPayloadDecodeError,
                      "offset entries must be sorted and unique");
      return false;
    }
  }
  return true;
}

void LoadSlot(const std::string& path, SlotInfo* slot) {
  slot->exists = false;
  slot->valid = false;
  slot->snapshot = OffsetSnapshot();
  slot->error.clear();

  std::string data;
  bool exists = false;
  const Status read_status = internal::ReadWholeFile(path, &data, &exists);
  if (!read_status.ok()) {
    slot->exists = true;
    slot->error = read_status.ToString();
    return;
  }
  if (!exists) {
    return;
  }
  slot->exists = true;

  OffsetSnapshot snapshot;
  const Status decode_status =
      DecodeOffsetSnapshot(data.data(), data.size(), &snapshot);
  if (!decode_status.ok()) {
    slot->error = decode_status.ToString();
    return;
  }
  slot->snapshot = std::move(snapshot);
  slot->valid = true;
}

const SlotInfo* BestSlot(const SlotInfo& a, const SlotInfo& b) {
  if (a.valid && b.valid) {
    return a.snapshot.generation >= b.snapshot.generation ? &a : &b;
  }
  if (a.valid) {
    return &a;
  }
  if (b.valid) {
    return &b;
  }
  return nullptr;
}

void AppendSlotWarning(const SlotInfo& slot, const char* name,
                       std::string* warning) {
  if (!slot.exists || slot.valid) {
    return;
  }
  if (!warning->empty()) {
    *warning += "; ";
  }
  *warning += name;
  if (!slot.error.empty()) {
    *warning += ": ";
    *warning += slot.error;
  }
}

}  // namespace

Status EncodeOffsetSnapshot(const OffsetSnapshot& snapshot,
                            std::string* bytes) {
  if (bytes == nullptr) {
    return Status(StatusCode::kInternalError, "null offset snapshot output");
  }
  if (snapshot.entries.size() >
      static_cast<std::size_t>(kMaxOffsetPartitions)) {
    return Status(StatusCode::kPayloadDecodeError,
                  "too many offset entries");
  }
  Status order_error;
  if (!ValidateEntryOrder(snapshot.entries, &order_error)) {
    return order_error;
  }

  std::string out;
  out.reserve(kOffsetFileHeaderSize +
              snapshot.entries.size() * kOffsetEntrySize +
              kOffsetFileFooterSize);
  internal::AppendU32(&out, kOffsetFileMagic);
  internal::AppendU16(&out, kOffsetFileVersion);
  internal::AppendU16(
      &out, static_cast<std::uint16_t>(kOffsetFileHeaderSize));
  internal::AppendU64(&out, snapshot.generation);
  internal::AppendU32(
      &out, static_cast<std::uint32_t>(snapshot.entries.size()));
  internal::AppendU32(&out, 0u);
  for (std::size_t i = 0; i < snapshot.entries.size(); ++i) {
    internal::AppendU32(&out, snapshot.entries[i].partition);
    internal::AppendU64(&out, snapshot.entries[i].committed_offset);
  }
  internal::AppendU32(&out, common::Crc32(out));

  *bytes = std::move(out);
  return Status();
}

Status DecodeOffsetSnapshot(const void* data, std::size_t size,
                            OffsetSnapshot* snapshot) {
  if (snapshot == nullptr) {
    return Status(StatusCode::kInternalError, "null offset snapshot output");
  }
  if (size < kOffsetFileMinSize) {
    return Status(StatusCode::kPayloadDecodeError,
                  "offset file is too small");
  }
  if (data == nullptr) {
    return Status(StatusCode::kInternalError, "null offset file input");
  }

  const unsigned char* bytes = static_cast<const unsigned char*>(data);
  const std::uint32_t magic = internal::ReadU32(bytes);
  if (magic != kOffsetFileMagic) {
    return Status(StatusCode::kInvalidFrame, "invalid offset file magic");
  }
  const std::uint16_t version = internal::ReadU16(bytes + 4u);
  if (version != kOffsetFileVersion) {
    return Status(StatusCode::kUnsupportedVersion,
                  "unsupported offset file version");
  }
  const std::uint16_t header_size = internal::ReadU16(bytes + 6u);
  if (header_size != kOffsetFileHeaderSize) {
    return Status(StatusCode::kPayloadDecodeError,
                  "invalid offset header size");
  }
  const std::uint64_t generation = internal::ReadU64(bytes + 8u);
  const std::uint32_t entry_count = internal::ReadU32(bytes + 16u);
  const std::uint32_t reserved = internal::ReadU32(bytes + 20u);
  if (reserved != 0u) {
    return Status(StatusCode::kPayloadDecodeError,
                  "offset header reserved field must be zero");
  }
  if (entry_count > kMaxOffsetPartitions) {
    return Status(StatusCode::kPayloadDecodeError,
                  "too many offset entries");
  }

  const std::uint64_t expected_size =
      static_cast<std::uint64_t>(kOffsetFileHeaderSize) +
      static_cast<std::uint64_t>(entry_count) *
          static_cast<std::uint64_t>(kOffsetEntrySize) +
      static_cast<std::uint64_t>(kOffsetFileFooterSize);
  if (expected_size != static_cast<std::uint64_t>(size)) {
    return Status(StatusCode::kPayloadDecodeError,
                  "offset file length does not match entry_count");
  }

  const std::uint32_t stored_crc =
      internal::ReadU32(bytes + size - kOffsetFileFooterSize);
  if (common::Crc32(data, size - kOffsetFileFooterSize) != stored_crc) {
    return Status(StatusCode::kFrameCrcError, "offset file CRC mismatch");
  }

  OffsetSnapshot decoded;
  decoded.generation = generation;
  decoded.entries.reserve(entry_count);
  std::size_t offset = kOffsetFileHeaderSize;
  for (std::uint32_t i = 0; i < entry_count; ++i) {
    OffsetEntry entry;
    entry.partition = internal::ReadU32(bytes + offset);
    entry.committed_offset = internal::ReadU64(bytes + offset + 4u);
    decoded.entries.push_back(entry);
    offset += kOffsetEntrySize;
  }
  Status order_error;
  if (!ValidateEntryOrder(decoded.entries, &order_error)) {
    return order_error;
  }

  *snapshot = std::move(decoded);
  return Status();
}

OffsetStore::OffsetStore(std::string directory)
    : directory_(std::move(directory)),
      slot_a_path_(internal::JoinPath(directory_, "offsets.a")),
      slot_b_path_(internal::JoinPath(directory_, "offsets.b")),
      tmp_path_(internal::JoinPath(directory_, "offsets.tmp")) {}

common::StatusOr<OffsetStore> OffsetStore::Open(const std::string& data_dir,
                                                const std::string& group,
                                                const std::string& topic) {
  std::string directory;
  const Status status =
      GroupTopicDirectory(data_dir, group, topic, &directory);
  if (!status.ok()) {
    return StatusOr<OffsetStore>(status);
  }
  return StatusOr<OffsetStore>(OffsetStore(std::move(directory)));
}

common::StatusOr<OffsetLoadResult> OffsetStore::Load() const {
  SlotInfo slot_a;
  SlotInfo slot_b;
  LoadSlot(slot_a_path_, &slot_a);
  LoadSlot(slot_b_path_, &slot_b);

  OffsetLoadResult result;
  const SlotInfo* best = BestSlot(slot_a, slot_b);
  if (best != nullptr) {
    result.found = true;
    result.snapshot = best->snapshot;
  }
  result.degraded =
      (slot_a.exists && !slot_a.valid) || (slot_b.exists && !slot_b.valid);
  if (result.degraded) {
    AppendSlotWarning(slot_a, "offsets.a", &result.warning);
    AppendSlotWarning(slot_b, "offsets.b", &result.warning);
  }
  return StatusOr<OffsetLoadResult>(std::move(result));
}

common::Status OffsetStore::Commit(const std::vector<OffsetEntry>& updates,
                                   OffsetSnapshot* resulting) {
  if (resulting == nullptr) {
    return Status(StatusCode::kInternalError, "null offset commit output");
  }
  for (std::size_t i = 0; i < updates.size(); ++i) {
    if (updates[i].partition >= kMaxOffsetPartitions) {
      return Status(StatusCode::kInvalidPartition,
                    "offset partition is out of range");
    }
  }

  SlotInfo slot_a;
  SlotInfo slot_b;
  LoadSlot(slot_a_path_, &slot_a);
  LoadSlot(slot_b_path_, &slot_b);
  const SlotInfo* best = BestSlot(slot_a, slot_b);

  std::string target_path;
  if (slot_a.valid && slot_b.valid) {
    target_path = slot_a.snapshot.generation <= slot_b.snapshot.generation
                      ? slot_a_path_
                      : slot_b_path_;
  } else if (slot_a.valid) {
    target_path = slot_b_path_;
  } else if (slot_b.valid) {
    target_path = slot_a_path_;
  } else if (!slot_a.exists && slot_b.exists) {
    target_path = slot_a_path_;
  } else if (slot_a.exists && !slot_b.exists) {
    target_path = slot_b_path_;
  } else {
    target_path = slot_a_path_;
  }

  std::uint64_t next_generation = 1u;
  if (best != nullptr) {
    if (best->snapshot.generation ==
        (std::numeric_limits<std::uint64_t>::max)()) {
      return Status(StatusCode::kInternalError,
                    "offset generation overflow");
    }
    next_generation = best->snapshot.generation + 1u;
  }

  OffsetSnapshot merged;
  merged.generation = next_generation;
  if (best != nullptr) {
    merged.entries = best->snapshot.entries;
  }
  for (std::size_t i = 0; i < updates.size(); ++i) {
    const OffsetEntry& update = updates[i];
    std::vector<OffsetEntry>::iterator it = std::lower_bound(
        merged.entries.begin(), merged.entries.end(), update.partition,
        [](const OffsetEntry& entry, std::uint32_t partition) {
          return entry.partition < partition;
        });
    if (it != merged.entries.end() && it->partition == update.partition) {
      if (update.committed_offset > it->committed_offset) {
        it->committed_offset = update.committed_offset;
      }
    } else {
      merged.entries.insert(it, update);
    }
  }

  std::string bytes;
  const Status encode_status = EncodeOffsetSnapshot(merged, &bytes);
  if (!encode_status.ok()) {
    return encode_status;
  }
  const Status directory_status = internal::EnsureDirectory(directory_);
  if (!directory_status.ok()) {
    return directory_status;
  }
  const Status write_status =
      internal::WriteFileAtomically(target_path, tmp_path_, bytes);
  if (!write_status.ok()) {
    return write_status;
  }

  *resulting = std::move(merged);
  return Status();
}

}  // namespace storage
}  // namespace minikafka
