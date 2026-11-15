#include "minikafka/storage/record.h"

#include <cstring>
#include <utility>

#include "minikafka/common/crc32.h"
#include "minikafka/common/json.h"

namespace minikafka {
namespace storage {

using common::Status;
using common::StatusCode;

namespace {

void WriteU16(std::string* out, std::size_t offset, std::uint16_t value) {
  (*out)[offset] = static_cast<char>((value >> 8) & 0xFFu);
  (*out)[offset + 1u] = static_cast<char>(value & 0xFFu);
}

void WriteU32(std::string* out, std::size_t offset, std::uint32_t value) {
  (*out)[offset] = static_cast<char>((value >> 24) & 0xFFu);
  (*out)[offset + 1u] = static_cast<char>((value >> 16) & 0xFFu);
  (*out)[offset + 2u] = static_cast<char>((value >> 8) & 0xFFu);
  (*out)[offset + 3u] = static_cast<char>(value & 0xFFu);
}

std::uint16_t ReadU16(const unsigned char* data) {
  return static_cast<std::uint16_t>(
      (static_cast<std::uint16_t>(data[0]) << 8) |
      static_cast<std::uint16_t>(data[1]));
}

std::uint32_t ReadU32(const unsigned char* data) {
  return (static_cast<std::uint32_t>(data[0]) << 24) |
         (static_cast<std::uint32_t>(data[1]) << 16) |
         (static_cast<std::uint32_t>(data[2]) << 8) |
         static_cast<std::uint32_t>(data[3]);
}

RecordDecodeOutcome Corrupt(StatusCode code, const std::string& message) {
  RecordDecodeOutcome outcome;
  outcome.result = RecordDecodeResult::kCorrupt;
  outcome.status = Status(code, message);
  return outcome;
}

RecordDecodeOutcome NeedMore() { return RecordDecodeOutcome(); }

}  // namespace

std::uint32_t RecordCrc32(const void* data, std::size_t record_size) {
  if (data == nullptr || record_size < 8u) {
    return 0u;
  }
  const unsigned char* bytes = static_cast<const unsigned char*>(data);
  std::uint32_t state = common::kCrc32Init;
  state = common::Crc32Update(state, bytes, 4u);
  state = common::Crc32Update(state, bytes + 8u, record_size - 8u);
  return common::Crc32Finalize(state);
}

Status EncodeRecord(const Record& record, std::string* bytes) {
  if (bytes == nullptr) {
    return Status(StatusCode::kInternalError, "null record output");
  }
  if ((record.flags & 0xFFFEu) != 0u) {
    return Status(StatusCode::kInvalidFrame,
                  "record flags reserved bits must be zero");
  }
  if ((record.flags & kFlagKeyPresent) == 0 && !record.key.empty()) {
    return Status(StatusCode::kInvalidFrame,
                  "key present flag is required for non-empty key");
  }
  if (!common::IsValidUtf8(record.key)) {
    return Status(StatusCode::kInvalidUtf8, "record key is not valid UTF-8");
  }
  if (!common::IsValidUtf8(record.value)) {
    return Status(StatusCode::kInvalidUtf8,
                  "record value is not valid UTF-8");
  }
  if (record.key.size() > kMaxRecordSize ||
      record.value.size() > kMaxRecordSize ||
      record.key.size() + record.value.size() >
          kMaxRecordSize - kRecordHeaderSize) {
    return Status(StatusCode::kMessageTooLarge,
                  "record_size exceeds 1 MiB");
  }

  const std::size_t record_size =
      kRecordHeaderSize + record.key.size() + record.value.size();
  const std::uint32_t total_len =
      static_cast<std::uint32_t>(record_size - 4u);

  std::string buffer(record_size, '\0');
  WriteU32(&buffer, 0u, total_len);
  WriteU32(&buffer, 8u, kRecordMagic);
  WriteU16(&buffer, 12u, kRecordVersion);
  WriteU16(&buffer, 14u, record.flags);
  WriteU32(&buffer, 16u, static_cast<std::uint32_t>(record.key.size()));
  WriteU32(&buffer, 20u, static_cast<std::uint32_t>(record.value.size()));
  buffer.replace(kRecordHeaderSize, record.key.size(), record.key);
  buffer.replace(kRecordHeaderSize + record.key.size(), record.value.size(),
                 record.value);
  WriteU32(&buffer, 4u, RecordCrc32(buffer.data(), buffer.size()));

  *bytes = std::move(buffer);
  return Status();
}

RecordDecodeOutcome DecodeRecord(const void* data, std::size_t size) {
  if (size < 4u) {
    return NeedMore();
  }
  if (data == nullptr) {
    return Corrupt(StatusCode::kInternalError, "null record input");
  }

  const unsigned char* bytes = static_cast<const unsigned char*>(data);
  const std::uint32_t total_len = ReadU32(bytes);
  if (total_len < 20u) {
    return Corrupt(StatusCode::kInvalidFrame, "record total_len is too small");
  }

  const std::uint64_t record_size64 =
      4ull + static_cast<std::uint64_t>(total_len);
  if (record_size64 > static_cast<std::uint64_t>(kMaxRecordSize)) {
    return Corrupt(StatusCode::kMessageTooLarge,
                   "record_size exceeds 1 MiB");
  }
  const std::size_t record_size = static_cast<std::size_t>(record_size64);
  if (size < record_size) {
    return NeedMore();
  }

  const std::uint32_t magic = ReadU32(bytes + 8u);
  if (magic != kRecordMagic) {
    return Corrupt(StatusCode::kInvalidFrame, "invalid record magic");
  }
  const std::uint16_t version = ReadU16(bytes + 12u);
  if (version != kRecordVersion) {
    return Corrupt(StatusCode::kUnsupportedVersion,
                   "unsupported record version");
  }
  const std::uint16_t flags = ReadU16(bytes + 14u);
  if ((flags & 0xFFFEu) != 0u) {
    return Corrupt(StatusCode::kInvalidFrame,
                   "record flags reserved bits must be zero");
  }

  const std::uint32_t key_len = ReadU32(bytes + 16u);
  const std::uint32_t value_len = ReadU32(bytes + 20u);
  const std::uint64_t expected_total =
      20ull + static_cast<std::uint64_t>(key_len) +
      static_cast<std::uint64_t>(value_len);
  if (expected_total != static_cast<std::uint64_t>(total_len)) {
    return Corrupt(StatusCode::kPayloadDecodeError,
                   "record length fields are inconsistent");
  }
  const std::uint64_t expected_record_size =
      static_cast<std::uint64_t>(kRecordHeaderSize) +
      static_cast<std::uint64_t>(key_len) +
      static_cast<std::uint64_t>(value_len);
  if (expected_record_size != static_cast<std::uint64_t>(record_size)) {
    return Corrupt(StatusCode::kPayloadDecodeError,
                   "record size is inconsistent");
  }
  if ((flags & kFlagKeyPresent) == 0 && key_len != 0u) {
    return Corrupt(StatusCode::kPayloadDecodeError,
                   "key present flag is inconsistent with key_len");
  }

  const std::uint32_t stored_crc = ReadU32(bytes + 4u);
  if (RecordCrc32(bytes, record_size) != stored_crc) {
    return Corrupt(StatusCode::kFrameCrcError, "record CRC mismatch");
  }

  Record record;
  record.flags = flags;
  if (key_len != 0u) {
    record.key.assign(
        reinterpret_cast<const char*>(bytes + kRecordHeaderSize), key_len);
  }
  if (value_len != 0u) {
    record.value.assign(
        reinterpret_cast<const char*>(bytes + kRecordHeaderSize + key_len),
        value_len);
  }
  if (!common::IsValidUtf8(record.key)) {
    return Corrupt(StatusCode::kInvalidUtf8, "record key is not valid UTF-8");
  }
  if (!common::IsValidUtf8(record.value)) {
    return Corrupt(StatusCode::kInvalidUtf8,
                   "record value is not valid UTF-8");
  }

  RecordDecodeOutcome outcome;
  outcome.result = RecordDecodeResult::kOk;
  outcome.record = std::move(record);
  outcome.consumed = record_size;
  return outcome;
}

}  // namespace storage
}  // namespace minikafka
