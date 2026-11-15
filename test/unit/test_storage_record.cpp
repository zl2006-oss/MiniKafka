#include "test_common.h"

#include <cstddef>
#include <cstdint>
#include <string>

#include "minikafka/common/status.h"
#include "minikafka/storage/record.h"

namespace {

using minikafka::common::Status;
using minikafka::common::StatusCode;
using minikafka::storage::DecodeRecord;
using minikafka::storage::EncodeRecord;
using minikafka::storage::kFlagKeyPresent;
using minikafka::storage::kMaxRecordSize;
using minikafka::storage::kRecordHeaderSize;
using minikafka::storage::Record;
using minikafka::storage::RecordCrc32;
using minikafka::storage::RecordDecodeOutcome;
using minikafka::storage::RecordDecodeResult;

void ExpectStatus(const Status& status, StatusCode code) {
  MK_EXPECT_FALSE(status.ok());
  MK_EXPECT_EQ(status.code(), code);
}

void ExpectCorrupt(const RecordDecodeOutcome& outcome, StatusCode code) {
  MK_EXPECT_EQ(outcome.result, RecordDecodeResult::kCorrupt);
  MK_EXPECT_FALSE(outcome.status.ok());
  MK_EXPECT_EQ(outcome.status.code(), code);
}

void ExpectNeedMore(const RecordDecodeOutcome& outcome) {
  MK_EXPECT_EQ(outcome.result, RecordDecodeResult::kNeedMore);
  MK_EXPECT_TRUE(outcome.status.ok());
}

void SetU16(std::string* bytes, std::size_t offset, std::uint16_t value) {
  (*bytes)[offset] = static_cast<char>((value >> 8) & 0xFFu);
  (*bytes)[offset + 1u] = static_cast<char>(value & 0xFFu);
}

void SetU32(std::string* bytes, std::size_t offset, std::uint32_t value) {
  (*bytes)[offset] = static_cast<char>((value >> 24) & 0xFFu);
  (*bytes)[offset + 1u] = static_cast<char>((value >> 16) & 0xFFu);
  (*bytes)[offset + 2u] = static_cast<char>((value >> 8) & 0xFFu);
  (*bytes)[offset + 3u] = static_cast<char>(value & 0xFFu);
}

std::uint32_t ReadU32(const std::string& bytes, std::size_t offset) {
  return (static_cast<std::uint32_t>(
              static_cast<unsigned char>(bytes[offset])) << 24) |
         (static_cast<std::uint32_t>(
              static_cast<unsigned char>(bytes[offset + 1u])) << 16) |
         (static_cast<std::uint32_t>(
              static_cast<unsigned char>(bytes[offset + 2u])) << 8) |
         static_cast<std::uint32_t>(
             static_cast<unsigned char>(bytes[offset + 3u]));
}

void RefreshCrc(std::string* bytes) {
  SetU32(bytes, 4u, RecordCrc32(bytes->data(), bytes->size()));
}

std::string EncodeOrFail(const Record& record) {
  std::string bytes;
  const Status status = EncodeRecord(record, &bytes);
  MK_EXPECT_TRUE(status.ok());
  return bytes;
}

}  // namespace

void TestStorageRecord() {
  {
    Record record;
    record.flags = kFlagKeyPresent;
    record.key = "k";
    record.value = "v";
    const std::string bytes = EncodeOrFail(record);
    MK_EXPECT_EQ(bytes.size(), static_cast<std::size_t>(26));

    const RecordDecodeOutcome outcome =
        DecodeRecord(bytes.data(), bytes.size());
    MK_EXPECT_EQ(outcome.result, RecordDecodeResult::kOk);
    MK_EXPECT_TRUE(outcome.status.ok());
    MK_EXPECT_EQ(outcome.consumed, bytes.size());
    MK_EXPECT_EQ(outcome.record.flags, kFlagKeyPresent);
    MK_EXPECT_EQ(outcome.record.key, std::string("k"));
    MK_EXPECT_EQ(outcome.record.value, std::string("v"));
  }

  {
    Record record;
    record.flags = kFlagKeyPresent;
    record.key = "";
    record.value = "v";
    const std::string bytes = EncodeOrFail(record);
    const RecordDecodeOutcome outcome =
        DecodeRecord(bytes.data(), bytes.size());
    MK_EXPECT_EQ(outcome.result, RecordDecodeResult::kOk);
    MK_EXPECT_EQ(outcome.record.key, std::string());
    MK_EXPECT_EQ(outcome.record.value, std::string("v"));
  }

  {
    Record record;
    record.flags = kFlagKeyPresent;
    record.key = "k";
    record.value = "";
    const std::string bytes = EncodeOrFail(record);
    const RecordDecodeOutcome outcome =
        DecodeRecord(bytes.data(), bytes.size());
    MK_EXPECT_EQ(outcome.result, RecordDecodeResult::kOk);
    MK_EXPECT_EQ(outcome.record.key, std::string("k"));
    MK_EXPECT_EQ(outcome.record.value, std::string());
  }

  {
    Record record;
    record.flags = 0u;
    record.key = "";
    record.value = "";
    const std::string bytes = EncodeOrFail(record);
    MK_EXPECT_EQ(bytes.size(), kRecordHeaderSize);
    const RecordDecodeOutcome outcome =
        DecodeRecord(bytes.data(), bytes.size());
    MK_EXPECT_EQ(outcome.result, RecordDecodeResult::kOk);
    MK_EXPECT_EQ(outcome.record.flags, 0);
    MK_EXPECT_EQ(outcome.record.key, std::string());
    MK_EXPECT_EQ(outcome.record.value, std::string());
  }

  {
    const std::string max_value(kMaxRecordSize - kRecordHeaderSize, 'a');
    Record record;
    record.flags = kFlagKeyPresent;
    record.value = max_value;
    const std::string bytes = EncodeOrFail(record);
    MK_EXPECT_EQ(bytes.size(), kMaxRecordSize);
    const RecordDecodeOutcome outcome =
        DecodeRecord(bytes.data(), bytes.size());
    MK_EXPECT_EQ(outcome.result, RecordDecodeResult::kOk);
    MK_EXPECT_EQ(outcome.consumed, kMaxRecordSize);
    MK_EXPECT_EQ(outcome.record.value.size(), max_value.size());
  }

  {
    Record record;
    record.flags = kFlagKeyPresent;
    record.value.assign(kMaxRecordSize - kRecordHeaderSize + 1u, 'a');
    std::string bytes;
    ExpectStatus(EncodeRecord(record, &bytes),
                 StatusCode::kMessageTooLarge);
  }

  {
    std::string raw(4u, '\0');
    SetU32(&raw, 0u, static_cast<std::uint32_t>(kMaxRecordSize - 3u));
    ExpectCorrupt(DecodeRecord(raw.data(), raw.size()),
                  StatusCode::kMessageTooLarge);
  }

  {
    std::string raw(4u, '\0');
    SetU32(&raw, 0u, 19u);
    ExpectCorrupt(DecodeRecord(raw.data(), raw.size()),
                  StatusCode::kInvalidFrame);
  }

  {
    Record record;
    record.flags = kFlagKeyPresent;
    record.key = "k";
    record.value = "v";
    const std::string bytes = EncodeOrFail(record);
    for (std::size_t cut = 0; cut < bytes.size(); ++cut) {
      ExpectNeedMore(DecodeRecord(bytes.data(), cut));
    }
    const RecordDecodeOutcome full =
        DecodeRecord(bytes.data(), bytes.size());
    MK_EXPECT_EQ(full.result, RecordDecodeResult::kOk);

    std::string with_extra = bytes;
    with_extra.push_back('\0');
    const RecordDecodeOutcome extra =
        DecodeRecord(with_extra.data(), with_extra.size());
    MK_EXPECT_EQ(extra.result, RecordDecodeResult::kOk);
    MK_EXPECT_EQ(extra.consumed, bytes.size());
  }

  {
    Record record;
    record.flags = kFlagKeyPresent;
    record.key = "k";
    record.value = "v";
    const std::string bytes = EncodeOrFail(record);
    const std::uint32_t crc = RecordCrc32(bytes.data(), bytes.size());
    MK_EXPECT_EQ(ReadU32(bytes, 4u), crc);

    std::string crc_changed = bytes;
    SetU32(&crc_changed, 4u, crc ^ 0xFFFFFFFFu);
    MK_EXPECT_EQ(RecordCrc32(crc_changed.data(), crc_changed.size()), crc);
    ExpectCorrupt(DecodeRecord(crc_changed.data(), crc_changed.size()),
                  StatusCode::kFrameCrcError);

    std::string total_changed = bytes;
    SetU32(&total_changed, 0u, 21u);
    MK_EXPECT_TRUE(RecordCrc32(total_changed.data(), total_changed.size()) !=
                   crc);
    ExpectCorrupt(DecodeRecord(total_changed.data(), total_changed.size()),
                  StatusCode::kPayloadDecodeError);

    std::string payload_changed = bytes;
    payload_changed[25u] = 'x';
    ExpectCorrupt(
        DecodeRecord(payload_changed.data(), payload_changed.size()),
        StatusCode::kFrameCrcError);
  }

  {
    Record record;
    record.flags = kFlagKeyPresent;
    record.key = "k";
    record.value = "v";
    const std::string bytes = EncodeOrFail(record);

    std::string bad_magic = bytes;
    bad_magic[8u] = static_cast<char>(0);
    ExpectCorrupt(DecodeRecord(bad_magic.data(), bad_magic.size()),
                  StatusCode::kInvalidFrame);

    std::string bad_version = bytes;
    SetU16(&bad_version, 12u, 2u);
    ExpectCorrupt(DecodeRecord(bad_version.data(), bad_version.size()),
                  StatusCode::kUnsupportedVersion);
  }

  {
    Record record;
    record.flags = kFlagKeyPresent;
    record.key = "k";
    record.value = "v";
    const std::string bytes = EncodeOrFail(record);

    std::string bad_flags = bytes;
    SetU16(&bad_flags, 14u, 2u);
    RefreshCrc(&bad_flags);
    ExpectCorrupt(DecodeRecord(bad_flags.data(), bad_flags.size()),
                  StatusCode::kInvalidFrame);

    std::string missing_key_flag = bytes;
    SetU16(&missing_key_flag, 14u, 0u);
    RefreshCrc(&missing_key_flag);
    ExpectCorrupt(DecodeRecord(missing_key_flag.data(),
                               missing_key_flag.size()),
                  StatusCode::kPayloadDecodeError);
  }

  {
    Record record;
    record.flags = kFlagKeyPresent;
    record.key = "k";
    record.value = "v";
    const std::string bytes = EncodeOrFail(record);

    std::string bad_value_len = bytes;
    SetU32(&bad_value_len, 20u, 2u);
    ExpectCorrupt(DecodeRecord(bad_value_len.data(), bad_value_len.size()),
                  StatusCode::kPayloadDecodeError);

    std::string bad_key_len = bytes;
    SetU32(&bad_key_len, 16u, 2u);
    ExpectCorrupt(DecodeRecord(bad_key_len.data(), bad_key_len.size()),
                  StatusCode::kPayloadDecodeError);
  }

  {
    Record record;
    record.flags = kFlagKeyPresent;
    record.key.push_back(static_cast<char>(0xC3));
    record.key.push_back('(');
    record.value = "v";
    std::string bytes;
    ExpectStatus(EncodeRecord(record, &bytes), StatusCode::kInvalidUtf8);
  }

  {
    Record record;
    record.flags = kFlagKeyPresent;
    record.key = "k";
    record.value = "v";
    std::string bytes = EncodeOrFail(record);
    bytes[24u] = static_cast<char>(0xC3);
    RefreshCrc(&bytes);
    ExpectCorrupt(DecodeRecord(bytes.data(), bytes.size()),
                  StatusCode::kInvalidUtf8);
  }

  {
    Record record;
    record.flags = 2u;
    record.value = "v";
    std::string bytes;
    ExpectStatus(EncodeRecord(record, &bytes), StatusCode::kInvalidFrame);

    record.flags = 0u;
    record.key = "k";
    ExpectStatus(EncodeRecord(record, &bytes), StatusCode::kInvalidFrame);

    record.flags = kFlagKeyPresent;
    ExpectStatus(EncodeRecord(record, nullptr),
                 StatusCode::kInternalError);
  }

  {
    ExpectNeedMore(DecodeRecord(nullptr, 0u));
    ExpectCorrupt(DecodeRecord(nullptr, 4u), StatusCode::kInternalError);
  }
}
