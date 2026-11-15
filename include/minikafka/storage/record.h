#ifndef MINIKAFKA_STORAGE_RECORD_H_
#define MINIKAFKA_STORAGE_RECORD_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "minikafka/common/status.h"

namespace minikafka {
namespace storage {

// 04 §3：24B 固定头 + key + value。
constexpr std::uint32_t kRecordMagic = 0x4D4B4146u;  // "MKAF"
constexpr std::uint16_t kRecordVersion = 1u;
constexpr std::size_t kRecordHeaderSize = 24u;
constexpr std::size_t kMaxRecordSize = 1024u * 1024u;
constexpr std::uint16_t kFlagKeyPresent = 0x0001u;

struct Record {
  std::uint16_t flags = 0;
  std::string key;
  std::string value;
};

enum class RecordDecodeResult {
  kOk,
  kNeedMore,
  kCorrupt,
};

struct RecordDecodeOutcome {
  RecordDecodeResult result = RecordDecodeResult::kNeedMore;
  common::Status status;
  Record record;
  std::size_t consumed = 0;
};

// 严格编码：flags/key 组合、UTF-8 与 1 MiB 上限都会校验。
common::Status EncodeRecord(const Record& record, std::string* bytes);

// 三态解码：kOk / kNeedMore / kCorrupt；kCorrupt 时 status 给出错误码。
// data 至少可读 size 字节；成功时 consumed = record_size（可处理尾部多余字节）。
RecordDecodeOutcome DecodeRecord(const void* data, std::size_t size);

// CRC32 IEEE，覆盖 [0,4) ∪ [8,record_size)；record_size 必须 >= 8。
std::uint32_t RecordCrc32(const void* data, std::size_t record_size);

}  // namespace storage
}  // namespace minikafka

#endif  // MINIKAFKA_STORAGE_RECORD_H_
