#ifndef MINIKAFKA_PROTOCOL_CODEC_H_
#define MINIKAFKA_PROTOCOL_CODEC_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "minikafka/common/status.h"

namespace minikafka {
namespace protocol {

using common::Status;
using common::StatusCode;

// 05 §3.1：诊断 message 为 UTF-8，最长 512 字节。
constexpr std::size_t kMaxMessageBytes = 512u;

// 05 §2.3 编码原语：u8/u16/u32/u64、str/blob（u32 len + bytes）、array（u32 count）。
// 所有整数大端；写入失败时调用方丢弃已构造的 payload。
class Encoder {
 public:
  explicit Encoder(std::string& out) : out_(out) {}

  Status WriteU8(std::uint8_t value);
  Status WriteU16(std::uint16_t value);
  Status WriteU32(std::uint32_t value);
  Status WriteU64(std::uint64_t value);

  Status WriteBytes(const void* data, std::size_t size);
  Status WriteBlob(const std::string& blob);
  Status WriteUtf8Blob(const std::string& blob);
  Status WriteStr(const std::string& text);
  Status WriteMessage(const std::string& message);

 private:
  std::string& out_;
};

// 严格解码：字段不足、长度越界、尾随字节 -> payload_decode_error；
// 非法 UTF-8 -> invalid_utf8。不得按不可信长度直接分配。
class Decoder {
 public:
  explicit Decoder(const std::string& data);

  Status ReadU8(std::uint8_t* value);
  Status ReadU16(std::uint16_t* value);
  Status ReadU32(std::uint32_t* value);
  Status ReadU64(std::uint64_t* value);

  Status ReadBlob(std::string* out);
  Status ReadUtf8Blob(std::string* out);
  Status ReadStr(std::string* out);
  Status ReadMessage(std::string* out);

  Status Finish() const;
  std::size_t remaining() const { return size_ - pos_; }

 private:
  Status Need(std::size_t count) const;
  Status ReadLengthAndBytes(std::size_t max_length, bool validate_utf8,
                            std::string* out);

  const unsigned char* data_;
  std::size_t size_;
  std::size_t pos_;
};

}  // namespace protocol
}  // namespace minikafka

#endif  // MINIKAFKA_PROTOCOL_CODEC_H_
