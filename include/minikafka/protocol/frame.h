#ifndef MINIKAFKA_PROTOCOL_FRAME_H_
#define MINIKAFKA_PROTOCOL_FRAME_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "minikafka/common/status.h"

namespace minikafka {
namespace protocol {

using common::Status;
using common::StatusCode;

// 05 §2：4B magic + 2B version + 2B msg_type + 8B request_id + 4B payload_len
// + payload + 4B CRC。
constexpr std::uint32_t kFrameMagic = 0x4D4B5031u;
constexpr std::uint16_t kFrameVersion = 1u;
constexpr std::size_t kFrameHeaderSize = 20u;
constexpr std::size_t kFrameCrcSize = 4u;
constexpr std::size_t kFrameOverhead = 24u;
constexpr std::uint32_t kMaxFramePayload = 8u * 1024u * 1024u;

struct Frame {
  std::uint16_t msg_type = 0;
  std::uint64_t request_id = 0;
  std::string payload;
};

// 成功时覆盖 *bytes；payload > 8 MiB 返回 request_too_large。
Status EncodeFrame(const Frame& frame, std::string* bytes);

// 增量解码：Feed 追加字节；Next 每次最多产出一帧。
// 半包时返回 ok 且 has_frame=false；错误后解码器进入失败态。
class FrameDecoder {
 public:
  FrameDecoder();

  Status Feed(const void* data, std::size_t size);
  Status Feed(const std::string& bytes);

  // 成功产出帧：返回 ok 且 *has_frame=true；
  // 需要更多字节：返回 ok 且 *has_frame=false；
  // 帧错误：返回对应 Status，*has_frame=false，且后续调用持续返回该错误。
  Status Next(Frame* frame, bool* has_frame);

  std::size_t buffered() const { return buffer_.size() - read_pos_; }
  bool failed() const { return failed_; }

  void Reset();

 private:
  Status Fail(Status status);
  void Compact();

  std::string buffer_;
  std::size_t read_pos_;
  bool failed_;
  Status failure_;
};

}  // namespace protocol
}  // namespace minikafka

#endif  // MINIKAFKA_PROTOCOL_FRAME_H_
