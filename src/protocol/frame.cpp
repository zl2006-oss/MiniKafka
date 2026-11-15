#include "minikafka/protocol/frame.h"

#include <utility>

#include "minikafka/common/crc32.h"
#include "minikafka/protocol/codec.h"

namespace minikafka {
namespace protocol {
namespace {

std::uint16_t ReadBe16(const unsigned char* data) {
  return static_cast<std::uint16_t>(
      (static_cast<std::uint16_t>(data[0]) << 8) |
      static_cast<std::uint16_t>(data[1]));
}

std::uint32_t ReadBe32(const unsigned char* data) {
  return (static_cast<std::uint32_t>(data[0]) << 24) |
         (static_cast<std::uint32_t>(data[1]) << 16) |
         (static_cast<std::uint32_t>(data[2]) << 8) |
         static_cast<std::uint32_t>(data[3]);
}

std::uint64_t ReadBe64(const unsigned char* data) {
  std::uint64_t value = 0;
  for (std::size_t i = 0; i < 8u; ++i) {
    value = (value << 8) | static_cast<std::uint64_t>(data[i]);
  }
  return value;
}

}  // namespace

Status EncodeFrame(const Frame& frame, std::string* bytes) {
  if (bytes == nullptr) {
    return Status(StatusCode::kInternalError, "null frame output");
  }
  if (frame.payload.size() > static_cast<std::size_t>(kMaxFramePayload)) {
    return Status(StatusCode::kRequestTooLarge,
                  "frame payload exceeds 8 MiB");
  }

  std::string out;
  out.reserve(kFrameOverhead + frame.payload.size());

  Encoder encoder(out);
  encoder.WriteU32(kFrameMagic);
  encoder.WriteU16(kFrameVersion);
  encoder.WriteU16(frame.msg_type);
  encoder.WriteU64(frame.request_id);
  encoder.WriteU32(static_cast<std::uint32_t>(frame.payload.size()));
  encoder.WriteBytes(frame.payload.data(), frame.payload.size());

  const std::uint32_t crc = common::Crc32(out);
  encoder.WriteU32(crc);

  *bytes = std::move(out);
  return Status();
}

FrameDecoder::FrameDecoder()
    : buffer_(), read_pos_(0), failed_(false), failure_() {}

Status FrameDecoder::Feed(const void* data, std::size_t size) {
  if (failed_) {
    return failure_;
  }
  if (size == 0) {
    return Status();
  }
  if (data == nullptr) {
    return Fail(Status(StatusCode::kInternalError, "null frame input"));
  }
  buffer_.append(static_cast<const char*>(data), size);
  return Status();
}

Status FrameDecoder::Feed(const std::string& bytes) {
  return Feed(bytes.data(), bytes.size());
}

Status FrameDecoder::Next(Frame* frame, bool* has_frame) {
  if (frame == nullptr || has_frame == nullptr) {
    return Status(StatusCode::kInternalError, "null frame output");
  }
  *has_frame = false;
  if (failed_) {
    return failure_;
  }

  const std::size_t available = buffer_.size() - read_pos_;
  if (available < kFrameHeaderSize) {
    return Status();
  }

  const unsigned char* data =
      reinterpret_cast<const unsigned char*>(buffer_.data()) + read_pos_;
  const std::uint32_t magic = ReadBe32(data);
  if (magic != kFrameMagic) {
    return Fail(Status(StatusCode::kInvalidFrame, "invalid frame magic"));
  }

  const std::uint16_t version = ReadBe16(data + 4u);
  if (version != kFrameVersion) {
    return Fail(Status(StatusCode::kUnsupportedVersion,
                       "unsupported frame version"));
  }

  const std::uint32_t payload_len = ReadBe32(data + 16u);
  if (payload_len > kMaxFramePayload) {
    return Fail(Status(StatusCode::kRequestTooLarge,
                       "payload_len exceeds 8 MiB"));
  }

  const std::size_t frame_size =
      kFrameOverhead + static_cast<std::size_t>(payload_len);
  if (available < frame_size) {
    return Status();
  }

  const std::uint32_t expected_crc =
      ReadBe32(data + kFrameHeaderSize + payload_len);
  const std::uint32_t actual_crc =
      common::Crc32(data, kFrameHeaderSize + payload_len);
  if (expected_crc != actual_crc) {
    return Fail(Status(StatusCode::kFrameCrcError, "frame CRC mismatch"));
  }

  frame->msg_type = ReadBe16(data + 6u);
  frame->request_id = ReadBe64(data + 8u);
  if (payload_len == 0u) {
    frame->payload.clear();
  } else {
    frame->payload.assign(
        reinterpret_cast<const char*>(data + kFrameHeaderSize), payload_len);
  }

  read_pos_ += frame_size;
  Compact();
  *has_frame = true;
  return Status();
}

void FrameDecoder::Reset() {
  buffer_.clear();
  read_pos_ = 0;
  failed_ = false;
  failure_ = Status();
}

Status FrameDecoder::Fail(Status status) {
  failed_ = true;
  failure_ = status;
  buffer_.clear();
  read_pos_ = 0;
  return status;
}

void FrameDecoder::Compact() {
  if (read_pos_ == buffer_.size()) {
    buffer_.clear();
    read_pos_ = 0;
  } else if (read_pos_ >= 64u * 1024u) {
    buffer_.erase(0, read_pos_);
    read_pos_ = 0;
  }
}

}  // namespace protocol
}  // namespace minikafka
