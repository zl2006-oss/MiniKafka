#include "minikafka/protocol/codec.h"

#include "minikafka/common/json.h"

namespace minikafka {
namespace protocol {
namespace {

const std::size_t kMaxU32AsSizeT = static_cast<std::size_t>(0xFFFFFFFFu);

}  // namespace

Status Encoder::WriteU8(std::uint8_t value) {
  out_.push_back(static_cast<char>(value));
  return Status();
}

Status Encoder::WriteU16(std::uint16_t value) {
  out_.push_back(static_cast<char>((value >> 8) & 0xFFu));
  out_.push_back(static_cast<char>(value & 0xFFu));
  return Status();
}

Status Encoder::WriteU32(std::uint32_t value) {
  out_.push_back(static_cast<char>((value >> 24) & 0xFFu));
  out_.push_back(static_cast<char>((value >> 16) & 0xFFu));
  out_.push_back(static_cast<char>((value >> 8) & 0xFFu));
  out_.push_back(static_cast<char>(value & 0xFFu));
  return Status();
}

Status Encoder::WriteU64(std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out_.push_back(static_cast<char>((value >> shift) & 0xFFu));
  }
  return Status();
}

Status Encoder::WriteBytes(const void* data, std::size_t size) {
  if (size == 0) {
    return Status();
  }
  if (data == nullptr) {
    return Status(StatusCode::kInternalError, "null byte source");
  }
  out_.append(static_cast<const char*>(data), size);
  return Status();
}

Status Encoder::WriteBlob(const std::string& blob) {
  if (blob.size() > kMaxU32AsSizeT) {
    return Status(StatusCode::kInternalError, "blob exceeds u32 length");
  }
  Status status = WriteU32(static_cast<std::uint32_t>(blob.size()));
  if (!status.ok()) {
    return status;
  }
  return WriteBytes(blob.data(), blob.size());
}

Status Encoder::WriteUtf8Blob(const std::string& blob) {
  if (!common::IsValidUtf8(blob)) {
    return Status(StatusCode::kInvalidUtf8, "blob is not valid UTF-8");
  }
  return WriteBlob(blob);
}

Status Encoder::WriteStr(const std::string& text) {
  return WriteUtf8Blob(text);
}

Status Encoder::WriteMessage(const std::string& message) {
  if (message.size() > kMaxMessageBytes) {
    return Status(StatusCode::kInternalError,
                  "diagnostic message exceeds 512 bytes");
  }
  return WriteUtf8Blob(message);
}

Decoder::Decoder(const std::string& data)
    : data_(reinterpret_cast<const unsigned char*>(data.data())),
      size_(data.size()),
      pos_(0) {}

Status Decoder::Need(std::size_t count) const {
  if (count > size_ - pos_) {
    return Status(StatusCode::kPayloadDecodeError, "truncated field");
  }
  return Status();
}

Status Decoder::ReadU8(std::uint8_t* value) {
  if (value == nullptr) {
    return Status(StatusCode::kInternalError, "null output");
  }
  Status status = Need(1u);
  if (!status.ok()) {
    return status;
  }
  *value = data_[pos_];
  ++pos_;
  return Status();
}

Status Decoder::ReadU16(std::uint16_t* value) {
  if (value == nullptr) {
    return Status(StatusCode::kInternalError, "null output");
  }
  Status status = Need(2u);
  if (!status.ok()) {
    return status;
  }
  *value = static_cast<std::uint16_t>(
      (static_cast<std::uint16_t>(data_[pos_]) << 8) |
      static_cast<std::uint16_t>(data_[pos_ + 1u]));
  pos_ += 2u;
  return Status();
}

Status Decoder::ReadU32(std::uint32_t* value) {
  if (value == nullptr) {
    return Status(StatusCode::kInternalError, "null output");
  }
  Status status = Need(4u);
  if (!status.ok()) {
    return status;
  }
  *value = (static_cast<std::uint32_t>(data_[pos_]) << 24) |
           (static_cast<std::uint32_t>(data_[pos_ + 1u]) << 16) |
           (static_cast<std::uint32_t>(data_[pos_ + 2u]) << 8) |
           static_cast<std::uint32_t>(data_[pos_ + 3u]);
  pos_ += 4u;
  return Status();
}

Status Decoder::ReadU64(std::uint64_t* value) {
  if (value == nullptr) {
    return Status(StatusCode::kInternalError, "null output");
  }
  Status status = Need(8u);
  if (!status.ok()) {
    return status;
  }
  std::uint64_t parsed = 0;
  for (std::size_t i = 0; i < 8u; ++i) {
    parsed = (parsed << 8) | static_cast<std::uint64_t>(data_[pos_ + i]);
  }
  pos_ += 8u;
  *value = parsed;
  return Status();
}

Status Decoder::ReadLengthAndBytes(std::size_t max_length,
                                   bool validate_utf8,
                                   std::string* out) {
  if (out == nullptr) {
    return Status(StatusCode::kInternalError, "null output");
  }
  std::uint32_t length = 0;
  Status status = ReadU32(&length);
  if (!status.ok()) {
    return status;
  }
  const std::size_t byte_count = static_cast<std::size_t>(length);
  if (byte_count > max_length) {
    return Status(StatusCode::kPayloadDecodeError,
                  "length exceeds field limit");
  }
  if (byte_count > remaining()) {
    return Status(StatusCode::kPayloadDecodeError, "truncated bytes");
  }
  if (byte_count == 0) {
    out->clear();
  } else {
    out->assign(reinterpret_cast<const char*>(data_ + pos_), byte_count);
  }
  pos_ += byte_count;
  if (validate_utf8 && !common::IsValidUtf8(*out)) {
    return Status(StatusCode::kInvalidUtf8, "invalid UTF-8");
  }
  return Status();
}

Status Decoder::ReadBlob(std::string* out) {
  return ReadLengthAndBytes(kMaxU32AsSizeT, false, out);
}

Status Decoder::ReadUtf8Blob(std::string* out) {
  return ReadLengthAndBytes(kMaxU32AsSizeT, true, out);
}

Status Decoder::ReadStr(std::string* out) {
  return ReadLengthAndBytes(kMaxU32AsSizeT, true, out);
}

Status Decoder::ReadMessage(std::string* out) {
  return ReadLengthAndBytes(kMaxMessageBytes, true, out);
}

Status Decoder::Finish() const {
  if (pos_ != size_) {
    return Status(StatusCode::kPayloadDecodeError, "trailing bytes");
  }
  return Status();
}

}  // namespace protocol
}  // namespace minikafka
