#ifndef MINIKAFKA_TEST_PROTOCOL_UTIL_H_
#define MINIKAFKA_TEST_PROTOCOL_UTIL_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "minikafka/common/status.h"
#include "test_common.h"

namespace mk_protocol_test {

using minikafka::common::Status;
using minikafka::common::StatusCode;

inline void ExpectOk(const Status& status) {
  MK_EXPECT_TRUE(status.ok());
}

inline void ExpectStatus(const Status& status, StatusCode code) {
  MK_EXPECT_FALSE(status.ok());
  MK_EXPECT_EQ(status.code(), code);
}

inline void AppendU16(std::string* out, std::uint16_t value) {
  out->push_back(static_cast<char>((value >> 8) & 0xFFu));
  out->push_back(static_cast<char>(value & 0xFFu));
}

inline void AppendU32(std::string* out, std::uint32_t value) {
  out->push_back(static_cast<char>((value >> 24) & 0xFFu));
  out->push_back(static_cast<char>((value >> 16) & 0xFFu));
  out->push_back(static_cast<char>((value >> 8) & 0xFFu));
  out->push_back(static_cast<char>(value & 0xFFu));
}

inline void AppendU64(std::string* out, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xFFu));
  }
}

inline std::uint16_t ReadU16At(const std::string& data, std::size_t offset) {
  return static_cast<std::uint16_t>(
      (static_cast<std::uint16_t>(
           static_cast<unsigned char>(data[offset])) << 8) |
      static_cast<std::uint16_t>(
          static_cast<unsigned char>(data[offset + 1u])));
}

inline std::uint32_t ReadU32At(const std::string& data, std::size_t offset) {
  return (static_cast<std::uint32_t>(
              static_cast<unsigned char>(data[offset])) << 24) |
         (static_cast<std::uint32_t>(
              static_cast<unsigned char>(data[offset + 1u])) << 16) |
         (static_cast<std::uint32_t>(
              static_cast<unsigned char>(data[offset + 2u])) << 8) |
         static_cast<std::uint32_t>(
             static_cast<unsigned char>(data[offset + 3u]));
}

inline std::uint64_t ReadU64At(const std::string& data, std::size_t offset) {
  std::uint64_t value = 0;
  for (std::size_t i = 0; i < 8u; ++i) {
    value = (value << 8) |
            static_cast<std::uint64_t>(
                static_cast<unsigned char>(data[offset + i]));
  }
  return value;
}

}  // namespace mk_protocol_test

#endif  // MINIKAFKA_TEST_PROTOCOL_UTIL_H_
