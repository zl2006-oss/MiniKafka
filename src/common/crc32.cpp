#include "minikafka/common/crc32.h"

#include <array>

namespace minikafka {
namespace common {
namespace {

const std::array<std::uint32_t, 256>& Crc32Table() {
  static const std::array<std::uint32_t, 256> table = []() {
    std::array<std::uint32_t, 256> values = {};
    for (std::uint32_t i = 0; i < 256u; ++i) {
      std::uint32_t value = i;
      for (int bit = 0; bit < 8; ++bit) {
        if ((value & 1u) != 0u) {
          value = 0xEDB88320u ^ (value >> 1);
        } else {
          value >>= 1;
        }
      }
      values[i] = value;
    }
    return values;
  }();
  return table;
}

}  // namespace

std::uint32_t Crc32Update(std::uint32_t state, const void* data,
                         std::size_t size) {
  const unsigned char* bytes = static_cast<const unsigned char*>(data);
  const std::array<std::uint32_t, 256>& table = Crc32Table();
  std::uint32_t value = state;
  for (std::size_t i = 0; i < size; ++i) {
    value = table[(value ^ static_cast<std::uint32_t>(bytes[i])) & 0xFFu] ^
            (value >> 8);
  }
  return value;
}

std::uint32_t Crc32Finalize(std::uint32_t state) {
  return state ^ 0xFFFFFFFFu;
}

std::uint32_t Crc32(const void* data, std::size_t size) {
  return Crc32Finalize(Crc32Update(kCrc32Init, data, size));
}

std::uint32_t Crc32(const std::string& data) {
  return Crc32(data.data(), data.size());
}

}  // namespace common
}  // namespace minikafka
