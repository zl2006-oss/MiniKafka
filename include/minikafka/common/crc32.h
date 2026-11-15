#ifndef MINIKAFKA_COMMON_CRC32_H_
#define MINIKAFKA_COMMON_CRC32_H_

#include <cstddef>
#include <cstdint>
#include <string>

namespace minikafka {
namespace common {

// CRC32 IEEE：反射多项式 0xEDB88320，初值/终值异或 0xFFFFFFFF。
// 存储记录与协议帧共用此实现；测试向量 "123456789" -> 0xCBF43926。
constexpr std::uint32_t kCrc32Init = 0xFFFFFFFFu;

// 增量更新；state 为尚未做最终异或的中间值。
std::uint32_t Crc32Update(std::uint32_t state, const void* data, std::size_t size);

// 对中间值做最终异或。
std::uint32_t Crc32Finalize(std::uint32_t state);

std::uint32_t Crc32(const void* data, std::size_t size);
std::uint32_t Crc32(const std::string& data);

}  // namespace common
}  // namespace minikafka

#endif  // MINIKAFKA_COMMON_CRC32_H_
