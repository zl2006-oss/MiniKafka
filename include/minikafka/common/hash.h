#ifndef MINIKAFKA_COMMON_HASH_H_
#define MINIKAFKA_COMMON_HASH_H_

#include <cstddef>
#include <cstdint>
#include <string>

namespace minikafka {
namespace common {

// B1/Q-P5：FNV-1a 32-bit，对 key 的 UTF-8 字节计算，跨进程/跨版本稳定。
std::uint32_t Fnv1a32(const void* data, std::size_t size);
std::uint32_t Fnv1a32(const std::string& bytes);

// partition_count 必须 > 0；为 0 时返回 0（调用方应先做业务校验）。
std::uint32_t PartitionForKey(const std::string& key,
                              std::uint32_t partition_count);

}  // namespace common
}  // namespace minikafka

#endif  // MINIKAFKA_COMMON_HASH_H_
