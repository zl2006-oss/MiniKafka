#include "minikafka/common/hash.h"

namespace minikafka {
namespace common {
namespace {

const std::uint32_t kFnvOffsetBasis = 2166136261u;
const std::uint32_t kFnvPrime = 16777619u;

}  // namespace

std::uint32_t Fnv1a32(const void* data, std::size_t size) {
  const unsigned char* bytes = static_cast<const unsigned char*>(data);
  std::uint32_t hash = kFnvOffsetBasis;
  for (std::size_t i = 0; i < size; ++i) {
    hash ^= static_cast<std::uint32_t>(bytes[i]);
    hash *= kFnvPrime;
  }
  return hash;
}

std::uint32_t Fnv1a32(const std::string& bytes) {
  return Fnv1a32(bytes.data(), bytes.size());
}

std::uint32_t PartitionForKey(const std::string& key,
                              std::uint32_t partition_count) {
  if (partition_count == 0) {
    return 0;
  }
  return Fnv1a32(key) % partition_count;
}

}  // namespace common
}  // namespace minikafka
