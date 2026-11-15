#ifndef MINIKAFKA_STORAGE_PATHS_H_
#define MINIKAFKA_STORAGE_PATHS_H_

#include <cstdint>
#include <string>

#include "minikafka/common/status.h"

namespace minikafka {
namespace storage {

// Topic/Group 名与 Q-P8/R31 完全一致：仅 a-z0-9_-，1..64 个字节。
bool IsValidTopicName(const std::string& name);
bool IsValidGroupName(const std::string& name);

common::Status ValidateTopicName(const std::string& name);
common::Status ValidateGroupName(const std::string& name);
common::Status ValidatePartition(std::uint32_t partition,
                                 std::uint32_t partition_count);

// 20 位十进制、左侧补零；用于分段文件名。
std::string FormatBaseOffset(std::uint64_t base_offset);

// 以下路径函数返回不带尾部 '/' 的规范路径；失败时不修改 *out。
common::Status TopicDirectory(const std::string& data_dir,
                              const std::string& topic, std::string* out);
common::Status PartitionDirectory(const std::string& data_dir,
                                  const std::string& topic,
                                  std::uint32_t partition,
                                  std::uint32_t partition_count,
                                  std::string* out);
common::Status MetaPath(const std::string& data_dir, const std::string& topic,
                        std::string* out);
common::Status GroupTopicDirectory(const std::string& data_dir,
                                   const std::string& group,
                                   const std::string& topic, std::string* out);
common::Status SegmentLogPath(const std::string& data_dir,
                              const std::string& topic,
                              std::uint32_t partition,
                              std::uint32_t partition_count,
                              std::uint64_t base_offset, std::string* out);
common::Status SegmentIndexPath(const std::string& data_dir,
                                const std::string& topic,
                                std::uint32_t partition,
                                std::uint32_t partition_count,
                                std::uint64_t base_offset, std::string* out);

}  // namespace storage
}  // namespace minikafka

#endif  // MINIKAFKA_STORAGE_PATHS_H_
