#ifndef MINIKAFKA_STORAGE_TOPIC_META_H_
#define MINIKAFKA_STORAGE_TOPIC_META_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "minikafka/common/status.h"

namespace minikafka {
namespace storage {

// 04 §7.2：meta/<topic>.meta 的固定格式。
constexpr std::uint32_t kTopicMetaMagic = 0x4D4B544Du;  // "MKTM"
constexpr std::uint16_t kTopicMetaVersion = 1u;
constexpr std::size_t kTopicMetaHeaderSize = 16u;
constexpr std::size_t kTopicMetaFooterSize = 4u;

struct TopicMetadata {
  std::string name;
  std::uint32_t partition_count = 0;
};

struct TopicScanResult {
  std::vector<TopicMetadata> topics;
  std::vector<std::string> warnings;
};

common::Status EncodeTopicMetadata(const TopicMetadata& metadata,
                                   std::string* bytes);
common::Status DecodeTopicMetadata(const void* data, std::size_t size,
                                   TopicMetadata* metadata);

// <data-dir>/meta 下 Topic 元数据的创建、读取与启动扫描。
class TopicMetadataStore {
 public:
  static common::StatusOr<TopicMetadataStore> Open(
      const std::string& data_dir);

  // 同名 .meta 已存在（包括损坏文件）返回 kTopicExists，不覆盖。
  common::Status Create(const TopicMetadata& metadata);

  common::StatusOr<TopicMetadata> Load(const std::string& topic) const;

  // 扫描 meta/*.meta，忽略 *.tmp；单个损坏跳过并写入 warnings。
  common::Status Scan(TopicScanResult* result) const;

  const std::string& meta_directory() const { return meta_directory_; }

 private:
  TopicMetadataStore(std::string data_dir, std::string meta_directory);

  std::string data_dir_;
  std::string meta_directory_;
};

}  // namespace storage
}  // namespace minikafka

#endif  // MINIKAFKA_STORAGE_TOPIC_META_H_
