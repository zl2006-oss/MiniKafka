#ifndef MINIKAFKA_BROKER_TOPIC_REGISTRY_H_
#define MINIKAFKA_BROKER_TOPIC_REGISTRY_H_

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "minikafka/common/status.h"
#include "minikafka/storage/topic_meta.h"

namespace minikafka {
namespace broker {

struct TopicEntry {
  std::string name;
  std::uint32_t partition_count = 0;
};

// R14 / R30 / F-T01–F-T04：Topic 名称与分区数的内存表 + 元数据持久化。
// 分区数创建后不可变，故一旦创建只读；不提供删除与扩容（F-T03）。
// 本类不加锁：调用方（Broker）用一把互斥量串行化 Create/List，
// 原因是创建属于低频管理操作，而读路径走的是 PartitionManager 的无锁快照。
class TopicRegistry {
 public:
  // 扫描 data-dir/meta 重建；单个元数据损坏跳过并记入 warnings（R30）。
  static common::StatusOr<TopicRegistry> Load(const std::string& data_dir);

  // 校验顺序与错误码：名称（kInvalidTopicName）→ 分区数（kInvalidPartitions）
  // → 重复（kTopicExists）→ 持久化失败（存储层错误码）。
  common::Status Create(const std::string& topic, std::uint32_t partition_count);

  bool Contains(const std::string& topic) const;
  // Topic 不存在返回 kTopicNotFound。
  common::StatusOr<std::uint32_t> PartitionCount(const std::string& topic) const;

  // 按名称升序；损坏被跳过的 Topic 不出现在结果中（05 §3.3）。
  std::vector<TopicEntry> List() const;
  std::size_t size() const { return topics_.size(); }

  const std::vector<std::string>& warnings() const { return warnings_; }
  const std::string& data_dir() const { return data_dir_; }

 private:
  TopicRegistry(std::string data_dir, storage::TopicMetadataStore store);

  std::string data_dir_;
  storage::TopicMetadataStore store_;
  std::map<std::string, TopicEntry> topics_;
  std::vector<std::string> warnings_;
};

}  // namespace broker
}  // namespace minikafka

#endif  // MINIKAFKA_BROKER_TOPIC_REGISTRY_H_
