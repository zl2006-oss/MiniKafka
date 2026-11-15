#ifndef MINIKAFKA_BROKER_PARTITION_MANAGER_H_
#define MINIKAFKA_BROKER_PARTITION_MANAGER_H_

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "minikafka/common/status.h"
#include "minikafka/storage/storage_worker.h"

namespace minikafka {
namespace broker {

// AD-05 分区亲和键：topic 与 partition 共同决定所属 Storage Worker。
// 只用 partition 会造成"所有 Topic 的 0 号分区挤在同一个 Worker"。
std::uint32_t PartitionKeyFor(const std::string& topic, std::uint32_t partition);

// Topic 分区表 + 分区到 Storage Worker 的归属（AD-05）。
// - 分区数创建后不可变（F-T03），故 topic 表用"写时复制快照 + 原子指针"发布：
//   Create 一侧构新快照，读写一侧无锁读取，热路径不加锁。
// - workers_ 内每个 StorageWorker 只被其所属线程访问（M5b 起），本类不做分区级加锁。
class PartitionManager {
 public:
  PartitionManager(std::string data_dir, std::uint64_t segment_bytes,
                   std::uint64_t disk_threshold_bytes,
                   std::size_t worker_count);

  PartitionManager(const PartitionManager&) = delete;
  PartitionManager& operator=(const PartitionManager&) = delete;

  // 打开 topic 的全部分区（幂等）。单个分区打不开只记 warning，不阻断其余分区（R30）。
  common::Status OpenTopic(const std::string& topic,
                           std::uint32_t partition_count);

  // 校验 topic 是否存在、partition 是否越界，并返回该分区的存储门面。
  // 错误码：kTopicNotFound / kInvalidPartition / kStorageUnavailable。
  common::StatusOr<storage::PartitionStore*> Resolve(
      const std::string& topic, std::uint32_t partition) const;

  // 不做校验的查找，未打开返回 nullptr。
  storage::PartitionStore* Find(const std::string& topic,
                                std::uint32_t partition) const;

  bool Knows(const std::string& topic) const;
  std::size_t topic_count() const;
  std::size_t worker_count() const { return workers_.size(); }
  std::size_t WorkerIndexFor(std::uint32_t partition_key) const;
  storage::StorageWorker& worker(std::size_t index);
  const storage::StorageWorker& worker(std::size_t index) const;

  std::vector<std::string> warnings() const;
  const std::string& data_dir() const { return data_dir_; }

  std::size_t ready_partitions() const;
  std::size_t unavailable_partitions() const;

 private:
  using TopicTable = std::map<std::string, std::uint32_t>;

  void PublishTopic(const std::string& topic, std::uint32_t partition_count);
  void AddWarning(const std::string& warning);
  std::shared_ptr<const TopicTable> Snapshot() const;

  std::string data_dir_;
  std::uint64_t segment_bytes_;
  std::uint64_t disk_threshold_bytes_;
  storage::StorageWorkerPool pool_;
  std::vector<storage::StorageWorker> workers_;

  // 只保护"快照指针替换"与 warnings_；读路径靠原子指针，不取锁。
  mutable std::mutex publish_mutex_;
  std::shared_ptr<const TopicTable> topics_;
  std::vector<std::string> warnings_;
};

}  // namespace broker
}  // namespace minikafka

#endif  // MINIKAFKA_BROKER_PARTITION_MANAGER_H_
