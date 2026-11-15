#include "minikafka/broker/partition_manager.h"

#include <atomic>
#include <utility>

#include "minikafka/common/hash.h"
#include "minikafka/storage/paths.h"
#include "minikafka/storage/recovery.h"

namespace minikafka {
namespace broker {

using common::Status;
using common::StatusCode;
using common::StatusOr;
using storage::PartitionStore;

std::uint32_t PartitionKeyFor(const std::string& topic,
                              std::uint32_t partition) {
  // 黄金比例常数做分区号散列，避免 partition 连续时低位相同导致聚集。
  const std::uint32_t mixed =
      partition * 0x9E3779B9u + 0x7F4A7C15u;
  return common::Fnv1a32(topic) ^ mixed;
}

PartitionManager::PartitionManager(std::string data_dir,
                                   std::uint64_t segment_bytes,
                                   std::uint64_t disk_threshold_bytes,
                                   std::size_t worker_count)
    : data_dir_(std::move(data_dir)),
      segment_bytes_(segment_bytes),
      disk_threshold_bytes_(disk_threshold_bytes),
      pool_(worker_count),
      topics_(std::make_shared<const TopicTable>()) {
  workers_.reserve(pool_.worker_count());
  for (std::size_t i = 0; i < pool_.worker_count(); ++i) {
    workers_.emplace_back(i);
  }
}

std::shared_ptr<const PartitionManager::TopicTable>
PartitionManager::Snapshot() const {
  return std::atomic_load(&topics_);
}

void PartitionManager::PublishTopic(const std::string& topic,
                                    std::uint32_t partition_count) {
  std::lock_guard<std::mutex> guard(publish_mutex_);
  std::shared_ptr<TopicTable> next =
      std::make_shared<TopicTable>(*std::atomic_load(&topics_));
  (*next)[topic] = partition_count;
  std::atomic_store(&topics_,
                    std::shared_ptr<const TopicTable>(std::move(next)));
}

void PartitionManager::AddWarning(const std::string& warning) {
  std::lock_guard<std::mutex> guard(publish_mutex_);
  warnings_.push_back(warning);
}

std::vector<std::string> PartitionManager::warnings() const {
  std::lock_guard<std::mutex> guard(publish_mutex_);
  return warnings_;
}

std::size_t PartitionManager::WorkerIndexFor(
    std::uint32_t partition_key) const {
  return pool_.WorkerIndexFor(partition_key);
}

storage::StorageWorker& PartitionManager::worker(std::size_t index) {
  return workers_[index];
}

const storage::StorageWorker& PartitionManager::worker(
    std::size_t index) const {
  return workers_[index];
}

common::Status PartitionManager::OpenTopic(const std::string& topic,
                                           std::uint32_t partition_count) {
  for (std::uint32_t partition = 0; partition < partition_count; ++partition) {
    const Status status = OpenPartition(topic, partition, partition_count);
    if (!status.ok()) {
      return status;
    }
  }
  PublishTopicCount(topic, partition_count);
  return Status();
}

void PartitionManager::PublishTopicCount(const std::string& topic,
                                         std::uint32_t partition_count) {
  PublishTopic(topic, partition_count);
}

common::Status PartitionManager::OpenPartition(
    const std::string& topic, std::uint32_t partition,
    std::uint32_t partition_count) {
  const std::uint32_t key = PartitionKeyFor(topic, partition);
  const std::size_t index = pool_.WorkerIndexFor(key);
  if (workers_[index].Find(key) != nullptr) {
    return Status();  // 幂等：已打开的分区不重复打开
  }

  std::string directory;
  const Status path_status = storage::PartitionDirectory(
      data_dir_, topic, partition, partition_count, &directory);
  if (!path_status.ok()) {
    return path_status;
  }

  StatusOr<PartitionStore> opened =
      PartitionStore::Open(directory, segment_bytes_, disk_threshold_bytes_);
  if (!opened.ok()) {
    AddWarning("partition open failed: " + topic + "/" +
               std::to_string(partition) + ": " + opened.status().message());
    return Status();
  }
  if (!opened.value().ready()) {
    AddWarning("partition unavailable: " + topic + "/" +
               std::to_string(partition) + ": " + opened.value().warning());
  }
  std::unique_ptr<PartitionStore> owner(
      new PartitionStore(std::move(opened).value()));
  workers_[index].AddPartition(key, std::move(owner));
  return Status();
}

PartitionStore* PartitionManager::Find(const std::string& topic,
                                       std::uint32_t partition) const {
  const std::shared_ptr<const TopicTable> table = Snapshot();
  const TopicTable::const_iterator it = table->find(topic);
  if (it == table->end() || partition >= it->second) {
    return nullptr;
  }
  const std::uint32_t key = PartitionKeyFor(topic, partition);
  const std::size_t index = pool_.WorkerIndexFor(key);
  return workers_[index].Find(key);
}

common::StatusOr<PartitionStore*> PartitionManager::Resolve(
    const std::string& topic, std::uint32_t partition) const {
  const std::shared_ptr<const TopicTable> table = Snapshot();
  const TopicTable::const_iterator it = table->find(topic);
  if (it == table->end()) {
    return StatusOr<PartitionStore*>(
        Status(StatusCode::kTopicNotFound, "topic not found: " + topic));
  }
  if (partition >= it->second) {
    return StatusOr<PartitionStore*>(Status(
        StatusCode::kInvalidPartition,
        "partition out of range: " + std::to_string(partition) + " >= " +
            std::to_string(it->second)));
  }
  PartitionStore* store = Find(topic, partition);
  if (store == nullptr) {
    return StatusOr<PartitionStore*>(Status(
        StatusCode::kStorageUnavailable,
        "partition is not open: " + topic + "/" + std::to_string(partition)));
  }
  return StatusOr<PartitionStore*>(store);
}

bool PartitionManager::Knows(const std::string& topic) const {
  const std::shared_ptr<const TopicTable> table = Snapshot();
  return table->find(topic) != table->end();
}

std::size_t PartitionManager::topic_count() const {
  return Snapshot()->size();
}

std::size_t PartitionManager::ready_partitions() const {
  std::size_t count = 0;
  for (std::size_t i = 0; i < workers_.size(); ++i) {
    const std::vector<std::uint32_t>& keys = workers_[i].partitions();
    for (std::size_t k = 0; k < keys.size(); ++k) {
      const PartitionStore* store = workers_[i].Find(keys[k]);
      if (store != nullptr && store->ready()) {
        ++count;
      }
    }
  }
  return count;
}

std::size_t PartitionManager::unavailable_partitions() const {
  std::size_t count = 0;
  for (std::size_t i = 0; i < workers_.size(); ++i) {
    const std::vector<std::uint32_t>& keys = workers_[i].partitions();
    for (std::size_t k = 0; k < keys.size(); ++k) {
      const PartitionStore* store = workers_[i].Find(keys[k]);
      if (store != nullptr && !store->ready()) {
        ++count;
      }
    }
  }
  return count;
}

}  // namespace broker
}  // namespace minikafka
