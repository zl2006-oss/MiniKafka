#include "minikafka/broker/topic_registry.h"

#include <utility>

#include "minikafka/common/defaults.h"
#include "minikafka/storage/paths.h"

namespace minikafka {
namespace broker {

using common::Status;
using common::StatusCode;
using common::StatusOr;

TopicRegistry::TopicRegistry(std::string data_dir,
                             storage::TopicMetadataStore store)
    : data_dir_(std::move(data_dir)), store_(std::move(store)) {}

common::StatusOr<TopicRegistry> TopicRegistry::Load(
    const std::string& data_dir) {
  StatusOr<storage::TopicMetadataStore> store =
      storage::TopicMetadataStore::Open(data_dir);
  if (!store.ok()) {
    return StatusOr<TopicRegistry>(store.status());
  }

  storage::TopicScanResult scanned;
  const Status scan_status = store.value().Scan(&scanned);
  if (!scan_status.ok()) {
    return StatusOr<TopicRegistry>(scan_status);
  }

  TopicRegistry registry(data_dir, std::move(store).value());
  registry.warnings_ = scanned.warnings;
  for (std::size_t i = 0; i < scanned.topics.size(); ++i) {
    TopicEntry entry;
    entry.name = scanned.topics[i].name;
    entry.partition_count = scanned.topics[i].partition_count;
    registry.topics_[entry.name] = entry;
  }
  return StatusOr<TopicRegistry>(std::move(registry));
}

common::Status TopicRegistry::Create(const std::string& topic,
                                     std::uint32_t partition_count) {
  const Status name_status = storage::ValidateTopicName(topic);
  if (!name_status.ok()) {
    return name_status;
  }
  if (partition_count <
          static_cast<std::uint32_t>(common::kMinPartitions) ||
      partition_count >
          static_cast<std::uint32_t>(common::kMaxPartitions)) {
    return Status(StatusCode::kInvalidPartitions,
                  "partition_count must be in 1..16");
  }
  if (topics_.find(topic) != topics_.end()) {
    return Status(StatusCode::kTopicExists, "topic already exists: " + topic);
  }

  storage::TopicMetadata metadata;
  metadata.name = topic;
  metadata.partition_count = partition_count;
  // 先落盘再生效：持久化失败时不留下内存里"存在但重启后消失"的 Topic。
  const Status persist_status = store_.Create(metadata);
  if (!persist_status.ok()) {
    return persist_status;
  }

  TopicEntry entry;
  entry.name = topic;
  entry.partition_count = partition_count;
  topics_[topic] = entry;
  return Status();
}

bool TopicRegistry::Contains(const std::string& topic) const {
  return topics_.find(topic) != topics_.end();
}

common::StatusOr<std::uint32_t> TopicRegistry::PartitionCount(
    const std::string& topic) const {
  const std::map<std::string, TopicEntry>::const_iterator it =
      topics_.find(topic);
  if (it == topics_.end()) {
    return StatusOr<std::uint32_t>(
        Status(StatusCode::kTopicNotFound, "topic not found: " + topic));
  }
  return StatusOr<std::uint32_t>(it->second.partition_count);
}

std::vector<TopicEntry> TopicRegistry::List() const {
  std::vector<TopicEntry> entries;
  entries.reserve(topics_.size());
  for (std::map<std::string, TopicEntry>::const_iterator it = topics_.begin();
       it != topics_.end(); ++it) {
    entries.push_back(it->second);
  }
  return entries;
}

}  // namespace broker
}  // namespace minikafka
