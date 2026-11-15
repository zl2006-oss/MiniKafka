#include "minikafka/storage/paths.h"

#include <cstddef>
#include <cstdio>
#include <string>

#include "minikafka/common/defaults.h"

namespace minikafka {
namespace storage {

using common::Status;
using common::StatusCode;

namespace {

constexpr std::size_t kMaxNameLength = 64u;

bool IsValidName(const std::string& name) {
  if (name.empty() || name.size() > kMaxNameLength) {
    return false;
  }
  if (name == "." || name == "..") {
    return false;
  }
  for (char c : name) {
    const bool valid = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
                       c == '-' || c == '_';
    if (!valid) {
      return false;
    }
  }
  return true;
}

std::string JoinPath(const std::string& base, const std::string& child) {
  if (base.empty()) {
    return child;
  }
  if (base.back() == '/') {
    return base + child;
  }
  return base + "/" + child;
}

Status ValidateDataDirectory(const std::string& data_dir) {
  if (data_dir.empty()) {
    return Status(StatusCode::kInternalError, "empty data directory");
  }
  return Status();
}

}  // namespace

bool IsValidTopicName(const std::string& name) { return IsValidName(name); }

bool IsValidGroupName(const std::string& name) { return IsValidName(name); }

Status ValidateTopicName(const std::string& name) {
  if (!IsValidName(name)) {
    return Status(StatusCode::kInvalidTopicName, "invalid topic name");
  }
  return Status();
}

Status ValidateGroupName(const std::string& name) {
  if (!IsValidName(name)) {
    return Status(StatusCode::kInvalidGroupName, "invalid group name");
  }
  return Status();
}

Status ValidatePartition(std::uint32_t partition,
                         std::uint32_t partition_count) {
  if (partition_count < 1u ||
      partition_count > static_cast<std::uint32_t>(common::kMaxPartitions)) {
    return Status(StatusCode::kInvalidPartitions,
                  "partition_count must be in 1..16");
  }
  if (partition >= partition_count) {
    return Status(StatusCode::kInvalidPartition,
                  "partition must be in 0..partition_count-1");
  }
  return Status();
}

std::string FormatBaseOffset(std::uint64_t base_offset) {
  char buffer[32];
  const int written = std::snprintf(
      buffer, sizeof(buffer), "%020llu",
      static_cast<unsigned long long>(base_offset));
  if (written <= 0) {
    return std::string();
  }
  return std::string(buffer, static_cast<std::size_t>(written));
}

Status TopicDirectory(const std::string& data_dir, const std::string& topic,
                      std::string* out) {
  if (out == nullptr) {
    return Status(StatusCode::kInternalError, "null path output");
  }
  Status status = ValidateDataDirectory(data_dir);
  if (!status.ok()) {
    return status;
  }
  status = ValidateTopicName(topic);
  if (!status.ok()) {
    return status;
  }
  *out = JoinPath(JoinPath(data_dir, "topics"), topic);
  return Status();
}

Status PartitionDirectory(const std::string& data_dir,
                          const std::string& topic, std::uint32_t partition,
                          std::uint32_t partition_count, std::string* out) {
  if (out == nullptr) {
    return Status(StatusCode::kInternalError, "null path output");
  }
  Status status = ValidateTopicName(topic);
  if (!status.ok()) {
    return status;
  }
  status = ValidatePartition(partition, partition_count);
  if (!status.ok()) {
    return status;
  }
  std::string topic_dir;
  status = TopicDirectory(data_dir, topic, &topic_dir);
  if (!status.ok()) {
    return status;
  }
  *out = JoinPath(topic_dir, std::to_string(partition));
  return Status();
}

Status MetaPath(const std::string& data_dir, const std::string& topic,
                std::string* out) {
  if (out == nullptr) {
    return Status(StatusCode::kInternalError, "null path output");
  }
  Status status = ValidateDataDirectory(data_dir);
  if (!status.ok()) {
    return status;
  }
  status = ValidateTopicName(topic);
  if (!status.ok()) {
    return status;
  }
  *out = JoinPath(JoinPath(data_dir, "meta"), topic + ".meta");
  return Status();
}

Status GroupTopicDirectory(const std::string& data_dir,
                           const std::string& group, const std::string& topic,
                           std::string* out) {
  if (out == nullptr) {
    return Status(StatusCode::kInternalError, "null path output");
  }
  Status status = ValidateDataDirectory(data_dir);
  if (!status.ok()) {
    return status;
  }
  status = ValidateGroupName(group);
  if (!status.ok()) {
    return status;
  }
  status = ValidateTopicName(topic);
  if (!status.ok()) {
    return status;
  }
  *out = JoinPath(JoinPath(JoinPath(data_dir, "groups"), group), topic);
  return Status();
}

Status SegmentLogPath(const std::string& data_dir, const std::string& topic,
                      std::uint32_t partition,
                      std::uint32_t partition_count,
                      std::uint64_t base_offset, std::string* out) {
  if (out == nullptr) {
    return Status(StatusCode::kInternalError, "null path output");
  }
  std::string directory;
  Status status =
      PartitionDirectory(data_dir, topic, partition, partition_count,
                         &directory);
  if (!status.ok()) {
    return status;
  }
  *out = JoinPath(directory, FormatBaseOffset(base_offset) + ".log");
  return Status();
}

Status SegmentIndexPath(const std::string& data_dir, const std::string& topic,
                        std::uint32_t partition,
                        std::uint32_t partition_count,
                        std::uint64_t base_offset, std::string* out) {
  if (out == nullptr) {
    return Status(StatusCode::kInternalError, "null path output");
  }
  std::string directory;
  Status status =
      PartitionDirectory(data_dir, topic, partition, partition_count,
                         &directory);
  if (!status.ok()) {
    return status;
  }
  *out = JoinPath(directory, FormatBaseOffset(base_offset) + ".index");
  return Status();
}

}  // namespace storage
}  // namespace minikafka
