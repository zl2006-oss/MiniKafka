#include "minikafka/storage/storage_worker.h"

#include <utility>

#include "minikafka/storage/recovery.h"

namespace minikafka {
namespace storage {

using common::Status;
using common::StatusCode;
using common::StatusOr;

PartitionStore::PartitionStore(std::string directory, PartitionState state,
                               std::string warning,
                               std::uint64_t truncated_bytes,
                               std::uint64_t segment_bytes,
                               std::uint64_t disk_threshold,
                               std::uint64_t max_record_bytes,
                               std::unique_ptr<SegmentSet> segments)
    : directory_(std::move(directory)),
      state_(state),
      warning_(std::move(warning)),
      truncated_bytes_(truncated_bytes),
      segment_bytes_(segment_bytes),
      disk_threshold_(disk_threshold),
      max_record_bytes_(max_record_bytes),
      segments_(std::move(segments)),
      low_space_(false) {}

common::StatusOr<PartitionStore> PartitionStore::Open(
    const std::string& partition_dir, std::uint64_t segment_bytes,
    std::uint64_t disk_threshold_bytes, std::uint64_t max_record_bytes) {
  if (partition_dir.empty()) {
    return StatusOr<PartitionStore>(
        Status(StatusCode::kInternalError, "empty partition directory"));
  }

  // 04 §5.2：恢复只读校验 + 仅截断 active 段尾部。
  StatusOr<RecoveredPartition> recovered =
      RecoverPartition(partition_dir, segment_bytes);
  if (!recovered.ok()) {
    return StatusOr<PartitionStore>(recovered.status());
  }
  const RecoveredPartition& info = recovered.value();

  // 基址断链 / 封存段损坏：保持不可用，绝不写文件，也不阻止 Broker 启动。
  if (info.state != PartitionState::kReady) {
    std::unique_ptr<SegmentSet> none;
    return StatusOr<PartitionStore>(PartitionStore(
        partition_dir, PartitionState::kUnavailable, info.warning,
        info.truncated_bytes, segment_bytes, disk_threshold_bytes,
        max_record_bytes, std::move(none)));
  }

  StatusOr<SegmentSet> segments =
      SegmentSet::Open(partition_dir, segment_bytes, info.next_offset);
  if (!segments.ok()) {
    return StatusOr<PartitionStore>(segments.status());
  }
  std::unique_ptr<SegmentSet> owner(
      new SegmentSet(std::move(segments).value()));

  PartitionStore store(partition_dir, PartitionState::kReady, info.warning,
                       info.truncated_bytes, segment_bytes,
                       disk_threshold_bytes, max_record_bytes,
                       std::move(owner));
  (void)store.RefreshDiskSpace();
  return StatusOr<PartitionStore>(std::move(store));
}

common::Status PartitionStore::Append(const Record& record,
                                     std::uint64_t* offset) {
  if (offset == nullptr) {
    return Status(StatusCode::kInternalError, "null offset output");
  }
  if (state_ != PartitionState::kReady || !segments_) {
    return Status(StatusCode::kStorageUnavailable,
                  "partition is unavailable: " + warning_);
  }

  // R17：Broker 端可配置上限（默认 1 MiB，受 S-09/P-08 约束不超过 1 MiB）。
  const std::uint64_t record_size =
      static_cast<std::uint64_t>(kRecordHeaderSize) +
      static_cast<std::uint64_t>(record.key.size()) +
      static_cast<std::uint64_t>(record.value.size());
  if (record_size > max_record_bytes_) {
    return Status(StatusCode::kMessageTooLarge,
                  "record_size exceeds configured limit " +
                      std::to_string(max_record_bytes_) + " bytes");
  }

  // R16：写前预检，不写任何字节就返回 disk_full。
  const Status space_status = CheckDiskSpace(directory_, disk_threshold_);
  if (!space_status.ok()) {
    low_space_ = true;
    return space_status;
  }
  const Status append_status = segments_->Append(record, offset);
  if (!append_status.ok()) {
    return append_status;
  }
  low_space_ = false;
  return Status();
}

common::StatusOr<Record> PartitionStore::ReadAt(std::uint64_t offset) const {
  if (!segments_) {
    return StatusOr<Record>(
        Status(StatusCode::kStorageUnavailable,
               "partition is unavailable: " + warning_));
  }
  return segments_->ReadAt(offset);
}

common::Status PartitionStore::RefreshDiskSpace() {
  StatusOr<std::uint64_t> available = AvailableBytes(directory_);
  if (!available.ok()) {
    return available.status();
  }
  low_space_ = available.value() < disk_threshold_;
  return Status();
}

std::uint64_t PartitionStore::next_offset() const {
  return segments_ ? segments_->next_offset() : 0u;
}

std::size_t PartitionStore::segment_count() const {
  return segments_ ? segments_->segment_count() : 0u;
}

void StorageWorker::AddPartition(std::uint32_t partition,
                                 std::unique_ptr<PartitionStore> store) {
  for (std::size_t i = 0; i < partitions_.size(); ++i) {
    if (partitions_[i].first == partition) {
      partitions_[i].second = std::move(store);
      return;
    }
  }
  partitions_.push_back(
      std::make_pair(partition, std::move(store)));
  partition_ids_.push_back(partition);
}

PartitionStore* StorageWorker::Find(std::uint32_t partition) const {
  for (std::size_t i = 0; i < partitions_.size(); ++i) {
    if (partitions_[i].first == partition) {
      return partitions_[i].second.get();
    }
  }
  return nullptr;
}

// AD-05：partition % worker_count 恒定映射，保证同分区永远落在同一 worker。
StorageWorkerPool::StorageWorkerPool(std::size_t worker_count)
    : worker_count_(worker_count == 0u ? 1u : worker_count) {}

std::size_t StorageWorkerPool::WorkerIndexFor(std::uint32_t partition) const {
  return static_cast<std::size_t>(partition) % worker_count_;
}

}  // namespace storage
}  // namespace minikafka
