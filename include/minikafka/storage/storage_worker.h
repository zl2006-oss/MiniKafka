#ifndef MINIKAFKA_STORAGE_STORAGE_WORKER_H_
#define MINIKAFKA_STORAGE_STORAGE_WORKER_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "minikafka/common/status.h"
#include "minikafka/storage/record.h"
#include "minikafka/storage/recovery.h"
#include "minikafka/storage/segment_set.h"

namespace minikafka {
namespace storage {

// 单个分区的读写门面：恢复结果 + 分段集合 + 写前空间预检。
// 线程安全由所属 StorageWorker 保证（同分区串行），本类自身不加锁。
class PartitionStore {
 public:
  // 目录不存在时自动创建；恢复失败（基址断链/封存段损坏）也返回对象，
  // 但 state()=kUnavailable，Append 会拒绝，ReadAt 仍可尝试。
  static common::StatusOr<PartitionStore> Open(
      const std::string& partition_dir, std::uint64_t segment_bytes,
      std::uint64_t disk_threshold_bytes);

  // R16：每批追加前预检可用空间；低于阈值返回 disk_full，不写任何字节。
  common::Status Append(const Record& record, std::uint64_t* offset);

  common::StatusOr<Record> ReadAt(std::uint64_t offset) const;

  // 供上层定时调用（至少每秒一次），刷新低空间状态。
  common::Status RefreshDiskSpace();

  std::uint64_t next_offset() const;
  PartitionState state() const { return state_; }
  bool ready() const { return state_ == PartitionState::kReady; }
  bool low_space() const { return low_space_; }
  const std::string& warning() const { return warning_; }
  const std::string& directory() const { return directory_; }
  std::uint64_t truncated_bytes() const { return truncated_bytes_; }
  std::size_t segment_count() const;

 private:
  PartitionStore(std::string directory, PartitionState state,
                 std::string warning, std::uint64_t truncated_bytes,
                 std::uint64_t segment_bytes, std::uint64_t disk_threshold,
                 std::unique_ptr<SegmentSet> segments);

  std::string directory_;
  PartitionState state_;
  std::string warning_;
  std::uint64_t truncated_bytes_;
  std::uint64_t segment_bytes_;
  std::uint64_t disk_threshold_;
  std::unique_ptr<SegmentSet> segments_;
  bool low_space_;
};

// AD-05：每个分区恒定属于一个 Storage Worker，单分区天然串行、无需分区锁。
class StorageWorker {
 public:
  explicit StorageWorker(std::size_t index) : index_(index) {}

  void AddPartition(std::uint32_t partition,
                    std::unique_ptr<PartitionStore> store);
  PartitionStore* Find(std::uint32_t partition) const;
  std::size_t partition_count() const { return partitions_.size(); }
  const std::vector<std::uint32_t>& partitions() const { return partition_ids_; }
  std::size_t index() const { return index_; }

 private:
  std::size_t index_;
  std::vector<std::pair<std::uint32_t, std::unique_ptr<PartitionStore>>>
      partitions_;
  std::vector<std::uint32_t> partition_ids_;
};

// 分区亲和分配：同一分区恒定映射到同一 worker。
class StorageWorkerPool {
 public:
  explicit StorageWorkerPool(std::size_t worker_count);

  std::size_t worker_count() const { return worker_count_; }
  std::size_t WorkerIndexFor(std::uint32_t partition) const;

 private:
  std::size_t worker_count_;
};

}  // namespace storage
}  // namespace minikafka

#endif  // MINIKAFKA_STORAGE_STORAGE_WORKER_H_
