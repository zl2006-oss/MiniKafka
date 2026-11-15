#ifndef MINIKAFKA_STORAGE_RECOVERY_H_
#define MINIKAFKA_STORAGE_RECOVERY_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "minikafka/common/status.h"

namespace minikafka {
namespace storage {

// 04 §8.4 / D-04-6：分区恢复后的服务状态。
enum class PartitionState {
  kReady,        // 可服务
  kUnavailable,  // 基址断链或封存段损坏：不修改任何文件，仅该分区不可用
};

struct SegmentFileInfo {
  std::uint64_t base_offset = 0;
  std::string log_path;
  std::uint64_t file_size = 0;
};

struct RecoveredPartition {
  PartitionState state = PartitionState::kUnavailable;
  std::uint64_t next_offset = 0;
  std::size_t segment_count = 0;
  std::uint64_t truncated_bytes = 0;
  std::string warning;
  common::Status status;
};

// 枚举 <partition>/ 下的 *.log，按 base_offset 升序；忽略 *.tmp 与非法文件名。
common::Status ListSegments(const std::string& partition_dir,
                            std::vector<SegmentFileInfo>* out);

// 04 §5.2：恢复一个分区目录。
// - 基址链校验：首段 base 必须为 0，相邻段必须前段 next_offset 衔接；
// - 只截断 active segment 的残缺尾部，封存段只读校验，绝不修改；
// - 重建 active 段索引（派生数据，失败仅降级为线性扫描）。
// 基址断链 / 封存段损坏时返回 kUnavailable，且不修改任何文件。
common::StatusOr<RecoveredPartition> RecoverPartition(
    const std::string& partition_dir, std::uint64_t segment_bytes);

// R16 / Q-P11：可用空间查询与写入前预检（statvfs）。
common::StatusOr<std::uint64_t> AvailableBytes(const std::string& path);
common::Status CheckDiskSpace(const std::string& path,
                              std::uint64_t required_bytes);

}  // namespace storage
}  // namespace minikafka

#endif  // MINIKAFKA_STORAGE_RECOVERY_H_
