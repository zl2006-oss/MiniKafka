#ifndef MINIKAFKA_STORAGE_OFFSET_FILE_H_
#define MINIKAFKA_STORAGE_OFFSET_FILE_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "minikafka/common/status.h"

namespace minikafka {
namespace storage {

// 04 §6.2：offsets.a / offsets.b 的固定格式。
constexpr std::uint32_t kOffsetFileMagic = 0x4D4B4F46u;  // "MKOF"
constexpr std::uint16_t kOffsetFileVersion = 1u;
constexpr std::size_t kOffsetFileHeaderSize = 24u;
constexpr std::size_t kOffsetEntrySize = 12u;
constexpr std::size_t kOffsetFileFooterSize = 4u;
constexpr std::uint32_t kMaxOffsetPartitions = 16u;

struct OffsetEntry {
  std::uint32_t partition = 0;
  std::uint64_t committed_offset = 0;
};

// entries 必须按 partition 严格升序且唯一。
struct OffsetSnapshot {
  std::uint64_t generation = 0;
  std::vector<OffsetEntry> entries;
};

// 双槽读取结果；found=false 表示无任何可用快照（两槽缺失/均损坏）。
struct OffsetLoadResult {
  bool found = false;
  bool degraded = false;
  OffsetSnapshot snapshot;
  std::string warning;
};

common::Status EncodeOffsetSnapshot(const OffsetSnapshot& snapshot,
                                    std::string* bytes);
common::Status DecodeOffsetSnapshot(const void* data, std::size_t size,
                                    OffsetSnapshot* snapshot);

// 一个 (group, topic) 的 offsets.a / offsets.b 双槽快照。
class OffsetStore {
 public:
  static common::StatusOr<OffsetStore> Open(const std::string& data_dir,
                                            const std::string& group,
                                            const std::string& topic);

  // 读取两槽，选择 generation 最大且校验通过者；损坏时回退另一槽。
  // 两槽均不可用返回 ok + found=false，不因位点损坏报错退出。
  common::StatusOr<OffsetLoadResult> Load() const;

  // 合并 updates（同 partition 取 max），generation+1，写 tmp 后覆盖较旧槽。
  common::Status Commit(const std::vector<OffsetEntry>& updates,
                        OffsetSnapshot* resulting);

  const std::string& directory() const { return directory_; }
  const std::string& slot_a_path() const { return slot_a_path_; }
  const std::string& slot_b_path() const { return slot_b_path_; }
  const std::string& tmp_path() const { return tmp_path_; }

 private:
  explicit OffsetStore(std::string directory);

  std::string directory_;
  std::string slot_a_path_;
  std::string slot_b_path_;
  std::string tmp_path_;
};

}  // namespace storage
}  // namespace minikafka

#endif  // MINIKAFKA_STORAGE_OFFSET_FILE_H_
