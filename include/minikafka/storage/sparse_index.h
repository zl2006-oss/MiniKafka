#ifndef MINIKAFKA_STORAGE_SPARSE_INDEX_H_
#define MINIKAFKA_STORAGE_SPARSE_INDEX_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "minikafka/common/status.h"

namespace minikafka {
namespace storage {

// 04 §4.4：索引条目固定 12 字节，无文件头。
struct IndexEntry {
  std::uint64_t offset = 0;
  std::uint32_t position = 0;
};

// 稀疏索引文件原语；索引是派生数据，缺失/损坏只降级，不影响数据。
class SparseIndex {
 public:
  explicit SparseIndex(std::string path);

  // 新分段：内存索引有效且为空，首次 AppendEntry 写检查点。
  void ResetForNew();

  // 读取并校验索引；缺失/损坏时 valid()=false，不抛错、不阻塞。
  void Load(std::uint64_t segment_size, std::uint64_t base_offset);

  // 写完整索引：tmp + rename；成功后更新内存索引。
  common::Status Store(const std::vector<IndexEntry>& entries,
                       std::uint64_t segment_size,
                       std::uint64_t base_offset);

  // 追加一个检查点（数据已写入后再调用）；失败时调用方可降级。
  common::Status AppendEntry(std::uint64_t offset, std::uint32_t position);

  // 二分找最后一个 offset <= target 的检查点。
  bool FindCheckpoint(std::uint64_t target, IndexEntry* entry) const;

  void Invalidate();

  bool valid() const { return valid_; }
  const std::vector<IndexEntry>& entries() const { return entries_; }
  std::size_t entry_count() const { return entries_.size(); }
  const std::string& path() const { return path_; }

 private:
  std::string path_;
  std::vector<IndexEntry> entries_;
  bool valid_;
};

}  // namespace storage
}  // namespace minikafka

#endif  // MINIKAFKA_STORAGE_SPARSE_INDEX_H_
