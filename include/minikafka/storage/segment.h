#ifndef MINIKAFKA_STORAGE_SEGMENT_H_
#define MINIKAFKA_STORAGE_SEGMENT_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "minikafka/common/status.h"
#include "minikafka/storage/record.h"
#include "minikafka/storage/sparse_index.h"

namespace minikafka {
namespace storage {

// 单个分段文件（<base_offset:020d>.log/.index）。
// 同一分段不提供线程安全保证，由所属 Storage Worker 串行调用。
class Segment {
 public:
  // 新建空分段；segment_bytes 由调用方传入，不在此处写死默认值。
  static common::StatusOr<Segment> Create(const std::string& directory,
                                          std::uint64_t base_offset,
                                          std::uint64_t segment_bytes);

  // 打开既有分段；next_offset 由恢复流程/调用方提供，本轮不做恢复扫描。
  static common::StatusOr<Segment> Open(const std::string& directory,
                                        std::uint64_t base_offset,
                                        std::uint64_t next_offset,
                                        std::uint64_t segment_bytes);

  Segment(Segment&& other) noexcept;
  Segment& operator=(Segment&& other) noexcept;
  ~Segment();

  Segment(const Segment&) = delete;
  Segment& operator=(const Segment&) = delete;

  // 编码记录后追加，成功返回分配的逻辑 offset。
  common::StatusOr<std::uint64_t> Append(const Record& record);

  // 追加已由 EncodeRecord 生成的记录字节；供上层滚动封装避免重复编码。
  common::StatusOr<std::uint64_t> AppendEncoded(const std::string& bytes);

  // 先查稀疏索引，再从检查点顺序扫描；索引无效时从分段起点线性扫描。
  common::StatusOr<Record> ReadAt(std::uint64_t offset) const;

  // 封存后拒绝写入；重复调用幂等。
  common::Status Seal();

  // 封存当前分段并新建 base=next_offset 的 active 分段。
  common::StatusOr<Segment> Roll();

  // 从日志顺序扫描并原子重建索引；失败时仅降级为线性扫描。
  common::Status RebuildIndex();

  std::uint64_t base_offset() const { return base_offset_; }
  std::uint64_t next_offset() const { return next_offset_; }
  std::uint64_t size() const { return size_; }
  std::uint64_t segment_bytes() const { return segment_bytes_; }
  bool sealed() const { return sealed_; }
  const std::string& path() const { return path_; }
  const std::string& index_path() const { return index_path_; }
  bool index_valid() const { return index_.valid(); }
  std::size_t index_entry_count() const { return index_.entry_count(); }
  const SparseIndex& index() const { return index_; }

 private:
  Segment(std::string directory, std::uint64_t base_offset,
          std::uint64_t segment_bytes, std::string path,
          std::string index_path, int fd);

  common::Status ReadRecordAt(std::uint64_t position, Record* record,
                              std::size_t* consumed) const;

  std::string directory_;
  std::string path_;
  std::string index_path_;
  std::uint64_t base_offset_;
  std::uint64_t next_offset_;
  std::uint64_t segment_bytes_;
  std::uint64_t size_;
  bool sealed_;
  int fd_;
  SparseIndex index_;
  std::uint32_t last_checkpoint_position_;
};

}  // namespace storage
}  // namespace minikafka

#endif  // MINIKAFKA_STORAGE_SEGMENT_H_
