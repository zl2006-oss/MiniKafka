#ifndef MINIKAFKA_STORAGE_SEGMENT_SET_H_
#define MINIKAFKA_STORAGE_SEGMENT_SET_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "minikafka/common/status.h"
#include "minikafka/storage/record.h"
#include "minikafka/storage/segment.h"

namespace minikafka {
namespace storage {

// 上层最小封装：按 base_offset 管理 active/sealed 分段、滚动与跨段定位。
// 单分区滚动/追加仍由所属 Storage Worker 串行调用。
class SegmentSet {
 public:
  static common::StatusOr<SegmentSet> Create(const std::string& directory,
                                             std::uint64_t base_offset,
                                             std::uint64_t segment_bytes);

  SegmentSet(SegmentSet&&) noexcept = default;
  SegmentSet& operator=(SegmentSet&&) noexcept = default;
  SegmentSet(const SegmentSet&) = delete;
  SegmentSet& operator=(const SegmentSet&) = delete;
  ~SegmentSet() = default;

  // 追加前检查滚动条件；成功返回该分区逻辑 offset。
  common::Status Append(const Record& record, std::uint64_t* offset);

  common::StatusOr<Record> ReadAt(std::uint64_t offset) const;

  std::uint64_t next_offset() const;
  std::size_t segment_count() const;
  std::vector<std::uint64_t> base_offsets() const;
  const Segment& active_segment() const { return *active_; }

 private:
  SegmentSet(std::unique_ptr<Segment> active, std::uint64_t segment_bytes);

  std::uint64_t segment_bytes_;
  std::vector<std::unique_ptr<Segment>> sealed_;
  std::unique_ptr<Segment> active_;
};

}  // namespace storage
}  // namespace minikafka

#endif  // MINIKAFKA_STORAGE_SEGMENT_SET_H_
