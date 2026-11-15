#include "minikafka/storage/segment_set.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "minikafka/storage/recovery.h"

namespace minikafka {
namespace storage {

using common::Status;
using common::StatusCode;
using common::StatusOr;

SegmentSet::SegmentSet(std::unique_ptr<Segment> active,
                       std::uint64_t segment_bytes)
    : segment_bytes_(segment_bytes), active_(std::move(active)) {}

SegmentSet::SegmentSet(std::vector<std::unique_ptr<Segment>> sealed,
                       std::unique_ptr<Segment> active,
                       std::uint64_t segment_bytes)
    : segment_bytes_(segment_bytes),
      sealed_(std::move(sealed)),
      active_(std::move(active)) {}

common::StatusOr<SegmentSet> SegmentSet::Create(
    const std::string& directory, std::uint64_t base_offset,
    std::uint64_t segment_bytes) {
  StatusOr<Segment> active =
      Segment::Create(directory, base_offset, segment_bytes);
  if (!active.ok()) {
    return StatusOr<SegmentSet>(active.status());
  }
  std::unique_ptr<Segment> owner(
      new Segment(std::move(active).value()));
  return StatusOr<SegmentSet>(
      SegmentSet(std::move(owner), segment_bytes));
}

// 打开既有分段目录：文件按 base_offset 升序，最后一段为 active。
// 封存段 next_offset 取下一段 base_offset；active 段取扫描得出的 active_next_offset。
common::StatusOr<SegmentSet> SegmentSet::Open(
    const std::string& directory, std::uint64_t segment_bytes,
    std::uint64_t active_next_offset) {
  if (directory.empty()) {
    return StatusOr<SegmentSet>(
        Status(StatusCode::kInternalError, "empty segment directory"));
  }

  std::vector<SegmentFileInfo> files;
  const Status list_status = ListSegments(directory, &files);
  if (!list_status.ok()) {
    return StatusOr<SegmentSet>(list_status);
  }
  if (files.empty()) {
    return StatusOr<SegmentSet>(
        Status(StatusCode::kStorageUnavailable,
               "no segment file found in " + directory));
  }

  std::vector<std::unique_ptr<Segment>> sealed;
  sealed.reserve(files.size() - 1u);
  for (std::size_t i = 0; i < files.size(); ++i) {
    const bool is_active = (i + 1u == files.size());
    const std::uint64_t next_offset =
        is_active ? active_next_offset : files[i + 1u].base_offset;
    StatusOr<Segment> opened =
        Segment::Open(directory, files[i].base_offset, next_offset,
                      segment_bytes);
    if (!opened.ok()) {
      return StatusOr<SegmentSet>(opened.status());
    }
    std::unique_ptr<Segment> owner(new Segment(std::move(opened).value()));
    if (!is_active) {
      (void)owner->Seal();
      sealed.push_back(std::move(owner));
      continue;
    }
    return StatusOr<SegmentSet>(
        SegmentSet(std::move(sealed), std::move(owner), segment_bytes));
  }

  // 循环必在最后一段返回，此处仅为消除控制流缺口。
  return StatusOr<SegmentSet>(
      Status(StatusCode::kStorageUnavailable, "no active segment found"));
}

common::Status SegmentSet::Append(const Record& record,
                                  std::uint64_t* offset) {
  if (offset == nullptr) {
    return Status(StatusCode::kInternalError, "null offset output");
  }
  if (!active_) {
    return Status(StatusCode::kInternalError, "segment set is not open");
  }

  std::string bytes;
  const Status encode_status = EncodeRecord(record, &bytes);
  if (!encode_status.ok()) {
    return encode_status;
  }
  const std::uint64_t record_size =
      static_cast<std::uint64_t>(bytes.size());
  if (record_size > segment_bytes_) {
    return Status(StatusCode::kMessageTooLarge,
                  "record_size exceeds segment_bytes");
  }

  if (active_->size() != 0u &&
      active_->size() + record_size > segment_bytes_) {
    StatusOr<Segment> next = active_->Roll();
    if (!next.ok()) {
      return next.status();
    }
    sealed_.push_back(std::move(active_));
    active_.reset(new Segment(std::move(next).value()));
  }

  StatusOr<std::uint64_t> appended = active_->AppendEncoded(bytes);
  if (!appended.ok()) {
    return appended.status();
  }
  *offset = appended.value();
  return Status();
}

common::StatusOr<Record> SegmentSet::ReadAt(std::uint64_t offset) const {
  if (active_ && offset >= active_->base_offset() &&
      offset < active_->next_offset()) {
    return active_->ReadAt(offset);
  }
  // sealed_ 按 base_offset 升序：二分定位最后一个 base <= offset 的段。
  std::vector<std::unique_ptr<Segment>>::const_iterator it = std::upper_bound(
      sealed_.begin(), sealed_.end(), offset,
      [](std::uint64_t value, const std::unique_ptr<Segment>& segment) {
        return value < segment->base_offset();
      });
  if (it != sealed_.begin()) {
    --it;
    if (offset < (*it)->next_offset()) {
      return (*it)->ReadAt(offset);
    }
  }
  return StatusOr<Record>(
      Status(StatusCode::kOffsetOutOfRange, "offset is outside segment set"));
}

std::uint64_t SegmentSet::next_offset() const {
  return active_ ? active_->next_offset() : 0u;
}

std::size_t SegmentSet::segment_count() const {
  return sealed_.size() + (active_ ? 1u : 0u);
}

std::vector<std::uint64_t> SegmentSet::base_offsets() const {
  std::vector<std::uint64_t> offsets;
  offsets.reserve(sealed_.size() + (active_ ? 1u : 0u));
  for (std::size_t i = 0; i < sealed_.size(); ++i) {
    offsets.push_back(sealed_[i]->base_offset());
  }
  if (active_) {
    offsets.push_back(active_->base_offset());
  }
  return offsets;
}

}  // namespace storage
}  // namespace minikafka
