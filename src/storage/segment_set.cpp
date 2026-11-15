#include "minikafka/storage/segment_set.h"

#include <utility>

namespace minikafka {
namespace storage {

using common::Status;
using common::StatusCode;
using common::StatusOr;

SegmentSet::SegmentSet(std::unique_ptr<Segment> active,
                       std::uint64_t segment_bytes)
    : segment_bytes_(segment_bytes), active_(std::move(active)) {}

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
  for (std::vector<std::unique_ptr<Segment>>::const_reverse_iterator it =
           sealed_.rbegin();
       it != sealed_.rend(); ++it) {
    if (offset >= (*it)->base_offset() && offset < (*it)->next_offset()) {
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
