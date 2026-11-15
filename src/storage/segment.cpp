#include "minikafka/storage/segment.h"

#include "scoped_fd.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <string>
#include <utility>
#include <vector>

#include "minikafka/storage/paths.h"

namespace minikafka {
namespace storage {

using common::Status;
using common::StatusCode;
using common::StatusOr;

namespace {

const std::uint64_t kMaxSegmentBytes = 0xFFFFFFFFull;
const std::uint32_t kIndexInterval = 4096u;

std::string JoinPath(const std::string& directory, const std::string& name) {
  if (directory.empty()) {
    return name;
  }
  if (directory.back() == '/') {
    return directory + name;
  }
  return directory + "/" + name;
}

bool PwriteAll(int fd, const char* data, std::size_t size,
               std::uint64_t offset) {
  std::size_t done = 0;
  while (done < size) {
    const ssize_t n = ::pwrite(fd, data + done, size - done,
                               static_cast<off_t>(offset + done));
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (n == 0) {
      return false;
    }
    done += static_cast<std::size_t>(n);
  }
  return true;
}

bool PreadAll(int fd, char* data, std::size_t size, std::uint64_t offset) {
  std::size_t done = 0;
  while (done < size) {
    const ssize_t n = ::pread(fd, data + done, size - done,
                              static_cast<off_t>(offset + done));
    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (n == 0) {
      return false;
    }
    done += static_cast<std::size_t>(n);
  }
  return true;
}

}  // namespace

Segment::Segment(std::string directory, std::uint64_t base_offset,
                 std::uint64_t segment_bytes, std::string path,
                 std::string index_path, int fd)
    : directory_(std::move(directory)),
      path_(std::move(path)),
      index_path_(std::move(index_path)),
      base_offset_(base_offset),
      next_offset_(base_offset),
      segment_bytes_(segment_bytes),
      size_(0),
      sealed_(false),
      fd_(fd),
      index_(index_path_),
      last_checkpoint_position_(0) {}

Segment::Segment(Segment&& other) noexcept
    : directory_(std::move(other.directory_)),
      path_(std::move(other.path_)),
      index_path_(std::move(other.index_path_)),
      base_offset_(other.base_offset_),
      next_offset_(other.next_offset_),
      segment_bytes_(other.segment_bytes_),
      size_(other.size_),
      sealed_(other.sealed_),
      fd_(other.fd_),
      index_(std::move(other.index_)),
      last_checkpoint_position_(other.last_checkpoint_position_) {
  other.fd_ = -1;
}

Segment& Segment::operator=(Segment&& other) noexcept {
  if (this != &other) {
    if (fd_ >= 0) {
      ::close(fd_);
    }
    directory_ = std::move(other.directory_);
    path_ = std::move(other.path_);
    index_path_ = std::move(other.index_path_);
    base_offset_ = other.base_offset_;
    next_offset_ = other.next_offset_;
    segment_bytes_ = other.segment_bytes_;
    size_ = other.size_;
    sealed_ = other.sealed_;
    fd_ = other.fd_;
    index_ = std::move(other.index_);
    last_checkpoint_position_ = other.last_checkpoint_position_;
    other.fd_ = -1;
  }
  return *this;
}

Segment::~Segment() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

common::StatusOr<Segment> Segment::Create(const std::string& directory,
                                          std::uint64_t base_offset,
                                          std::uint64_t segment_bytes) {
  if (directory.empty()) {
    return StatusOr<Segment>(
        Status(StatusCode::kInternalError, "empty segment directory"));
  }
  if (segment_bytes == 0u || segment_bytes > kMaxSegmentBytes) {
    return StatusOr<Segment>(
        Status(StatusCode::kInternalError, "invalid segment_bytes"));
  }

  const std::string name = FormatBaseOffset(base_offset);
  const std::string log_path = JoinPath(directory, name + ".log");
  const std::string index_path = JoinPath(directory, name + ".index");

  internal::ScopedFd fd(::open(
      log_path.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0644));
  if (!fd.valid()) {
    return StatusOr<Segment>(
        Status(StatusCode::kStorageIoError, "cannot create segment log"));
  }
  if (::unlink(index_path.c_str()) != 0 && errno != ENOENT) {
    return StatusOr<Segment>(
        Status(StatusCode::kStorageIoError,
               "cannot clear stale segment index"));
  }

  Segment segment(directory, base_offset, segment_bytes, log_path, index_path,
                  fd.get());
  fd.Release();
  segment.index_.ResetForNew();
  return StatusOr<Segment>(std::move(segment));
}

common::StatusOr<Segment> Segment::Open(const std::string& directory,
                                        std::uint64_t base_offset,
                                        std::uint64_t next_offset,
                                        std::uint64_t segment_bytes) {
  if (directory.empty()) {
    return StatusOr<Segment>(
        Status(StatusCode::kInternalError, "empty segment directory"));
  }
  if (segment_bytes == 0u || segment_bytes > kMaxSegmentBytes) {
    return StatusOr<Segment>(
        Status(StatusCode::kInternalError, "invalid segment_bytes"));
  }
  if (next_offset < base_offset) {
    return StatusOr<Segment>(
        Status(StatusCode::kInternalError, "next_offset precedes base_offset"));
  }

  const std::string name = FormatBaseOffset(base_offset);
  const std::string log_path = JoinPath(directory, name + ".log");
  const std::string index_path = JoinPath(directory, name + ".index");

  internal::ScopedFd fd(::open(log_path.c_str(), O_RDWR | O_CLOEXEC));
  if (!fd.valid()) {
    return StatusOr<Segment>(
        Status(StatusCode::kStorageIoError, "cannot open segment log"));
  }
  struct stat st;
  if (::fstat(fd.get(), &st) != 0 || st.st_size < 0) {
    return StatusOr<Segment>(
        Status(StatusCode::kStorageIoError, "cannot stat segment log"));
  }

  Segment segment(directory, base_offset, segment_bytes, log_path, index_path,
                  fd.get());
  fd.Release();
  segment.size_ = static_cast<std::uint64_t>(st.st_size);
  segment.next_offset_ = next_offset;
  segment.index_.Load(segment.size_, base_offset);
  if (segment.index_.valid() && !segment.index_.entries().empty()) {
    segment.last_checkpoint_position_ =
        segment.index_.entries().back().position;
  }
  return StatusOr<Segment>(std::move(segment));
}

common::StatusOr<std::uint64_t> Segment::Append(const Record& record) {
  std::string bytes;
  const Status status = EncodeRecord(record, &bytes);
  if (!status.ok()) {
    return StatusOr<std::uint64_t>(status);
  }
  return AppendEncoded(bytes);
}

common::StatusOr<std::uint64_t> Segment::AppendEncoded(
    const std::string& bytes) {
  if (sealed_) {
    return StatusOr<std::uint64_t>(
        Status(StatusCode::kInternalError, "segment is sealed"));
  }
  if (bytes.empty()) {
    return StatusOr<std::uint64_t>(
        Status(StatusCode::kInternalError, "empty encoded record"));
  }

  const std::uint64_t record_size = static_cast<std::uint64_t>(bytes.size());
  if (record_size > segment_bytes_) {
    return StatusOr<std::uint64_t>(
        Status(StatusCode::kMessageTooLarge,
               "record_size exceeds segment_bytes"));
  }
  if (size_ != 0u && size_ + record_size > segment_bytes_) {
    return StatusOr<std::uint64_t>(
        Status(StatusCode::kInternalError, "segment is full; roll required"));
  }

  const std::uint64_t position = size_;
  const std::uint64_t offset = next_offset_;
  if (!PwriteAll(fd_, bytes.data(), bytes.size(), position)) {
    return StatusOr<std::uint64_t>(
        Status(StatusCode::kStorageIoError, "cannot append record to segment"));
  }

  size_ += record_size;
  next_offset_ = offset + 1u;

  if (index_.valid()) {
    const std::uint32_t position32 = static_cast<std::uint32_t>(position);
    bool checkpoint = false;
    if (index_.entries().empty()) {
      if (position32 == 0u) {
        checkpoint = true;
      } else {
        index_.Invalidate();
      }
    } else if (position32 >= last_checkpoint_position_ &&
               position32 - last_checkpoint_position_ >= kIndexInterval) {
      checkpoint = true;
    }
    if (checkpoint) {
      const Status index_status = index_.AppendEntry(offset, position32);
      if (index_status.ok()) {
        last_checkpoint_position_ = position32;
      } else {
        index_.Invalidate();
      }
    }
  }

  return StatusOr<std::uint64_t>(offset);
}

common::StatusOr<Record> Segment::ReadAt(std::uint64_t offset) const {
  if (offset < base_offset_ || offset >= next_offset_) {
    return StatusOr<Record>(
        Status(StatusCode::kOffsetOutOfRange, "offset is outside segment"));
  }

  std::uint64_t position = 0;
  std::uint64_t current_offset = base_offset_;
  if (index_.valid()) {
    IndexEntry checkpoint;
    if (index_.FindCheckpoint(offset, &checkpoint)) {
      position = checkpoint.position;
      current_offset = checkpoint.offset;
    }
  }

  while (position < size_) {
    Record record;
    std::size_t consumed = 0;
    const Status status = ReadRecordAt(position, &record, &consumed);
    if (!status.ok()) {
      return StatusOr<Record>(status);
    }
    if (current_offset == offset) {
      return StatusOr<Record>(std::move(record));
    }
    if (current_offset > offset) {
      break;
    }
    position += static_cast<std::uint64_t>(consumed);
    ++current_offset;
  }

  return StatusOr<Record>(
      Status(StatusCode::kOffsetOutOfRange, "offset not found in segment"));
}

common::Status Segment::ReadRecordAt(std::uint64_t position, Record* record,
                                     std::size_t* consumed) const {
  if (position >= size_) {
    return Status(StatusCode::kPayloadDecodeError,
                  "record position is outside segment");
  }
  const std::uint64_t available = size_ - position;
  const std::size_t to_read =
      available > static_cast<std::uint64_t>(kMaxRecordSize)
          ? kMaxRecordSize
          : static_cast<std::size_t>(available);

  std::string buffer(to_read, '\0');
  if (!PreadAll(fd_, &buffer[0], buffer.size(), position)) {
    return Status(StatusCode::kStorageIoError, "cannot read segment record");
  }

  RecordDecodeOutcome outcome = DecodeRecord(buffer.data(), buffer.size());
  if (outcome.result == RecordDecodeResult::kOk) {
    *record = std::move(outcome.record);
    *consumed = outcome.consumed;
    return Status();
  }
  if (outcome.result == RecordDecodeResult::kNeedMore) {
    return Status(StatusCode::kPayloadDecodeError,
                  "truncated record in segment");
  }
  return outcome.status;
}

common::Status Segment::Seal() {
  sealed_ = true;
  return Status();
}

common::StatusOr<Segment> Segment::Roll() {
  if (sealed_) {
    return StatusOr<Segment>(
        Status(StatusCode::kInternalError, "segment is already sealed"));
  }
  if (size_ == 0u) {
    return StatusOr<Segment>(
        Status(StatusCode::kInternalError, "cannot roll an empty segment"));
  }
  const Status seal_status = Seal();
  if (!seal_status.ok()) {
    return StatusOr<Segment>(seal_status);
  }
  return Segment::Create(directory_, next_offset_, segment_bytes_);
}

common::Status Segment::RebuildIndex() {
  std::vector<IndexEntry> entries;
  std::uint64_t position = 0;
  std::uint64_t current_offset = base_offset_;
  bool first_record = true;
  std::uint32_t last_position = 0;

  while (position < size_) {
    Record record;
    std::size_t consumed = 0;
    const Status status = ReadRecordAt(position, &record, &consumed);
    if (!status.ok()) {
      index_.Invalidate();
      return Status();
    }

    const std::uint32_t position32 = static_cast<std::uint32_t>(position);
    if (first_record) {
      IndexEntry entry;
      entry.offset = current_offset;
      entry.position = position32;
      entries.push_back(entry);
      last_position = position32;
      first_record = false;
    } else if (position32 >= last_position &&
               position32 - last_position >= kIndexInterval) {
      IndexEntry entry;
      entry.offset = current_offset;
      entry.position = position32;
      entries.push_back(entry);
      last_position = position32;
    }

    position += static_cast<std::uint64_t>(consumed);
    ++current_offset;
  }

  const Status store_status = index_.Store(entries, size_, base_offset_);
  if (!store_status.ok()) {
    index_.Invalidate();
    return Status();
  }
  last_checkpoint_position_ =
      entries.empty() ? 0u : entries.back().position;
  return Status();
}

}  // namespace storage
}  // namespace minikafka
