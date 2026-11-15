#include "minikafka/storage/recovery.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <utility>
#include <vector>

#include "minikafka/storage/paths.h"
#include "minikafka/storage/record.h"
#include "minikafka/storage/segment.h"

#include "byte_codec.h"
#include "file_util.h"
#include "scoped_fd.h"

namespace minikafka {
namespace storage {

using common::Status;
using common::StatusCode;
using common::StatusOr;

namespace {

// 20 位十进制 + ".log"
constexpr std::size_t kSegmentNameLength = 24u;
constexpr std::size_t kOffsetDigits = 20u;

bool ParseSegmentName(const char* name, std::uint64_t* base_offset) {
  if (name == nullptr || base_offset == nullptr) {
    return false;
  }
  if (std::strlen(name) != kSegmentNameLength) {
    return false;
  }
  if (std::strcmp(name + kOffsetDigits, ".log") != 0) {
    return false;
  }
  std::uint64_t value = 0;
  for (std::size_t i = 0; i < kOffsetDigits; ++i) {
    if (name[i] < '0' || name[i] > '9') {
      return false;
    }
    value = value * 10u + static_cast<std::uint64_t>(name[i] - '0');
  }
  *base_offset = value;
  return true;
}

struct ScanResult {
  bool readable = true;
  bool has_record = false;
  std::uint64_t next_offset = 0;
  std::uint64_t valid_end = 0;
  std::uint64_t file_size = 0;
};

// 从分段起点顺序扫描：只统计「校验通过」的记录，遇到第一条不合格记录即停。
ScanResult ScanSegment(const std::string& path, std::uint64_t base_offset) {
  ScanResult out;
  out.next_offset = base_offset;

  internal::ScopedFd fd(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
  if (!fd.valid()) {
    out.readable = false;
    return out;
  }
  struct stat st;
  if (::fstat(fd.get(), &st) != 0 || st.st_size < 0) {
    out.readable = false;
    return out;
  }
  out.file_size = static_cast<std::uint64_t>(st.st_size);

  std::uint64_t position = 0;
  unsigned char header[kRecordHeaderSize];
  while (position + kRecordHeaderSize <= out.file_size) {
    const ssize_t header_read =
        ::pread(fd.get(), header, kRecordHeaderSize,
                static_cast<off_t>(position));
    if (header_read != static_cast<ssize_t>(kRecordHeaderSize)) {
      break;
    }
    const std::uint32_t total_len = internal::ReadU32(header);
    if (total_len < 20u) {
      break;
    }
    const std::uint64_t record_size =
        4u + static_cast<std::uint64_t>(total_len);
    if (record_size > kMaxRecordSize || record_size > out.file_size ||
        position + record_size > out.file_size) {
      break;
    }

    std::string buffer(static_cast<std::size_t>(record_size), '\0');
    const ssize_t body_read =
        ::pread(fd.get(), &buffer[0], buffer.size(),
                static_cast<off_t>(position));
    if (body_read != static_cast<ssize_t>(buffer.size())) {
      break;
    }
    const RecordDecodeOutcome decoded =
        DecodeRecord(buffer.data(), buffer.size());
    if (decoded.result != RecordDecodeResult::kOk ||
        decoded.consumed != buffer.size()) {
      break;
    }

    out.has_record = true;
    ++out.next_offset;
    out.valid_end = position + record_size;
    position = out.valid_end;
  }
  return out;
}

RecoveredPartition Unavailable(std::size_t segment_count,
                               const std::string& warning) {
  RecoveredPartition result;
  result.state = PartitionState::kUnavailable;
  result.segment_count = segment_count;
  result.warning = warning;
  return result;
}

}  // namespace

common::Status ListSegments(const std::string& partition_dir,
                            std::vector<SegmentFileInfo>* out) {
  if (out == nullptr) {
    return Status(StatusCode::kInternalError, "null segment list output");
  }
  out->clear();
  if (partition_dir.empty()) {
    return Status(StatusCode::kInternalError, "empty partition directory");
  }

  DIR* dir = ::opendir(partition_dir.c_str());
  if (dir == nullptr) {
    if (errno == ENOENT) {
      return Status();
    }
    return Status(StatusCode::kStorageIoError,
                  "cannot open partition directory: " + partition_dir);
  }

  struct dirent* entry = nullptr;
  while ((entry = ::readdir(dir)) != nullptr) {
    std::uint64_t base_offset = 0;
    if (!ParseSegmentName(entry->d_name, &base_offset)) {
      continue;
    }
    SegmentFileInfo info;
    info.base_offset = base_offset;
    info.log_path = internal::JoinPath(partition_dir, entry->d_name);
    struct stat st;
    if (::stat(info.log_path.c_str(), &st) == 0 && st.st_size >= 0) {
      info.file_size = static_cast<std::uint64_t>(st.st_size);
    }
    out->push_back(info);
  }
  ::closedir(dir);

  std::sort(out->begin(), out->end(),
            [](const SegmentFileInfo& a, const SegmentFileInfo& b) {
              return a.base_offset < b.base_offset;
            });
  return Status();
}

common::StatusOr<RecoveredPartition> RecoverPartition(
    const std::string& partition_dir, std::uint64_t segment_bytes) {
  if (partition_dir.empty()) {
    return StatusOr<RecoveredPartition>(
        Status(StatusCode::kInternalError, "empty partition directory"));
  }
  const Status dir_status = internal::EnsureDirectory(partition_dir);
  if (!dir_status.ok()) {
    return StatusOr<RecoveredPartition>(dir_status);
  }

  std::vector<SegmentFileInfo> files;
  const Status list_status = ListSegments(partition_dir, &files);
  if (!list_status.ok()) {
    return StatusOr<RecoveredPartition>(list_status);
  }

  // 04 §5.2 第 3 步：无分段 → next_offset = 0，创建 base=0 的空 active。
  if (files.empty()) {
    StatusOr<Segment> created =
        Segment::Create(partition_dir, 0u, segment_bytes);
    if (!created.ok()) {
      return StatusOr<RecoveredPartition>(created.status());
    }
    RecoveredPartition result;
    result.state = PartitionState::kReady;
    result.next_offset = 0u;
    result.segment_count = 1u;
    return StatusOr<RecoveredPartition>(result);
  }

  // 04 §5.2 第 2 步：首段 base 必须为 0。
  if (files.front().base_offset != 0u) {
    return StatusOr<RecoveredPartition>(Unavailable(
        files.size(),
        "base chain broken: first segment base is " +
            FormatBaseOffset(files.front().base_offset) + ", expected 0"));
  }

  RecoveredPartition result;
  std::uint64_t expected_base = 0u;

  for (std::size_t i = 0; i < files.size(); ++i) {
    const bool is_active = (i + 1u == files.size());
    if (files[i].base_offset != expected_base) {
      return StatusOr<RecoveredPartition>(Unavailable(
          files.size(), "base chain broken at " +
                            FormatBaseOffset(files[i].base_offset) +
                            ", expected " + FormatBaseOffset(expected_base)));
    }

    const ScanResult scan = ScanSegment(files[i].log_path, files[i].base_offset);
    if (!scan.readable) {
      return StatusOr<RecoveredPartition>(Unavailable(
          files.size(), "cannot read segment " +
                            FormatBaseOffset(files[i].base_offset)));
    }

    if (!is_active) {
      // S-11：封存段只读，出现残缺或空段即判定损坏，且不修改文件。
      if (!scan.has_record) {
        return StatusOr<RecoveredPartition>(Unavailable(
            files.size(), "sealed segment is empty: " +
                              FormatBaseOffset(files[i].base_offset)));
      }
      if (scan.valid_end != scan.file_size) {
        return StatusOr<RecoveredPartition>(Unavailable(
            files.size(), "sealed segment is corrupt: " +
                              FormatBaseOffset(files[i].base_offset)));
      }
      expected_base = scan.next_offset;
      continue;
    }

    // active 段：只截断残缺尾部（R7）。
    if (scan.valid_end != scan.file_size) {
      internal::ScopedFd fd(
          ::open(files[i].log_path.c_str(), O_WRONLY | O_CLOEXEC));
      if (!fd.valid() ||
          ::ftruncate(fd.get(), static_cast<off_t>(scan.valid_end)) != 0) {
        return StatusOr<RecoveredPartition>(Unavailable(
            files.size(), "cannot truncate active segment: " +
                              FormatBaseOffset(files[i].base_offset)));
      }
      result.truncated_bytes = scan.file_size - scan.valid_end;
    }

    StatusOr<Segment> active = Segment::Open(
        partition_dir, files[i].base_offset, scan.next_offset, segment_bytes);
    if (!active.ok()) {
      return StatusOr<RecoveredPartition>(active.status());
    }
    // 索引是派生数据；重建失败只降级为线性扫描，不影响分区可用性。
    (void)active.value().RebuildIndex();

    result.state = PartitionState::kReady;
    result.next_offset = scan.next_offset;
  }

  result.segment_count = files.size();
  return StatusOr<RecoveredPartition>(result);
}

common::StatusOr<std::uint64_t> AvailableBytes(const std::string& path) {
  if (path.empty()) {
    return StatusOr<std::uint64_t>(
        Status(StatusCode::kInternalError, "empty path for statvfs"));
  }
  struct statvfs vfs;
  if (::statvfs(path.c_str(), &vfs) != 0) {
    return StatusOr<std::uint64_t>(Status(
        StatusCode::kStorageIoError, "statvfs failed on " + path));
  }
  const std::uint64_t available =
      static_cast<std::uint64_t>(vfs.f_bavail) *
      static_cast<std::uint64_t>(vfs.f_frsize);
  return StatusOr<std::uint64_t>(available);
}

common::Status CheckDiskSpace(const std::string& path,
                              std::uint64_t required_bytes) {
  StatusOr<std::uint64_t> available = AvailableBytes(path);
  if (!available.ok()) {
    return available.status();
  }
  if (available.value() < required_bytes) {
    return Status(StatusCode::kDiskFull,
                  "insufficient disk space: available " +
                      std::to_string(available.value()) + " < required " +
                      std::to_string(required_bytes));
  }
  return Status();
}

}  // namespace storage
}  // namespace minikafka
