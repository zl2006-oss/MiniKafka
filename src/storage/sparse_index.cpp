#include "minikafka/storage/sparse_index.h"

#include "scoped_fd.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace minikafka {
namespace storage {

using common::Status;
using common::StatusCode;
using common::StatusOr;

namespace {

const std::size_t kIndexEntrySize = 12u;

void AppendU32(std::string* out, std::uint32_t value) {
  out->push_back(static_cast<char>((value >> 24) & 0xFFu));
  out->push_back(static_cast<char>((value >> 16) & 0xFFu));
  out->push_back(static_cast<char>((value >> 8) & 0xFFu));
  out->push_back(static_cast<char>(value & 0xFFu));
}

void AppendU64(std::string* out, std::uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xFFu));
  }
}

std::uint32_t ReadU32(const unsigned char* data) {
  return (static_cast<std::uint32_t>(data[0]) << 24) |
         (static_cast<std::uint32_t>(data[1]) << 16) |
         (static_cast<std::uint32_t>(data[2]) << 8) |
         static_cast<std::uint32_t>(data[3]);
}

std::uint64_t ReadU64(const unsigned char* data) {
  std::uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value = (value << 8) | static_cast<std::uint64_t>(data[i]);
  }
  return value;
}

bool WriteAll(int fd, const char* data, std::size_t size) {
  std::size_t done = 0;
  while (done < size) {
    const ssize_t written = ::write(fd, data + done, size - done);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (written == 0) {
      return false;
    }
    done += static_cast<std::size_t>(written);
  }
  return true;
}

bool ReadAll(int fd, char* data, std::size_t size) {
  std::size_t done = 0;
  while (done < size) {
    const ssize_t n = ::read(fd, data + done, size - done);
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

bool ValidateEntries(const std::vector<IndexEntry>& entries,
                     std::uint64_t segment_size,
                     std::uint64_t base_offset) {
  if (entries.empty()) {
    return segment_size == 0u;
  }
  if (entries.front().offset != base_offset ||
      entries.front().position != 0u) {
    return false;
  }
  for (std::size_t i = 0; i < entries.size(); ++i) {
    if (static_cast<std::uint64_t>(entries[i].position) >= segment_size) {
      return false;
    }
    if (i == 0u) {
      continue;
    }
    if (entries[i].offset <= entries[i - 1u].offset ||
        entries[i].position <= entries[i - 1u].position) {
      return false;
    }
  }
  return true;
}

}  // namespace

SparseIndex::SparseIndex(std::string path)
    : path_(std::move(path)), valid_(false) {}

void SparseIndex::ResetForNew() {
  entries_.clear();
  valid_ = true;
}

void SparseIndex::Load(std::uint64_t segment_size,
                       std::uint64_t base_offset) {
  entries_.clear();
  valid_ = false;

  internal::ScopedFd fd(::open(path_.c_str(), O_RDONLY | O_CLOEXEC));
  if (!fd.valid()) {
    return;
  }
  struct stat st;
  if (::fstat(fd.get(), &st) != 0 || st.st_size < 0) {
    return;
  }
  const std::uint64_t file_size = static_cast<std::uint64_t>(st.st_size);
  if (file_size % kIndexEntrySize != 0u ||
      file_size / kIndexEntrySize >
          static_cast<std::uint64_t>(
              (std::numeric_limits<std::size_t>::max)())) {
    return;
  }
  if (file_size == 0u) {
    valid_ = (segment_size == 0u);
    return;
  }

  std::string data(static_cast<std::size_t>(file_size), '\0');
  if (!ReadAll(fd.get(), &data[0], data.size())) {
    return;
  }
  fd.Reset();

  std::vector<IndexEntry> entries;
  entries.reserve(data.size() / kIndexEntrySize);
  const unsigned char* bytes =
      reinterpret_cast<const unsigned char*>(data.data());
  for (std::size_t pos = 0; pos < data.size(); pos += kIndexEntrySize) {
    IndexEntry entry;
    entry.offset = ReadU64(bytes + pos);
    entry.position = ReadU32(bytes + pos + 8u);
    entries.push_back(entry);
  }
  if (!ValidateEntries(entries, segment_size, base_offset)) {
    return;
  }
  entries_.swap(entries);
  valid_ = true;
}

Status SparseIndex::Store(const std::vector<IndexEntry>& entries,
                          std::uint64_t segment_size,
                          std::uint64_t base_offset) {
  if (!ValidateEntries(entries, segment_size, base_offset)) {
    return Status(StatusCode::kInternalError,
                  "invalid sparse index entries");
  }

  std::string data;
  data.reserve(entries.size() * kIndexEntrySize);
  for (std::size_t i = 0; i < entries.size(); ++i) {
    AppendU64(&data, entries[i].offset);
    AppendU32(&data, entries[i].position);
  }

  const std::string tmp_path = path_ + ".tmp";
  internal::ScopedFd fd(::open(tmp_path.c_str(),
                               O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC,
                               0644));
  if (!fd.valid()) {
    return Status(StatusCode::kStorageIoError,
                  "cannot open sparse index tmp file");
  }
  const bool wrote = WriteAll(fd.get(), data.empty() ? nullptr : &data[0],
                              data.size());
  const int raw_fd = fd.Release();
  const int close_result = ::close(raw_fd);
  if (!wrote || close_result != 0) {
    ::unlink(tmp_path.c_str());
    return Status(StatusCode::kStorageIoError,
                  "cannot write sparse index tmp file");
  }
  if (::rename(tmp_path.c_str(), path_.c_str()) != 0) {
    ::unlink(tmp_path.c_str());
    return Status(StatusCode::kStorageIoError,
                  "cannot rename sparse index");
  }

  entries_ = entries;
  valid_ = true;
  return Status();
}

Status SparseIndex::AppendEntry(std::uint64_t offset,
                                std::uint32_t position) {
  if (!valid_) {
    return Status(StatusCode::kInternalError, "sparse index is invalid");
  }
  if (entries_.empty()) {
    if (position != 0u) {
      return Status(StatusCode::kInternalError,
                    "first checkpoint must be at position 0");
    }
  } else if (offset <= entries_.back().offset ||
             position <= entries_.back().position) {
    return Status(StatusCode::kInternalError,
                  "sparse index entry is not increasing");
  }

  std::string data;
  data.reserve(kIndexEntrySize);
  AppendU64(&data, offset);
  AppendU32(&data, position);

  internal::ScopedFd fd(::open(path_.c_str(),
                               O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC,
                               0644));
  if (!fd.valid()) {
    return Status(StatusCode::kStorageIoError,
                  "cannot open sparse index for append");
  }
  const bool wrote = WriteAll(fd.get(), &data[0], data.size());
  const int raw_fd = fd.Release();
  const int close_result = ::close(raw_fd);
  if (!wrote || close_result != 0) {
    return Status(StatusCode::kStorageIoError,
                  "cannot append sparse index entry");
  }

  IndexEntry entry;
  entry.offset = offset;
  entry.position = position;
  entries_.push_back(entry);
  return Status();
}

bool SparseIndex::FindCheckpoint(std::uint64_t target,
                                 IndexEntry* entry) const {
  if (entry == nullptr || !valid_ || entries_.empty()) {
    return false;
  }
  std::size_t low = 0;
  std::size_t high = entries_.size();
  while (low < high) {
    const std::size_t mid = low + (high - low) / 2u;
    if (entries_[mid].offset <= target) {
      low = mid + 1u;
    } else {
      high = mid;
    }
  }
  if (low == 0u) {
    return false;
  }
  *entry = entries_[low - 1u];
  return true;
}

void SparseIndex::Invalidate() {
  entries_.clear();
  valid_ = false;
}

}  // namespace storage
}  // namespace minikafka
