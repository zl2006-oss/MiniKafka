#ifndef MINIKAFKA_STORAGE_FILE_UTIL_H_
#define MINIKAFKA_STORAGE_FILE_UTIL_H_

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>

#include "minikafka/common/status.h"
#include "scoped_fd.h"

namespace minikafka {
namespace storage {
namespace internal {

inline std::string JoinPath(const std::string& base,
                            const std::string& child) {
  if (base.empty()) {
    return child;
  }
  if (base.back() == '/') {
    return base + child;
  }
  return base + "/" + child;
}

inline bool IsDirectory(const std::string& path) {
  struct stat st;
  return ::stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

inline common::Status EnsureDirectory(const std::string& path) {
  if (path.empty()) {
    return common::Status(common::StatusCode::kInternalError,
                          "empty directory path");
  }
  if (IsDirectory(path)) {
    return common::Status();
  }

  std::string current;
  std::size_t pos = 0;
  if (path[0] == '/') {
    current = "/";
    pos = 1;
  }
  while (pos <= path.size()) {
    const std::size_t slash = path.find('/', pos);
    const std::size_t end = slash == std::string::npos ? path.size() : slash;
    if (end > pos) {
      const std::string part = path.substr(pos, end - pos);
      if (!current.empty() && current.back() != '/') {
        current.push_back('/');
      }
      current += part;
      if (::mkdir(current.c_str(), 0755) != 0 && errno != EEXIST) {
        return common::Status(common::StatusCode::kStorageIoError,
                              "cannot create directory: " + current);
      }
      if (!IsDirectory(current)) {
        return common::Status(common::StatusCode::kStorageIoError,
                              "path is not a directory: " + current);
      }
    }
    if (slash == std::string::npos) {
      break;
    }
    pos = slash + 1;
  }
  return common::Status();
}

inline bool WriteAll(int fd, const char* data, std::size_t size) {
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

inline bool ReadAll(int fd, char* data, std::size_t size) {
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

inline common::Status ReadWholeFile(const std::string& path,
                                    std::string* data, bool* exists) {
  if (data == nullptr || exists == nullptr) {
    return common::Status(common::StatusCode::kInternalError,
                          "null file buffer");
  }
  data->clear();
  *exists = false;

  ScopedFd fd(::open(path.c_str(), O_RDONLY | O_CLOEXEC));
  if (!fd.valid()) {
    if (errno == ENOENT) {
      return common::Status();
    }
    return common::Status(common::StatusCode::kStorageIoError,
                          "cannot open file: " + path);
  }
  *exists = true;

  struct stat st;
  if (::fstat(fd.get(), &st) != 0 || st.st_size < 0) {
    return common::Status(common::StatusCode::kStorageIoError,
                          "cannot stat file: " + path);
  }
  const std::uint64_t file_size = static_cast<std::uint64_t>(st.st_size);
  if (file_size >
      static_cast<std::uint64_t>(
          (std::numeric_limits<std::size_t>::max)())) {
    return common::Status(common::StatusCode::kStorageIoError,
                          "file is too large: " + path);
  }
  data->resize(static_cast<std::size_t>(file_size));
  if (!ReadAll(fd.get(), data->empty() ? nullptr : &(*data)[0], data->size())) {
    return common::Status(common::StatusCode::kStorageIoError,
                          "cannot read file: " + path);
  }
  return common::Status();
}

inline common::Status WriteFileAtomically(const std::string& path,
                                          const std::string& tmp_path,
                                          const std::string& data) {
  if (path.empty() || tmp_path.empty()) {
    return common::Status(common::StatusCode::kInternalError,
                          "empty atomic write path");
  }

  ScopedFd fd(::open(tmp_path.c_str(),
                     O_CREAT | O_TRUNC | O_WRONLY | O_CLOEXEC, 0644));
  if (!fd.valid()) {
    return common::Status(common::StatusCode::kStorageIoError,
                          "cannot open tmp file: " + tmp_path);
  }
  const bool wrote = WriteAll(fd.get(), data.empty() ? nullptr : &data[0],
                              data.size());
  const int raw_fd = fd.Release();
  const int close_result = ::close(raw_fd);
  if (!wrote || close_result != 0) {
    ::unlink(tmp_path.c_str());
    return common::Status(common::StatusCode::kStorageIoError,
                          "cannot write tmp file: " + tmp_path);
  }
  if (::rename(tmp_path.c_str(), path.c_str()) != 0) {
    ::unlink(tmp_path.c_str());
    return common::Status(common::StatusCode::kStorageIoError,
                          "cannot rename tmp file: " + tmp_path);
  }
  return common::Status();
}

}  // namespace internal
}  // namespace storage
}  // namespace minikafka

#endif  // MINIKAFKA_STORAGE_FILE_UTIL_H_
