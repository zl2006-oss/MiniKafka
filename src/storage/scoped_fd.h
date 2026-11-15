#ifndef MINIKAFKA_STORAGE_SCOPED_FD_H_
#define MINIKAFKA_STORAGE_SCOPED_FD_H_

#include <unistd.h>

namespace minikafka {
namespace storage {
namespace internal {

// 存储模块内部临时 fd 的 RAII 持有者；权限仅限 src/storage。
class ScopedFd {
 public:
  explicit ScopedFd(int fd = -1) noexcept : fd_(fd) {}
  ~ScopedFd() { Reset(); }

  ScopedFd(ScopedFd&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }

  ScopedFd& operator=(ScopedFd&& other) noexcept {
    if (this != &other) {
      Reset();
      fd_ = other.fd_;
      other.fd_ = -1;
    }
    return *this;
  }

  ScopedFd(const ScopedFd&) = delete;
  ScopedFd& operator=(const ScopedFd&) = delete;

  bool valid() const noexcept { return fd_ >= 0; }
  int get() const noexcept { return fd_; }

  int Release() noexcept {
    const int fd = fd_;
    fd_ = -1;
    return fd;
  }

  void Reset(int fd = -1) noexcept {
    if (fd_ >= 0) {
      ::close(fd_);
    }
    fd_ = fd;
  }

 private:
  int fd_;
};

}  // namespace internal
}  // namespace storage
}  // namespace minikafka

#endif  // MINIKAFKA_STORAGE_SCOPED_FD_H_
