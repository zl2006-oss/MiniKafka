#ifndef MINIKAFKA_NET_SOCKET_H_
#define MINIKAFKA_NET_SOCKET_H_

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstddef>

#include "minikafka/common/status.h"

namespace minikafka {
namespace net {

using common::Status;
using common::StatusCode;

// 裸 fd 的 RAII 包装；只允许移动，禁止复制。
class UniqueFd {
 public:
  UniqueFd() = default;
  explicit UniqueFd(int fd) : fd_(fd) {}
  ~UniqueFd() { Reset(); }

  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;

  UniqueFd(UniqueFd&& other) noexcept : fd_(other.Release()) {}
  UniqueFd& operator=(UniqueFd&& other) noexcept {
    if (this != &other) {
      Reset(other.Release());
    }
    return *this;
  }

  int Get() const { return fd_; }
  bool Valid() const { return fd_ >= 0; }
  int Release() {
    const int fd = fd_;
    fd_ = -1;
    return fd;
  }
  void Reset(int fd = -1) {
    if (fd_ >= 0) {
      ::close(fd_);
    }
    fd_ = fd;
  }

 private:
  int fd_ = -1;
};

// 非阻塞 TCP socket；创建后需显式 SetReuseAddr/Bind/Listen 或 Connect。
class Socket {
 public:
  Socket() = default;
  explicit Socket(int fd) : fd_(fd) {}
  ~Socket() = default;

  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;
  Socket(Socket&&) noexcept = default;
  Socket& operator=(Socket&&) noexcept = default;

  static Status CreateTcp(Socket* socket);

  Status SetNonBlocking();
  Status SetReuseAddr();
  Status Bind(const sockaddr_in& address);
  Status Listen(int backlog);
  Status LocalAddress(sockaddr_in* address) const;

  // accepted=false 表示当前无可 accept 连接（EAGAIN/EINTR）。
  Status Accept(Socket* socket, sockaddr_in* peer, bool* accepted) const;

  // in_progress=true 表示非阻塞 connect 已发起，需等待可写事件。
  Status Connect(const sockaddr_in& address, bool* in_progress);

  ssize_t Read(void* buffer, std::size_t size);
  ssize_t Write(const void* buffer, std::size_t size);
  Status ShutdownWrite();

  int Fd() const { return fd_.Get(); }
  bool Valid() const { return fd_.Valid(); }
  void Close() { fd_.Reset(); }

 private:
  UniqueFd fd_;
};

}  // namespace net
}  // namespace minikafka

#endif  // MINIKAFKA_NET_SOCKET_H_
