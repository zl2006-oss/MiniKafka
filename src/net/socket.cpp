#include "minikafka/net/socket.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <utility>

namespace minikafka {
namespace net {
namespace {

int SendFlags() {
#ifdef MSG_NOSIGNAL
  return MSG_NOSIGNAL;
#else
  return 0;
#endif
}

}  // namespace

Status Socket::CreateTcp(Socket* socket) {
  if (socket == nullptr) {
    return Status(StatusCode::kInternalError, "null socket output");
  }
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return Status(StatusCode::kInternalError, "socket() failed");
  }
  Socket created(fd);
  const Status status = created.SetNonBlocking();
  if (!status.ok()) {
    return status;
  }
  *socket = std::move(created);
  return Status();
}

Status Socket::SetNonBlocking() {
  if (!fd_.Valid()) {
    return Status(StatusCode::kInternalError, "invalid socket");
  }
  const int flags = ::fcntl(fd_.Get(), F_GETFL, 0);
  if (flags < 0) {
    return Status(StatusCode::kInternalError, "fcntl(F_GETFL) failed");
  }
  if (::fcntl(fd_.Get(), F_SETFL, flags | O_NONBLOCK) < 0) {
    return Status(StatusCode::kInternalError, "fcntl(F_SETFL) failed");
  }
  const int descriptor_flags = ::fcntl(fd_.Get(), F_GETFD, 0);
  if (descriptor_flags >= 0) {
    ::fcntl(fd_.Get(), F_SETFD, descriptor_flags | FD_CLOEXEC);
  }
  return Status();
}

Status Socket::SetReuseAddr() {
  if (!fd_.Valid()) {
    return Status(StatusCode::kInternalError, "invalid socket");
  }
  int value = 1;
  if (::setsockopt(fd_.Get(), SOL_SOCKET, SO_REUSEADDR, &value,
                   sizeof(value)) != 0) {
    return Status(StatusCode::kInternalError, "setsockopt(SO_REUSEADDR) failed");
  }
  return Status();
}

Status Socket::Bind(const sockaddr_in& address) {
  if (!fd_.Valid()) {
    return Status(StatusCode::kInternalError, "invalid socket");
  }
  if (::bind(fd_.Get(), reinterpret_cast<const sockaddr*>(&address),
             sizeof(address)) != 0) {
    return Status(StatusCode::kInternalError, "bind() failed");
  }
  return Status();
}

Status Socket::Listen(int backlog) {
  if (!fd_.Valid()) {
    return Status(StatusCode::kInternalError, "invalid socket");
  }
  if (::listen(fd_.Get(), backlog) != 0) {
    return Status(StatusCode::kInternalError, "listen() failed");
  }
  return Status();
}

Status Socket::LocalAddress(sockaddr_in* address) const {
  if (address == nullptr) {
    return Status(StatusCode::kInternalError, "null address output");
  }
  if (!fd_.Valid()) {
    return Status(StatusCode::kInternalError, "invalid socket");
  }
  std::memset(address, 0, sizeof(*address));
  socklen_t length = sizeof(*address);
  if (::getsockname(fd_.Get(), reinterpret_cast<sockaddr*>(address),
                    &length) != 0) {
    return Status(StatusCode::kInternalError, "getsockname() failed");
  }
  return Status();
}

Status Socket::Accept(Socket* socket, sockaddr_in* peer, bool* accepted) const {
  if (socket == nullptr || accepted == nullptr) {
    return Status(StatusCode::kInternalError, "null accept output");
  }
  *accepted = false;
  if (!fd_.Valid()) {
    return Status(StatusCode::kInternalError, "invalid socket");
  }

  sockaddr_in peer_address;
  std::memset(&peer_address, 0, sizeof(peer_address));
  socklen_t peer_length = sizeof(peer_address);
  int client_fd = -1;
  do {
    client_fd = ::accept(fd_.Get(),
                         reinterpret_cast<sockaddr*>(&peer_address),
                         &peer_length);
  } while (client_fd < 0 && errno == EINTR);

  if (client_fd < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return Status();
    }
    return Status(StatusCode::kInternalError, "accept() failed");
  }

  Socket client(client_fd);
  const Status status = client.SetNonBlocking();
  if (!status.ok()) {
    return status;
  }
  if (peer != nullptr) {
    *peer = peer_address;
  }
  *socket = std::move(client);
  *accepted = true;
  return Status();
}

Status Socket::Connect(const sockaddr_in& address, bool* in_progress) {
  if (in_progress == nullptr) {
    return Status(StatusCode::kInternalError, "null connect output");
  }
  *in_progress = false;
  if (!fd_.Valid()) {
    return Status(StatusCode::kInternalError, "invalid socket");
  }

  int result = -1;
  do {
    result = ::connect(fd_.Get(),
                       reinterpret_cast<const sockaddr*>(&address),
                       sizeof(address));
  } while (result < 0 && errno == EINTR);

  if (result == 0) {
    return Status();
  }
  const int error = errno;
  if (error == EISCONN) {
    return Status();
  }
  if (error == EINPROGRESS || error == EALREADY) {
    *in_progress = true;
    return Status();
  }
  return Status(StatusCode::kInternalError, "connect() failed");
}

ssize_t Socket::Read(void* buffer, std::size_t size) {
  if (!fd_.Valid()) {
    errno = EBADF;
    return -1;
  }
  ssize_t result = -1;
  do {
    result = ::recv(fd_.Get(), buffer, size, 0);
  } while (result < 0 && errno == EINTR);
  return result;
}

ssize_t Socket::Write(const void* buffer, std::size_t size) {
  if (!fd_.Valid()) {
    errno = EBADF;
    return -1;
  }
  ssize_t result = -1;
  do {
    result = ::send(fd_.Get(), buffer, size, SendFlags());
  } while (result < 0 && errno == EINTR);
  return result;
}

Status Socket::ShutdownWrite() {
  if (!fd_.Valid()) {
    return Status(StatusCode::kInternalError, "invalid socket");
  }
  if (::shutdown(fd_.Get(), SHUT_WR) != 0) {
    return Status(StatusCode::kInternalError, "shutdown(SHUT_WR) failed");
  }
  return Status();
}

}  // namespace net
}  // namespace minikafka
