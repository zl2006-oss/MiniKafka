#include "minikafka/client/connection.h"

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>

namespace minikafka {
namespace client {

using common::Status;
using common::StatusCode;

namespace {

int MsUntil(int64_t deadline_ms) {
  const int64_t remaining = deadline_ms - NowMs();
  if (remaining <= 0) {
    return 0;
  }
  return remaining > 1000 ? 1000 : static_cast<int>(remaining);
}

}  // namespace

std::int64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

BrokerConnection::BrokerConnection(int timeout_ms)
    : timeout_ms_(timeout_ms),
      port_(0),
      next_request_id_(1),
      connected_(false) {}

common::Status BrokerConnection::Connect(const std::string& host,
                                         std::uint16_t port) {
  host_ = host;
  port_ = port;
  Close();
  return ConnectInternal(timeout_ms_);
}

common::Status BrokerConnection::Reconnect() {
  if (host_.empty() || port_ == 0) {
    return Status(StatusCode::kBrokerUnavailable, "no previous address");
  }
  Close();
  return ConnectInternal(timeout_ms_);
}

void BrokerConnection::Close() {
  socket_.Close();
  decoder_.Reset();
  connected_ = false;
}

common::Status BrokerConnection::ConnectInternal(int timeout_ms) {
  Status status = net::Socket::CreateTcp(&socket_);
  if (!status.ok()) {
    return status;
  }
  status = socket_.SetNonBlocking();
  if (!status.ok()) {
    Close();
    return status;
  }

  sockaddr_in address;
  std::memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_port = htons(port_);
  if (::inet_pton(AF_INET, host_.c_str(), &address.sin_addr) != 1) {
    Close();
    return Status(StatusCode::kBrokerUnavailable,
                  "invalid broker address: " + host_);
  }

  bool in_progress = false;
  status = socket_.Connect(address, &in_progress);
  if (!status.ok()) {
    Close();
    return status;
  }

  if (in_progress) {
    const int64_t deadline = NowMs() + timeout_ms;
    while (true) {
      const int wait_ms = MsUntil(deadline);
      if (wait_ms == 0) {
        Close();
        return Status(StatusCode::kBrokerUnavailable,
                      "connect timed out: " + host_ + ":" +
                          std::to_string(port_));
      }
      pollfd descriptor;
      descriptor.fd = socket_.Fd();
      descriptor.events = POLLOUT;
      descriptor.revents = 0;
      const int ready = ::poll(&descriptor, 1, wait_ms);
      if (ready < 0 && errno == EINTR) {
        continue;
      }
      if (ready == 0) {
        continue;
      }
      if (ready < 0) {
        Close();
        return Status(StatusCode::kBrokerUnavailable, "connect failed");
      }
      break;
    }
    int error = 0;
    socklen_t length = sizeof(error);
    if (::getsockopt(socket_.Fd(), SOL_SOCKET, SO_ERROR, &error, &length) != 0 ||
        error != 0) {
      Close();
      return Status(StatusCode::kBrokerUnavailable,
                    "connect refused: " + host_ + ":" +
                        std::to_string(port_));
    }
  }

  connected_ = true;
  return Status();
}

common::Status BrokerConnection::WriteAll(const std::string& bytes,
                                          int64_t deadline_ms) {
  std::size_t written = 0;
  while (written < bytes.size()) {
    const ssize_t count =
        socket_.Write(bytes.data() + written, bytes.size() - written);
    if (count > 0) {
      written += static_cast<std::size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) {
      continue;
    }
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      const int wait_ms = MsUntil(deadline_ms);
      if (wait_ms == 0) {
        return Status(StatusCode::kBrokerUnavailable, "write timed out");
      }
      pollfd descriptor;
      descriptor.fd = socket_.Fd();
      descriptor.events = POLLOUT;
      descriptor.revents = 0;
      (void)::poll(&descriptor, 1, wait_ms);
      continue;
    }
    connected_ = false;
    return Status(StatusCode::kBrokerUnavailable,
                  "connection closed while writing");
  }
  return Status();
}

common::Status BrokerConnection::ReadFrame(protocol::Frame* frame,
                                           int64_t deadline_ms) {
  while (true) {
    bool has_frame = false;
    const Status next = decoder_.Next(frame, &has_frame);
    if (!next.ok()) {
      connected_ = false;
      return next;
    }
    if (has_frame) {
      return Status();
    }

    const int wait_ms = MsUntil(deadline_ms);
    if (wait_ms == 0) {
      return Status(StatusCode::kBrokerUnavailable, "read timed out");
    }
    pollfd descriptor;
    descriptor.fd = socket_.Fd();
    descriptor.events = POLLIN;
    descriptor.revents = 0;
    const int ready = ::poll(&descriptor, 1, wait_ms);
    if (ready < 0 && errno == EINTR) {
      continue;
    }
    if (ready == 0) {
      continue;
    }
    if (ready < 0) {
      connected_ = false;
      return Status(StatusCode::kBrokerUnavailable, "poll failed on read");
    }

    char buffer[64u * 1024u];
    const ssize_t count = socket_.Read(buffer, sizeof(buffer));
    if (count > 0) {
      const Status feed =
          decoder_.Feed(buffer, static_cast<std::size_t>(count));
      if (!feed.ok()) {
        connected_ = false;
        return feed;
      }
      continue;
    }
    if (count < 0 && errno == EINTR) {
      continue;
    }
    connected_ = false;
    return Status(StatusCode::kBrokerUnavailable,
                  "connection closed by broker");
  }
}

common::Status BrokerConnection::Call(std::uint16_t msg_type,
                                      const std::string& payload,
                                      protocol::Frame* response) {
  if (response == nullptr) {
    return Status(StatusCode::kInternalError, "null response output");
  }
  if (!connected()) {
    return Status(StatusCode::kBrokerUnavailable, "not connected");
  }

  protocol::Frame request;
  request.msg_type = msg_type;
  request.request_id = next_request_id_++;
  request.payload = payload;

  std::string bytes;
  const Status encode_status = protocol::EncodeFrame(request, &bytes);
  if (!encode_status.ok()) {
    return encode_status;
  }

  const int64_t deadline = NowMs() + timeout_ms_;
  Status status = WriteAll(bytes, deadline);
  if (!status.ok()) {
    return status;
  }

  // 单条在途：跳过不属于本次请求的帧（正常情况下不会出现）。
  while (true) {
    status = ReadFrame(response, deadline);
    if (!status.ok()) {
      return status;
    }
    if (response->request_id == request.request_id) {
      return Status();
    }
  }
}

}  // namespace client
}  // namespace minikafka
