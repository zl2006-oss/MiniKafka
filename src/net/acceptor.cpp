#include "minikafka/net/acceptor.h"

#include <arpa/inet.h>
#include <sys/epoll.h>

#include <utility>

namespace minikafka {
namespace net {

Acceptor::Acceptor(EventLoop* loop, const sockaddr_in& listen_address)
    : loop_(loop),
      listen_address_(listen_address),
      socket_(),
      callback_(),
      started_(false),
      port_(0) {}

Acceptor::~Acceptor() { Close(); }

Status Acceptor::Start(NewConnectionCallback callback, int backlog) {
  if (started_) {
    return Status(StatusCode::kInternalError, "acceptor already started");
  }
  if (loop_ == nullptr) {
    return Status(StatusCode::kInternalError, "null event loop");
  }

  Socket socket;
  Status status = Socket::CreateTcp(&socket);
  if (!status.ok()) {
    return status;
  }

  status = socket.SetReuseAddr();
  if (!status.ok()) {
    return status;
  }

  status = socket.Bind(listen_address_);
  if (!status.ok()) {
    return status;
  }

  status = socket.Listen(backlog);
  if (!status.ok()) {
    return status;
  }

  sockaddr_in local_address;
  status = socket.LocalAddress(&local_address);
  if (!status.ok()) {
    return status;
  }

  socket_ = std::move(socket);
  callback_ = std::move(callback);

  status = loop_->AddFd(
      socket_.Fd(), EPOLLIN,
      [this](std::uint32_t events) {
        (void)events;
        (void)HandleRead();
      });
  if (!status.ok()) {
    callback_ = nullptr;
    socket_.Close();
    return status;
  }

  listen_address_ = local_address;
  port_ = ntohs(local_address.sin_port);
  started_ = true;
  return Status();
}

void Acceptor::Close() {
  if (started_ && loop_ != nullptr) {
    (void)loop_->RemoveFd(socket_.Fd());
  }
  started_ = false;
  port_ = 0;
  callback_ = nullptr;
  socket_.Close();
}

Status Acceptor::HandleRead() {
  if (!started_ || !socket_.Valid()) {
    return Status();
  }

  while (true) {
    Socket client;
    sockaddr_in peer;
    bool accepted = false;
    const Status status = socket_.Accept(&client, &peer, &accepted);
    if (!status.ok()) {
      return status;
    }
    if (!accepted) {
      break;
    }
    if (callback_) {
      callback_(std::move(client), peer);
    }
  }
  return Status();
}

}  // namespace net
}  // namespace minikafka
