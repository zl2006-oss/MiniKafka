#include "minikafka/net/connection.h"

#include <sys/epoll.h>

#include <cerrno>
#include <utility>

namespace minikafka {
namespace net {
namespace {

constexpr std::size_t kReadChunkSize = 64u * 1024u;
constexpr std::size_t kCompactThreshold = 64u * 1024u;

std::uint32_t BaseEvents() {
  std::uint32_t events = EPOLLIN;
#ifdef EPOLLRDHUP
  events |= EPOLLRDHUP;
#endif
  return events;
}

}  // namespace

Connection::Connection(EventLoop* loop, ConnectionId id, Socket socket,
                       const sockaddr_in& peer,
                       std::size_t write_high_watermark)
    : loop_(loop),
      id_(id),
      socket_(std::move(socket)),
      peer_(peer),
      decoder_(),
      output_(),
      write_offset_(0),
      write_high_watermark_(write_high_watermark),
      closed_(false),
      peer_closed_(false),
      interest_(0),
      frame_callback_(),
      close_callback_() {}

Connection::~Connection() {
  if (socket_.Valid()) {
    socket_.Close();
  }
}

Status Connection::Start() {
  if (loop_ == nullptr) {
    return Status(StatusCode::kInternalError, "null event loop");
  }
  if (!loop_->IsInLoopThread()) {
    return Status(StatusCode::kInternalError,
                  "connection start must run in loop thread");
  }
  if (!socket_.Valid()) {
    return Status(StatusCode::kInternalError, "invalid socket");
  }

  interest_ = BaseEvents();
  const Status status = loop_->AddFd(
      socket_.Fd(), interest_,
      [this](std::uint32_t events) { HandleEvents(events); });
  if (!status.ok()) {
    socket_.Close();
    closed_ = true;
    return status;
  }
  return Status();
}

Status Connection::Send(const std::string& data) {
  if (data.empty()) {
    return Status();
  }
  if (loop_ == nullptr || !loop_->IsInLoopThread()) {
    return Status(StatusCode::kInternalError,
                  "send must run in connection loop");
  }
  if (closed_) {
    return Status(StatusCode::kInternalError, "connection closed");
  }

  if (write_offset_ == output_.size()) {
    output_.clear();
    write_offset_ = 0;
  } else if (write_offset_ >= kCompactThreshold) {
    output_.erase(0, write_offset_);
    write_offset_ = 0;
  }

  const std::size_t pending = PendingBytes();
  if (pending >= write_high_watermark_ ||
      data.size() > write_high_watermark_ - pending) {
    return Status(StatusCode::kInternalError,
                  "write buffer high watermark exceeded");
  }

  output_.append(data);
  return HandleWrite();
}

void Connection::Close() {
  if (closed_) {
    return;
  }
  if (loop_ != nullptr && !loop_->IsInLoopThread()) {
    return;
  }

  closed_ = true;
  if (loop_ != nullptr) {
    (void)loop_->RemoveFd(socket_.Fd());
  }
  socket_.Close();
  output_.clear();
  write_offset_ = 0;
  decoder_.Reset();
  frame_callback_ = FrameCallback();

  CloseCallback callback = std::move(close_callback_);
  close_callback_ = CloseCallback();
  if (callback) {
    callback(id_);
  }
}

void Connection::HandleEvents(std::uint32_t events) {
  if (closed_) {
    return;
  }

  const bool error = (events & static_cast<std::uint32_t>(EPOLLERR)) != 0u;
  const bool hangup = (events & static_cast<std::uint32_t>(EPOLLHUP)) != 0u;
  bool read_hangup = false;
#ifdef EPOLLRDHUP
  read_hangup =
      (events & static_cast<std::uint32_t>(EPOLLRDHUP)) != 0u;
#endif

  if ((events & static_cast<std::uint32_t>(EPOLLIN)) != 0u || hangup ||
      read_hangup) {
    (void)HandleRead();
    if (closed_) {
      return;
    }
  }
  if ((events & static_cast<std::uint32_t>(EPOLLOUT)) != 0u) {
    (void)HandleWrite();
    if (closed_) {
      return;
    }
  }
  if (error) {
    Close();
    return;
  }
  if (hangup) {
    Close();
    return;
  }
}

Status Connection::HandleRead() {
  if (closed_ || peer_closed_) {
    return Status();
  }

  char buffer[kReadChunkSize];
  while (true) {
    const ssize_t count = socket_.Read(buffer, sizeof(buffer));
    if (count > 0) {
      Status status = decoder_.Feed(buffer, static_cast<std::size_t>(count));
      if (!status.ok()) {
        Close();
        return status;
      }
      status = DispatchFrames();
      if (!status.ok()) {
        Close();
        return status;
      }
      if (closed_) {
        return Status();
      }
      continue;
    }
    if (count == 0) {
      peer_closed_ = true;
      break;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      break;
    }
    Close();
    return Status(StatusCode::kInternalError, "read failed");
  }

  if (closed_) {
    return Status();
  }
  if (peer_closed_ && PendingBytes() == 0) {
    Close();
  } else if (peer_closed_) {
    UpdateInterest();
  }
  return Status();
}

Status Connection::HandleWrite() {
  if (closed_) {
    return Status();
  }

  while (true) {
    const std::size_t pending = PendingBytes();
    if (pending == 0) {
      break;
    }
    const ssize_t count =
        socket_.Write(output_.data() + write_offset_, pending);
    if (count > 0) {
      write_offset_ += static_cast<std::size_t>(count);
      if (write_offset_ == output_.size()) {
        output_.clear();
        write_offset_ = 0;
      } else if (write_offset_ >= kCompactThreshold) {
        output_.erase(0, write_offset_);
        write_offset_ = 0;
      }
      continue;
    }
    if (count < 0) {
      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        break;
      }
      Close();
      return Status(StatusCode::kInternalError, "write failed");
    }
    Close();
    return Status(StatusCode::kInternalError, "write returned zero");
  }

  UpdateInterest();
  if (closed_) {
    return Status();
  }
  if (peer_closed_ && PendingBytes() == 0) {
    Close();
  }
  return Status();
}

Status Connection::DispatchFrames() {
  while (true) {
    protocol::Frame frame;
    bool has_frame = false;
    Status status = decoder_.Next(&frame, &has_frame);
    if (!status.ok()) {
      return status;
    }
    if (!has_frame) {
      break;
    }
    if (frame_callback_) {
      frame_callback_(id_, frame);
    }
    if (closed_) {
      break;
    }
  }
  return Status();
}

void Connection::UpdateInterest() {
  if (closed_ || loop_ == nullptr || !socket_.Valid()) {
    return;
  }
  if (peer_closed_ && PendingBytes() == 0) {
    Close();
    return;
  }

  std::uint32_t events = 0;
  if (!peer_closed_) {
    events |= BaseEvents();
  }
  if (PendingBytes() > 0) {
    events |= static_cast<std::uint32_t>(EPOLLOUT);
  }
  if (events == interest_) {
    return;
  }

  const Status status = loop_->ModifyFd(socket_.Fd(), events);
  if (!status.ok()) {
    Close();
    return;
  }
  interest_ = events;
}

std::size_t Connection::PendingBytes() const {
  return output_.size() - write_offset_;
}

}  // namespace net
}  // namespace minikafka
