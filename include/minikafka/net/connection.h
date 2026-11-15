#ifndef MINIKAFKA_NET_CONNECTION_H_
#define MINIKAFKA_NET_CONNECTION_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <utility>

#include "minikafka/common/status.h"
#include "minikafka/net/event_loop.h"
#include "minikafka/net/socket.h"
#include "minikafka/protocol/frame.h"

namespace minikafka {
namespace net {

using common::Status;
using common::StatusCode;

using ConnectionId = std::uint64_t;

// 写缓冲上限；Send 在追加后超过该值时拒绝并返回背压错误。
// 至少容纳一个 8 MiB 协议帧（05 §2 帧上限）后再触发背压。
constexpr std::size_t kDefaultConnectionWriteHighWatermark =
    8u * 1024u * 1024u + 4096u;

// 连接对象由所属 EventLoop 独占；所有方法必须在 loop 线程调用。
// 跨线程写只能通过 ReactorPool::Send(ConnectionId, ...) 排队，不持有 Connection 引用。
class Connection {
 public:
  using FrameCallback =
      std::function<void(ConnectionId, const protocol::Frame&)>;
  using CloseCallback = std::function<void(ConnectionId)>;

  Connection(EventLoop* loop, ConnectionId id, Socket socket,
             const sockaddr_in& peer, std::size_t write_high_watermark);
  ~Connection();

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;
  Connection(Connection&&) = delete;
  Connection& operator=(Connection&&) = delete;

  // 注册 fd 到所属 loop；必须在 loop 线程调用。
  Status Start();

  // 仅允许 loop 线程调用；缓冲超过高水位返回背压错误。
  Status Send(const std::string& data);

  // 仅允许 loop 线程调用；幂等，不直接 delete 自身。
  void Close();

  ConnectionId id() const { return id_; }
  bool closed() const { return closed_; }
  EventLoop* loop() const { return loop_; }
  const sockaddr_in& peer() const { return peer_; }

  void set_frame_callback(FrameCallback callback) {
    frame_callback_ = std::move(callback);
  }
  void set_close_callback(CloseCallback callback) {
    close_callback_ = std::move(callback);
  }

 private:
  void HandleEvents(std::uint32_t events);
  Status HandleRead();
  Status HandleWrite();
  Status DispatchFrames();
  void UpdateInterest();
  std::size_t PendingBytes() const;

  EventLoop* loop_;
  ConnectionId id_;
  Socket socket_;
  sockaddr_in peer_;
  protocol::FrameDecoder decoder_;

  std::string output_;
  std::size_t write_offset_;
  std::size_t write_high_watermark_;

  bool closed_;
  bool peer_closed_;
  std::uint32_t interest_;

  FrameCallback frame_callback_;
  CloseCallback close_callback_;
};

}  // namespace net
}  // namespace minikafka

#endif  // MINIKAFKA_NET_CONNECTION_H_
