#ifndef MINIKAFKA_NET_REACTOR_POOL_H_
#define MINIKAFKA_NET_REACTOR_POOL_H_

#include <signal.h>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "minikafka/common/status.h"
#include "minikafka/net/acceptor.h"
#include "minikafka/net/connection.h"
#include "minikafka/net/event_loop.h"
#include "minikafka/net/socket.h"

namespace minikafka {
namespace net {

using common::Status;
using common::StatusCode;

// Main Reactor 只 accept；N 个 IO Reactor 各带一个 EventLoop 与连接表。
// 新连接 round-robin 分配，连接不跨 loop 迁移。
// 跨线程写只传 ConnectionId，经 ReactorPool::Send 排到连接所属 loop。
// 析构前必须保证 Run() 已返回（调用方 join 其线程）。
class ReactorPool {
 public:
  using FrameCallback = Connection::FrameCallback;
  using ConnectionCallback =
      std::function<void(ConnectionId, const sockaddr_in&)>;

  explicit ReactorPool(
      std::size_t io_threads = 0,
      std::size_t write_high_watermark =
          kDefaultConnectionWriteHighWatermark);
  ~ReactorPool();

  ReactorPool(const ReactorPool&) = delete;
  ReactorPool& operator=(const ReactorPool&) = delete;

  // 创建 Acceptor 并启动 IO Reactor 线程；Run 负责运行 Main Reactor。
  Status Start(const sockaddr_in& listen_address,
               FrameCallback frame_callback,
               ConnectionCallback connection_callback =
                   ConnectionCallback());

  // 在调用线程运行 Main Reactor，直到 Stop 或 SIGINT/SIGTERM。
  Status Run();
  // 幂等；停止 accept、逐连接关闭、join IO loop 线程。
  void Stop();

  std::uint16_t port() const;
  bool running() const { return main_running_.load(); }
  // 跨线程写入：内部 QueueInLoop 到连接所属 loop，返回其 Send 结果（含背压错误）。
  Status Send(ConnectionId connection_id, std::string data);
  std::size_t connection_count() const;

  static std::size_t DefaultIoThreads();

 private:
  class Reactor;

  void DispatchConnection(Socket&& socket, const sockaddr_in& peer);
  void DoStop();
  void HandleSignal();
  void WaitForStopped();

  std::size_t io_threads_;
  std::size_t write_high_watermark_;

  EventLoop main_loop_;
  std::unique_ptr<Acceptor> acceptor_;
  std::vector<std::unique_ptr<Reactor>> reactors_;

  FrameCallback frame_callback_;
  ConnectionCallback connection_callback_;

  std::atomic<bool> started_;
  std::atomic<bool> stopping_;
  std::atomic<bool> stopped_;
  std::atomic<bool> main_running_;

  std::mutex stop_mutex_;
  std::condition_variable stop_cv_;

  std::size_t next_reactor_;
  std::uint64_t next_sequence_;
  std::uint16_t port_;

  UniqueFd signal_fd_;
  sigset_t signal_set_;
  sigset_t old_signal_mask_;
  bool signal_mask_saved_;
};

}  // namespace net
}  // namespace minikafka

#endif  // MINIKAFKA_NET_REACTOR_POOL_H_
