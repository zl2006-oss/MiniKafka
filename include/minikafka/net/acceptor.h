#ifndef MINIKAFKA_NET_ACCEPTOR_H_
#define MINIKAFKA_NET_ACCEPTOR_H_

#include <netinet/in.h>
#include <sys/socket.h>

#include <cstdint>
#include <functional>

#include "minikafka/common/status.h"
#include "minikafka/net/event_loop.h"
#include "minikafka/net/socket.h"

namespace minikafka {
namespace net {

using common::Status;
using common::StatusCode;

// 监听 socket 的接受器；端口 0 由内核分配，Start 后可读 port()。
// Acceptor 必须比其 EventLoop 先析构；Start/Close 建议在 loop 线程或 Loop 停止后调用。
class Acceptor {
 public:
  using NewConnectionCallback =
      std::function<void(Socket&& socket, const sockaddr_in& peer)>;

  Acceptor(EventLoop* loop, const sockaddr_in& listen_address);
  ~Acceptor();

  Acceptor(const Acceptor&) = delete;
  Acceptor& operator=(const Acceptor&) = delete;

  Status Start(NewConnectionCallback callback, int backlog = SOMAXCONN);
  void Close();

  bool started() const { return started_; }
  std::uint16_t port() const { return port_; }

 private:
  Status HandleRead();

  EventLoop* loop_;
  sockaddr_in listen_address_;
  Socket socket_;
  NewConnectionCallback callback_;
  bool started_;
  std::uint16_t port_;
};

}  // namespace net
}  // namespace minikafka

#endif  // MINIKAFKA_NET_ACCEPTOR_H_
