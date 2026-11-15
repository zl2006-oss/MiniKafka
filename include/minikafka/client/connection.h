#ifndef MINIKAFKA_CLIENT_CONNECTION_H_
#define MINIKAFKA_CLIENT_CONNECTION_H_

#include <cstdint>
#include <string>

#include "minikafka/common/status.h"
#include "minikafka/net/socket.h"
#include "minikafka/protocol/frame.h"
#include "minikafka/protocol/messages.h"

namespace minikafka {
namespace client {

// M3 客户端侧的同步连接：一次一条在途请求，request_id 单调递增并回显校验。
// 断线后由调用方决定是否 Reconnect（R8：消费者持续重连，生产者按 R3 重试）。
class BrokerConnection {
 public:
  explicit BrokerConnection(int timeout_ms = 5000);

  common::Status Connect(const std::string& host, std::uint16_t port);
  // 用上次的地址重连；未连接过返回错误。
  common::Status Reconnect();
  void Close();

  bool connected() const { return socket_.Valid() && connected_; }
  const std::string& host() const { return host_; }
  std::uint16_t port() const { return port_; }

  // 发送一条请求并等待同 request_id 的响应；超时或断线返回错误。
  common::Status Call(std::uint16_t msg_type, const std::string& payload,
                      protocol::Frame* response);

  template <typename Request, typename Response, typename EncodeFn,
            typename DecodeFn>
  common::Status Invoke(std::uint16_t msg_type, const Request& request,
                        Response* response, EncodeFn encode, DecodeFn decode) {
    std::string payload;
    common::Status status = encode(request, &payload);
    if (!status.ok()) {
      return status;
    }
    protocol::Frame frame;
    status = Call(msg_type, payload, &frame);
    if (!status.ok()) {
      return status;
    }
    return decode(frame.payload, response);
  }

 private:
  common::Status WriteAll(const std::string& bytes, int64_t deadline_ms);
  common::Status ReadFrame(protocol::Frame* frame, int64_t deadline_ms);
  common::Status ConnectInternal(int timeout_ms);

  int timeout_ms_;
  std::string host_;
  std::uint16_t port_;
  net::Socket socket_;
  protocol::FrameDecoder decoder_;
  std::uint64_t next_request_id_;
  bool connected_;
};

// 毫秒级单调时钟（内部使用）。
std::int64_t NowMs();

}  // namespace client
}  // namespace minikafka

#endif  // MINIKAFKA_CLIENT_CONNECTION_H_
