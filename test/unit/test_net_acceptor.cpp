#include "test_common.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <thread>

#include "minikafka/net/acceptor.h"
#include "minikafka/net/event_loop.h"
#include "minikafka/net/socket.h"

namespace {

using minikafka::common::Status;
using minikafka::net::Acceptor;
using minikafka::net::EventLoop;
using minikafka::net::Socket;

sockaddr_in MakeLoopbackAddress(std::uint16_t port) {
  sockaddr_in address;
  std::memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(port);
  return address;
}

int ClientSendFlags() {
#ifdef MSG_NOSIGNAL
  return MSG_NOSIGNAL;
#else
  return 0;
#endif
}

}  // namespace

void TestAcceptor() {
  EventLoop loop;
  Acceptor acceptor(&loop, MakeLoopbackAddress(0));

  std::atomic<bool> accepted(false);
  std::atomic<bool> echoed(false);
  const Status start = acceptor.Start(
      [&accepted, &echoed](Socket&& socket, const sockaddr_in&) {
        accepted.store(true);
        pollfd descriptor;
        descriptor.fd = socket.Fd();
        descriptor.events = POLLIN;
        descriptor.revents = 0;
        const int ready = ::poll(&descriptor, 1, 2000);
        if (ready > 0) {
          char buffer[16];
          const ssize_t received = socket.Read(buffer, sizeof(buffer));
          if (received > 0) {
            ssize_t written = 0;
            while (written < received) {
              const ssize_t count = socket.Write(
                  buffer + written,
                  static_cast<std::size_t>(received - written));
              if (count <= 0) {
                break;
              }
              written += count;
            }
            echoed.store(written == received);
          }
        }
        socket.Close();
      });
  MK_EXPECT_TRUE(start.ok());
  if (!start.ok()) {
    return;
  }

  const std::uint16_t port = acceptor.port();
  MK_EXPECT_TRUE(port != 0);

  Status loop_status;
  std::atomic<bool> loop_running(false);
  std::thread loop_thread([&loop, &loop_status] { loop_status = loop.Loop(); });
  (void)loop.QueueInLoop([&loop_running] { loop_running.store(true); });
  for (int i = 0; i < 2000 && !loop_running.load(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  MK_EXPECT_TRUE(loop_running.load());

  bool client_ok = false;
  const int client_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  MK_EXPECT_TRUE(client_fd >= 0);
  if (client_fd >= 0) {
    const sockaddr_in server = MakeLoopbackAddress(port);
    if (::connect(client_fd, reinterpret_cast<const sockaddr*>(&server),
                  sizeof(server)) == 0) {
      const char message[] = "hello";
      const ssize_t sent =
          ::send(client_fd, message, 5, ClientSendFlags());
      pollfd descriptor;
      descriptor.fd = client_fd;
      descriptor.events = POLLIN;
      descriptor.revents = 0;
      const int ready = ::poll(&descriptor, 1, 2000);
      if (sent == 5 && ready > 0) {
        char buffer[16];
        ssize_t received = 0;
        while (received < 5) {
          const ssize_t count = ::recv(
              client_fd, buffer + received,
              static_cast<std::size_t>(5 - received), 0);
          if (count <= 0) {
            break;
          }
          received += count;
        }
        client_ok = received == 5 && std::memcmp(buffer, message, 5) == 0;
      }
    }
    ::close(client_fd);
  }

  MK_EXPECT_TRUE(client_ok);
  MK_EXPECT_TRUE(accepted.load());
  // 服务端在 write 完成后才置位 echoed；客户端 recv 可能先返回（内核已投递），
  // 因此这里短暂等待，避免并发跑 ctest 时出现顺序误判。
  for (int i = 0; i < 2000 && !echoed.load(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  MK_EXPECT_TRUE(echoed.load());

  (void)loop.QueueInLoop([&loop] { loop.Quit(); });
  loop_thread.join();
  MK_EXPECT_TRUE(loop_status.ok());
}
