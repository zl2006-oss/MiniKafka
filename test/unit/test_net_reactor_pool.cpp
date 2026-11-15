#include "test_common.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "minikafka/net/reactor_pool.h"
#include "minikafka/protocol/frame.h"

namespace {

using minikafka::common::Status;
using minikafka::net::ConnectionId;
using minikafka::net::ReactorPool;
using minikafka::protocol::Frame;
using minikafka::protocol::FrameDecoder;

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

int ConnectLoopback(std::uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return -1;
  }
  const sockaddr_in address = MakeLoopbackAddress(port);
  if (::connect(fd, reinterpret_cast<const sockaddr*>(&address),
                sizeof(address)) != 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

bool SendAll(int fd, const std::string& data) {
  std::size_t sent = 0;
  while (sent < data.size()) {
    const ssize_t count =
        ::send(fd, data.data() + sent, data.size() - sent, ClientSendFlags());
    if (count > 0) {
      sent += static_cast<std::size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) {
      continue;
    }
    return false;
  }
  return true;
}

std::string ReadExact(int fd, std::size_t expected, int timeout_ms) {
  std::string result;
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() +
      std::chrono::milliseconds(timeout_ms);

  while (result.size() < expected &&
         std::chrono::steady_clock::now() < deadline) {
    pollfd descriptor;
    descriptor.fd = fd;
    descriptor.events = POLLIN;
    descriptor.revents = 0;
    const int ready = ::poll(&descriptor, 1, 100);
    if (ready < 0 && errno == EINTR) {
      continue;
    }
    if (ready <= 0) {
      continue;
    }

    char buffer[64u * 1024u];
    const ssize_t count = ::recv(fd, buffer, sizeof(buffer), 0);
    if (count > 0) {
      result.append(buffer, static_cast<std::size_t>(count));
      continue;
    }
    if (count == 0) {
      break;
    }
    if (errno == EINTR) {
      continue;
    }
    break;
  }
  return result;
}

bool ReadFrames(int fd, FrameDecoder* decoder, std::size_t expected,
                std::vector<Frame>* frames, int timeout_ms) {
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() +
      std::chrono::milliseconds(timeout_ms);

  while (frames->size() < expected &&
         std::chrono::steady_clock::now() < deadline) {
    pollfd descriptor;
    descriptor.fd = fd;
    descriptor.events = POLLIN;
    descriptor.revents = 0;
    const int ready = ::poll(&descriptor, 1, 100);
    if (ready < 0 && errno == EINTR) {
      continue;
    }
    if (ready <= 0) {
      continue;
    }

    char buffer[64u * 1024u];
    const ssize_t count = ::recv(fd, buffer, sizeof(buffer), 0);
    if (count > 0) {
      const Status status =
          decoder->Feed(buffer, static_cast<std::size_t>(count));
      if (!status.ok()) {
        return false;
      }
      while (true) {
        Frame frame;
        bool has_frame = false;
        const Status next = decoder->Next(&frame, &has_frame);
        if (!next.ok()) {
          return false;
        }
        if (!has_frame) {
          break;
        }
        frames->push_back(frame);
      }
      continue;
    }
    if (count == 0) {
      break;
    }
    if (errno == EINTR) {
      continue;
    }
    break;
  }
  return frames->size() >= expected;
}

std::string EncodeTestFrame(std::uint16_t msg_type, std::uint64_t request_id,
                            const std::string& payload) {
  Frame frame;
  frame.msg_type = msg_type;
  frame.request_id = request_id;
  frame.payload = payload;
  std::string bytes;
  if (!minikafka::protocol::EncodeFrame(frame, &bytes).ok()) {
    return std::string();
  }
  return bytes;
}

template <typename Predicate>
bool WaitFor(Predicate predicate, int timeout_ms) {
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() +
      std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return predicate();
}

void TestLoopback() {
  ReactorPool pool(2u, 8u * 1024u * 1024u);
  std::atomic<bool> got_frame(false);
  std::atomic<bool> payload_ok(false);
  std::atomic<std::uint64_t> request_id(0);
  const std::string expected_payload = "hello";

  const Status start = pool.Start(
      MakeLoopbackAddress(0),
      [&pool, &got_frame, &payload_ok, &request_id,
       &expected_payload](ConnectionId id, const Frame& frame) {
        payload_ok.store(frame.payload == expected_payload);
        request_id.store(frame.request_id);
        Frame reply;
        reply.msg_type = frame.msg_type;
        reply.request_id = frame.request_id;
        reply.payload = frame.payload;
        std::string bytes;
        if (minikafka::protocol::EncodeFrame(reply, &bytes).ok()) {
          (void)pool.Send(id, bytes);
        }
        got_frame.store(true);
      });
  MK_EXPECT_TRUE(start.ok());
  if (!start.ok()) {
    return;
  }

  Status run_status;
  std::thread run_thread([&pool, &run_status] { run_status = pool.Run(); });
  MK_EXPECT_TRUE(WaitFor([&pool] { return pool.running(); }, 2000));

  const std::string request = EncodeTestFrame(0x0003u, 42u, expected_payload);
  bool client_ok = false;
  const int client = ConnectLoopback(pool.port());
  MK_EXPECT_TRUE(client >= 0);
  if (client >= 0) {
    if (SendAll(client, request)) {
      const std::string reply = ReadExact(client, request.size(), 2000);
      client_ok = reply == request;
    }
    ::close(client);
  }

  MK_EXPECT_TRUE(client_ok);
  MK_EXPECT_TRUE(WaitFor([&got_frame] { return got_frame.load(); }, 2000));
  MK_EXPECT_TRUE(payload_ok.load());
  MK_EXPECT_EQ(request_id.load(), static_cast<std::uint64_t>(42));

  pool.Stop();
  run_thread.join();
  MK_EXPECT_TRUE(run_status.ok());
}

void TestHalfPacket() {
  ReactorPool pool(1u, 8u * 1024u * 1024u);
  std::atomic<int> frame_count(0);
  std::atomic<bool> payload_ok(false);
  std::atomic<std::uint64_t> request_id(0);
  const std::string expected_payload = "half-packet";

  const Status start = pool.Start(
      MakeLoopbackAddress(0),
      [&frame_count, &payload_ok, &request_id,
       &expected_payload](ConnectionId, const Frame& frame) {
        payload_ok.store(frame.payload == expected_payload);
        request_id.store(frame.request_id);
        frame_count.fetch_add(1);
      });
  MK_EXPECT_TRUE(start.ok());
  if (!start.ok()) {
    return;
  }

  Status run_status;
  std::thread run_thread([&pool, &run_status] { run_status = pool.Run(); });
  MK_EXPECT_TRUE(WaitFor([&pool] { return pool.running(); }, 2000));

  const std::string bytes =
      EncodeTestFrame(0x0004u, 7u, expected_payload);
  const int client = ConnectLoopback(pool.port());
  MK_EXPECT_TRUE(client >= 0);
  if (client >= 0) {
    const std::string first = bytes.substr(0u, 10u);
    const std::string second = bytes.substr(10u);
    MK_EXPECT_TRUE(SendAll(client, first));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    MK_EXPECT_TRUE(SendAll(client, second));
    MK_EXPECT_TRUE(
        WaitFor([&frame_count] { return frame_count.load() == 1; }, 2000));
    ::close(client);
  }

  MK_EXPECT_EQ(frame_count.load(), 1);
  MK_EXPECT_TRUE(payload_ok.load());
  MK_EXPECT_EQ(request_id.load(), static_cast<std::uint64_t>(7));

  pool.Stop();
  run_thread.join();
  MK_EXPECT_TRUE(run_status.ok());
}

void TestLargeWrite() {
  ReactorPool pool(2u, 16u * 1024u * 1024u);
  std::atomic<bool> send_ok(false);
  std::atomic<bool> triggered(false);
  const std::string large_payload(6u * 1024u * 1024u, 'x');

  Frame large_frame;
  large_frame.msg_type = 0x8004u;
  large_frame.request_id = 99u;
  large_frame.payload = large_payload;
  std::string large_bytes;
  const Status encoded =
      minikafka::protocol::EncodeFrame(large_frame, &large_bytes);
  MK_EXPECT_TRUE(encoded.ok());
  if (!encoded.ok()) {
    return;
  }

  const Status start = pool.Start(
      MakeLoopbackAddress(0),
      [&pool, &send_ok, &triggered, &large_bytes](ConnectionId id,
                                                  const Frame& frame) {
        if (frame.payload != "trigger") {
          return;
        }
        send_ok.store(pool.Send(id, large_bytes).ok());
        triggered.store(true);
      });
  MK_EXPECT_TRUE(start.ok());
  if (!start.ok()) {
    return;
  }

  Status run_status;
  std::thread run_thread([&pool, &run_status] { run_status = pool.Run(); });
  MK_EXPECT_TRUE(WaitFor([&pool] { return pool.running(); }, 2000));

  const int client = ConnectLoopback(pool.port());
  MK_EXPECT_TRUE(client >= 0);
  if (client >= 0) {
    MK_EXPECT_TRUE(SendAll(client, EncodeTestFrame(0x0004u, 1u, "trigger")));
    MK_EXPECT_TRUE(WaitFor([&triggered] { return triggered.load(); }, 2000));
    MK_EXPECT_TRUE(send_ok.load());

    const std::string received = ReadExact(client, large_bytes.size(), 10000);
    MK_EXPECT_EQ(received.size(), large_bytes.size());
    MK_EXPECT_TRUE(received == large_bytes);
    ::close(client);
  }

  pool.Stop();
  run_thread.join();
  MK_EXPECT_TRUE(run_status.ok());
}

void TestPeerCloseRecycle() {
  ReactorPool pool(2u, 8u * 1024u * 1024u);
  std::atomic<bool> accepted(false);

  const Status start = pool.Start(
      MakeLoopbackAddress(0), ReactorPool::FrameCallback(),
      [&accepted](ConnectionId, const sockaddr_in&) { accepted.store(true); });
  MK_EXPECT_TRUE(start.ok());
  if (!start.ok()) {
    return;
  }

  Status run_status;
  std::thread run_thread([&pool, &run_status] { run_status = pool.Run(); });
  MK_EXPECT_TRUE(WaitFor([&pool] { return pool.running(); }, 2000));

  const int client = ConnectLoopback(pool.port());
  MK_EXPECT_TRUE(client >= 0);
  MK_EXPECT_TRUE(WaitFor([&accepted] { return accepted.load(); }, 2000));
  MK_EXPECT_TRUE(WaitFor(
      [&pool] { return pool.connection_count() == 1u; }, 2000));
  if (client >= 0) {
    ::close(client);
  }
  MK_EXPECT_TRUE(WaitFor(
      [&pool] { return pool.connection_count() == 0u; }, 2000));

  pool.Stop();
  run_thread.join();
  MK_EXPECT_TRUE(run_status.ok());
}

void TestCrossThreadWrites() {
  ReactorPool pool(2u, 8u * 1024u * 1024u);
  std::atomic<bool> accepted(false);
  std::atomic<ConnectionId> connection_id(0);

  const Status start = pool.Start(
      MakeLoopbackAddress(0), ReactorPool::FrameCallback(),
      [&accepted, &connection_id](ConnectionId id, const sockaddr_in&) {
        connection_id.store(id);
        accepted.store(true);
      });
  MK_EXPECT_TRUE(start.ok());
  if (!start.ok()) {
    return;
  }

  Status run_status;
  std::thread run_thread([&pool, &run_status] { run_status = pool.Run(); });
  MK_EXPECT_TRUE(WaitFor([&pool] { return pool.running(); }, 2000));

  const int client = ConnectLoopback(pool.port());
  MK_EXPECT_TRUE(client >= 0);
  MK_EXPECT_TRUE(WaitFor([&accepted] { return accepted.load(); }, 2000));

  const int kThreads = 4;
  const int kPerThread = 5;
  const std::size_t expected_frames =
      static_cast<std::size_t>(kThreads * kPerThread);
  std::atomic<int> sent_ok(0);
  std::vector<std::thread> writers;
  writers.reserve(static_cast<std::size_t>(kThreads));
  for (int thread_index = 0; thread_index < kThreads; ++thread_index) {
    writers.emplace_back([&pool, &connection_id, &sent_ok, thread_index,
                          kPerThread] {
      for (int message_index = 0; message_index < kPerThread; ++message_index) {
        const std::string payload =
            "t" + std::to_string(thread_index) + "-" +
            std::to_string(message_index);
        const std::string bytes = EncodeTestFrame(
            0x0007u,
            static_cast<std::uint64_t>(thread_index * 100 + message_index),
            payload);
        if (pool.Send(connection_id.load(), bytes).ok()) {
          sent_ok.fetch_add(1);
        }
      }
    });
  }
  for (std::thread& writer : writers) {
    writer.join();
  }
  MK_EXPECT_EQ(sent_ok.load(), static_cast<int>(expected_frames));

  FrameDecoder decoder;
  std::vector<Frame> frames;
  const bool read_ok =
      ReadFrames(client, &decoder, expected_frames, &frames, 5000);
  MK_EXPECT_TRUE(read_ok);
  MK_EXPECT_EQ(frames.size(), expected_frames);

  std::set<std::string> payloads;
  for (const Frame& frame : frames) {
    payloads.insert(frame.payload);
  }
  std::set<std::string> expected_payloads;
  for (int thread_index = 0; thread_index < kThreads; ++thread_index) {
    for (int message_index = 0; message_index < kPerThread; ++message_index) {
      expected_payloads.insert(
          "t" + std::to_string(thread_index) + "-" +
          std::to_string(message_index));
    }
  }
  MK_EXPECT_TRUE(payloads == expected_payloads);

  if (client >= 0) {
    ::close(client);
  }
  pool.Stop();
  run_thread.join();
  MK_EXPECT_TRUE(run_status.ok());
}

}  // namespace

void TestReactorPool() {
  TestLoopback();
  TestHalfPacket();
  TestLargeWrite();
  TestPeerCloseRecycle();
  TestCrossThreadWrites();
}
