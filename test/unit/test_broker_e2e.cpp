#include "test_common.h"
#include "test_storage_util.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "minikafka/broker/broker.h"
#include "minikafka/common/status.h"
#include "minikafka/protocol/frame.h"
#include "minikafka/protocol/messages.h"
#include "minikafka/storage/record.h"

namespace {

using minikafka::broker::Broker;
using minikafka::broker::BrokerConfig;
using minikafka::common::Status;
using minikafka::common::StatusCode;
using minikafka::protocol::Frame;
using minikafka::protocol::FrameDecoder;
using minikafka::protocol::MessageType;
using minikafka::storage::kFlagKeyPresent;

int SendFlags() {
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
  sockaddr_in address;
  std::memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = htons(port);
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
        ::send(fd, data.data() + sent, data.size() - sent, SendFlags());
    if (count > 0) {
      sent += static_cast<std::size_t>(count);
      continue;
    }
    return false;
  }
  return true;
}

bool ReadFrame(int fd, FrameDecoder* decoder, Frame* frame, int timeout_ms) {
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() +
      std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    Frame pending;
    bool has_frame = false;
    const Status next = decoder->Next(&pending, &has_frame);
    if (!next.ok()) {
      return false;
    }
    if (has_frame) {
      *frame = std::move(pending);
      return true;
    }
    pollfd descriptor;
    descriptor.fd = fd;
    descriptor.events = POLLIN;
    descriptor.revents = 0;
    const int ready = ::poll(&descriptor, 1, 100);
    if (ready <= 0) {
      continue;
    }
    char buffer[16u * 1024u];
    const ssize_t count = ::recv(fd, buffer, sizeof(buffer), 0);
    if (count <= 0) {
      return false;
    }
    if (!decoder->Feed(buffer, static_cast<std::size_t>(count)).ok()) {
      return false;
    }
  }
  return false;
}

struct Client {
  explicit Client(int descriptor) : fd(descriptor), next_request_id(1) {}

  template <typename Request, typename Response>
  bool Call(std::uint16_t msg_type, const Request& request,
            Response* response) {
    std::string payload;
    if (!Encode(request, &payload).ok()) {
      return false;
    }
    return CallPayload(msg_type, payload, response);
  }

  bool CallPayload(std::uint16_t msg_type, const std::string& payload,
                   minikafka::protocol::ListTopicsResponse* response) {
    Frame frame;
    if (!RoundTrip(msg_type, payload, &frame)) {
      return false;
    }
    if (frame.msg_type !=
        static_cast<std::uint16_t>(msg_type | 0x8000u)) {
      return false;
    }
    return minikafka::protocol::DecodeListTopicsResponse(frame.payload, response)
        .ok();
  }

  bool CallPayload(std::uint16_t msg_type, const std::string& payload,
                   minikafka::protocol::CreateTopicResponse* response) {
    Frame frame;
    if (!RoundTrip(msg_type, payload, &frame)) {
      return false;
    }
    return minikafka::protocol::DecodeCreateTopicResponse(frame.payload,
                                                          response)
        .ok();
  }

  bool CallPayload(std::uint16_t msg_type, const std::string& payload,
                   minikafka::protocol::ProduceResponse* response) {
    Frame frame;
    if (!RoundTrip(msg_type, payload, &frame)) {
      return false;
    }
    return minikafka::protocol::DecodeProduceResponse(frame.payload, response)
        .ok();
  }

  bool CallPayload(std::uint16_t msg_type, const std::string& payload,
                   minikafka::protocol::FetchResponse* response) {
    Frame frame;
    if (!RoundTrip(msg_type, payload, &frame)) {
      return false;
    }
    return minikafka::protocol::DecodeFetchResponse(frame.payload, response)
        .ok();
  }

  bool RoundTrip(std::uint16_t msg_type, const std::string& payload,
                 Frame* response) {
    Frame request;
    request.msg_type = msg_type;
    request.request_id = next_request_id++;
    request.payload = payload;
    std::string bytes;
    if (!minikafka::protocol::EncodeFrame(request, &bytes).ok()) {
      return false;
    }
    if (!SendAll(fd, bytes)) {
      return false;
    }
    return ReadFrame(fd, &decoder, response, 3000) &&
           response->request_id == request.request_id;
  }

  Status Encode(const minikafka::protocol::CreateTopicRequest& request,
                std::string* payload) {
    return minikafka::protocol::EncodeCreateTopicRequest(request, payload);
  }
  Status Encode(const minikafka::protocol::ProduceRequest& request,
                std::string* payload) {
    return minikafka::protocol::EncodeProduceRequest(request, payload);
  }
  Status Encode(const minikafka::protocol::FetchRequest& request,
                std::string* payload) {
    return minikafka::protocol::EncodeFetchRequest(request, payload);
  }
  Status Encode(const minikafka::protocol::ReplayRequest& request,
                std::string* payload) {
    return minikafka::protocol::EncodeReplayRequest(request, payload);
  }
  Status Encode(const minikafka::protocol::OffsetFetchRequest& request,
                std::string* payload) {
    return minikafka::protocol::EncodeOffsetFetchRequest(request, payload);
  }

  int fd;
  std::uint64_t next_request_id;
  FrameDecoder decoder;
};

bool WaitForPort(std::uint16_t port, int* out_fd) {
  const std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(3000);
  while (std::chrono::steady_clock::now() < deadline) {
    const int fd = ConnectLoopback(port);
    if (fd >= 0) {
      *out_fd = fd;
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  return false;
}

minikafka::protocol::ProduceEntry MakeEntry(const std::string& key,
                                            const std::string& value) {
  minikafka::protocol::ProduceEntry entry;
  entry.flags = kFlagKeyPresent;
  entry.key = key;
  entry.value = value;
  return entry;
}

std::uint64_t FileSize(const std::string& path) {
  struct stat st;
  if (::stat(path.c_str(), &st) != 0 || st.st_size < 0) {
    return 0u;
  }
  return static_cast<std::uint64_t>(st.st_size);
}

// 端到端：真实 socket 走完 create → produce → fetch → replay → list，
// 并核对消息确实落到了 data 目录里的分段文件（V1 的可视证据）。
void RunEndToEnd() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }

  BrokerConfig config;
  config.data_dir = dir.path();
  config.port = 0;
  config.io_threads = 1;
  config.storage_workers = 2;
  config.segment_bytes = 64u * 1024u;
  config.disk_threshold_bytes = 1u;

  Broker broker(config);
  const Status started = broker.Start();
  MK_EXPECT_TRUE(started.ok());
  if (!started.ok()) {
    return;
  }
  MK_EXPECT_TRUE(broker.port() != 0u);

  Status run_status;
  std::thread run_thread([&broker, &run_status] { run_status = broker.Run(); });

  int fd = -1;
  MK_EXPECT_TRUE(WaitForPort(broker.port(), &fd));
  if (fd < 0) {
    broker.Stop();
    run_thread.join();
    return;
  }
  Client client(fd);

  // 1) 建 Topic
  minikafka::protocol::CreateTopicRequest create;
  create.topic = "orders";
  create.partition_count = 3u;
  minikafka::protocol::CreateTopicResponse created;
  MK_EXPECT_TRUE(client.Call(static_cast<std::uint16_t>(MessageType::kCreateTopic),
                             create, &created));
  MK_EXPECT_EQ(created.error_code, StatusCode::kOk);
  MK_EXPECT_EQ(created.partition_count, 3u);
  MK_EXPECT_EQ(created.topic, std::string("orders"));

  // 2) 生产两条到 1 号分区
  minikafka::protocol::ProduceRequest produce;
  produce.topic = "orders";
  produce.partition = 1u;
  produce.entries.push_back(MakeEntry("k0", "v0"));
  produce.entries.push_back(MakeEntry("k1", "v1"));
  minikafka::protocol::ProduceResponse produced;
  MK_EXPECT_TRUE(client.Call(static_cast<std::uint16_t>(MessageType::kProduce),
                             produce, &produced));
  MK_EXPECT_EQ(produced.error_code, StatusCode::kOk);
  MK_EXPECT_EQ(produced.status, 0u);
  MK_EXPECT_EQ(produced.results.size(), 2u);
  if (produced.results.size() == 2u) {
    MK_EXPECT_EQ(produced.results[0].offset, 0u);
    MK_EXPECT_EQ(produced.results[1].offset, 1u);
  }

  // 3) 按 offset 拉取
  minikafka::protocol::FetchRequest fetch;
  fetch.topic = "orders";
  fetch.partition = 1u;
  fetch.offset = 0u;
  fetch.max_bytes = 0u;
  minikafka::protocol::FetchResponse fetched;
  MK_EXPECT_TRUE(client.Call(static_cast<std::uint16_t>(MessageType::kFetch),
                             fetch, &fetched));
  MK_EXPECT_EQ(fetched.error_code, StatusCode::kOk);
  MK_EXPECT_EQ(fetched.log_end_offset, 2u);
  MK_EXPECT_EQ(fetched.records.size(), 2u);
  if (fetched.records.size() == 2u) {
    MK_EXPECT_EQ(fetched.records[0].key, std::string("k0"));
    MK_EXPECT_EQ(fetched.records[0].value, std::string("v0"));
    MK_EXPECT_EQ(fetched.records[1].value, std::string("v1"));
  }

  // 4) 只读重放
  minikafka::protocol::ReplayRequest replay;
  replay.topic = "orders";
  replay.partition = 1u;
  replay.offset = 1u;
  replay.count = 1u;
  minikafka::protocol::FetchResponse replayed;
  MK_EXPECT_TRUE(client.Call(static_cast<std::uint16_t>(MessageType::kReplay),
                             replay, &replayed));
  MK_EXPECT_EQ(replayed.error_code, StatusCode::kOk);
  MK_EXPECT_EQ(replayed.records.size(), 1u);
  if (!replayed.records.empty()) {
    MK_EXPECT_EQ(replayed.records[0].offset, 1u);
  }

  // 5) 列出 Topic（空载荷请求）
  minikafka::protocol::ListTopicsResponse listed;
  MK_EXPECT_TRUE(client.CallPayload(
      static_cast<std::uint16_t>(MessageType::kListTopics), std::string(),
      &listed));
  MK_EXPECT_EQ(listed.error_code, StatusCode::kOk);
  MK_EXPECT_EQ(listed.entries.size(), 1u);
  if (!listed.entries.empty()) {
    MK_EXPECT_EQ(listed.entries[0].topic, std::string("orders"));
    MK_EXPECT_EQ(listed.entries[0].partition_count, 3u);
  }

  // 6) 错误路径：不存在的 Topic / 越界分区
  minikafka::protocol::FetchRequest missing_fetch = fetch;
  missing_fetch.topic = "nope";
  minikafka::protocol::FetchResponse missing;
  MK_EXPECT_TRUE(client.Call(static_cast<std::uint16_t>(MessageType::kFetch),
                             missing_fetch, &missing));
  MK_EXPECT_EQ(missing.error_code, StatusCode::kTopicNotFound);

  minikafka::protocol::ProduceRequest bad_partition = produce;
  bad_partition.partition = 9u;
  minikafka::protocol::ProduceResponse rejected;
  MK_EXPECT_TRUE(client.Call(static_cast<std::uint16_t>(MessageType::kProduce),
                             bad_partition, &rejected));
  MK_EXPECT_EQ(rejected.error_code, StatusCode::kInvalidPartition);
  MK_EXPECT_EQ(rejected.status, 2u);
  MK_EXPECT_TRUE(rejected.results.empty());

  // 7) M6 尚未实现：组请求显式回错误而不是假装成功
  minikafka::protocol::OffsetFetchRequest offset_fetch;
  offset_fetch.group = "g1";
  offset_fetch.topic = "orders";
  offset_fetch.consumer_index = 0u;
  offset_fetch.consumer_count = 1u;
  Frame group_frame;
  std::string group_payload;
  MK_EXPECT_TRUE(minikafka::protocol::EncodeOffsetFetchRequest(
                     offset_fetch, &group_payload)
                     .ok());
  MK_EXPECT_TRUE(client.RoundTrip(
      static_cast<std::uint16_t>(MessageType::kOffsetFetch), group_payload,
      &group_frame));
  minikafka::protocol::OffsetFetchResponse group_response;
  MK_EXPECT_TRUE(minikafka::protocol::DecodeOffsetFetchResponse(
                     group_frame.payload, &group_response)
                     .ok());
  MK_EXPECT_EQ(group_response.error_code, StatusCode::kInternalError);
  MK_EXPECT_TRUE(group_response.assigned_partitions.empty());

  // 8) 落盘证据：分区日志文件里确实有字节（页缓存 ack，不要求 fsync）。
  const std::string log_path =
      dir.path() + "/topics/orders/1/00000000000000000000.log";
  MK_EXPECT_TRUE(FileSize(log_path) > 0u);

  broker.Stop();
  if (run_thread.joinable()) {
    run_thread.join();
  }
  MK_EXPECT_TRUE(run_status.ok());

  // 9) 重启后数据仍在：offset 从 2 继续。
  Broker restarted_broker(config);
  const Status restarted = restarted_broker.Start();
  MK_EXPECT_TRUE(restarted.ok());
  if (!restarted.ok()) {
    return;
  }
  Status restarted_run;
  std::thread restarted_thread(
      [&restarted_broker, &restarted_run] { restarted_run = restarted_broker.Run(); });

  int restarted_fd = -1;
  MK_EXPECT_TRUE(WaitForPort(restarted_broker.port(), &restarted_fd));
  if (restarted_fd >= 0) {
    Client second(restarted_fd);
    minikafka::protocol::ProduceRequest again;
    again.topic = "orders";
    again.partition = 1u;
    again.entries.push_back(MakeEntry("k2", "v2"));
    minikafka::protocol::ProduceResponse produced_again;
    MK_EXPECT_TRUE(second.Call(
        static_cast<std::uint16_t>(MessageType::kProduce), again,
        &produced_again));
    MK_EXPECT_EQ(produced_again.status, 0u);
    if (!produced_again.results.empty()) {
      MK_EXPECT_EQ(produced_again.results[0].offset, 2u);
    }
  }
  restarted_broker.Stop();
  if (restarted_thread.joinable()) {
    restarted_thread.join();
  }
}

}  // namespace

void TestBrokerEndToEnd() { RunEndToEnd(); }
