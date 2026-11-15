#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "minikafka/client/cli.h"
#include "minikafka/client/connection.h"
#include "minikafka/common/args.h"
#include "minikafka/common/defaults.h"
#include "minikafka/common/event_sink.h"
#include "minikafka/protocol/frame.h"
#include "minikafka/protocol/messages.h"
#include "minikafka/storage/record.h"

namespace minikafka {
namespace client {

using common::Args;
using common::EventSink;
using common::Status;
using common::StatusCode;

namespace {

// 05 §3.6：Fetch 在 offset > log_end 时返回 offset_out_of_range，且响应仍带
// log_end_offset —— 用它做一次只读探测即可拿到分区末尾（支撑 latest / 越界修复）。
constexpr std::uint64_t kProbeOffset = ~static_cast<std::uint64_t>(0);

// 跟随模式（无新消息）的轮询间隔：只影响延迟，不影响正确性。
constexpr int kFollowSleepMs = 100;

// R8：提交失败退避重试，绝不主动退出。
constexpr int kCommitRetries = 3;

// Q-P2=A：客户端槽位数上限与 group::kMaxConsumerCount 对齐（M6）。
constexpr std::uint64_t kMaxConsumerCount = 16u;

volatile std::sig_atomic_t g_stop_requested = 0;

void OnSignal(int /*signo*/) { g_stop_requested = 1; }

// 单个已分配分区的消费游标：next_offset 是"下一条要拉取的 offset"，
// 同时也是提交时使用的"已处理位点"（R6/04 S-13）。
struct Cursor {
  std::uint32_t partition = 0;
  std::uint64_t next_offset = 0;
  std::uint64_t committed = 0;
  bool has_committed = false;
  bool pending_commit = false;
};

bool WantsHelp(const std::vector<std::string>& tokens) {
  for (std::size_t i = 0; i < tokens.size(); ++i) {
    if (tokens[i] == "--help" || tokens[i] == "-h") {
      return true;
    }
  }
  return false;
}

Status ConnectDefault(BrokerConnection* connection) {
  Address address;
  const Status parsed = ParseAddress(common::kDefaultBrokerAddress, &address);
  if (!parsed.ok()) {
    return parsed;
  }
  return connection->Connect(address.host, address.port);
}

// 分区末尾探测：越界 Fetch 的响应里 log_end_offset 依然有效。
Status ProbeLogEnd(BrokerConnection* connection, const std::string& topic,
                   std::uint32_t partition, std::uint64_t* log_end) {
  protocol::FetchRequest request;
  request.topic = topic;
  request.partition = partition;
  request.offset = kProbeOffset;
  request.max_bytes = 0;

  protocol::FetchResponse response;
  const Status status = connection->Invoke(
      static_cast<std::uint16_t>(protocol::MessageType::kFetch), request,
      &response, protocol::EncodeFetchRequest, protocol::DecodeFetchResponse);
  if (!status.ok()) {
    return status;
  }
  if (response.error_code != StatusCode::kOk &&
      response.error_code != StatusCode::kOffsetOutOfRange) {
    return Status(response.error_code, response.message);
  }
  *log_end = response.log_end_offset;
  return Status();
}

Status FetchOnce(BrokerConnection* connection, const std::string& topic,
                 std::uint32_t partition, std::uint64_t offset,
                 protocol::FetchResponse* response) {
  protocol::FetchRequest request;
  request.topic = topic;
  request.partition = partition;
  request.offset = offset;
  request.max_bytes = 0;  // 由 Broker 取默认上限（1 MiB / 500 条）
  return connection->Invoke(
      static_cast<std::uint16_t>(protocol::MessageType::kFetch), request,
      response, protocol::EncodeFetchRequest, protocol::DecodeFetchResponse);
}

// 提交"已处理位点"；只在整体成功时清除 pending 标记（S-13 全量校验）。
Status CommitCursors(BrokerConnection* connection, const std::string& group,
                     const std::string& topic, std::vector<Cursor>* cursors) {
  protocol::OffsetCommitRequest request;
  request.group = group;
  request.topic = topic;
  std::vector<std::uint32_t> sent;
  for (std::size_t i = 0; i < cursors->size(); ++i) {
    if (!(*cursors)[i].pending_commit) {
      continue;
    }
    protocol::OffsetCommitEntry entry;
    entry.partition = (*cursors)[i].partition;
    entry.committed_offset = (*cursors)[i].next_offset;
    request.entries.push_back(entry);
    sent.push_back((*cursors)[i].partition);
  }
  if (request.entries.empty()) {
    return Status();
  }

  protocol::OffsetCommitResponse response;
  const Status status = connection->Invoke(
      static_cast<std::uint16_t>(protocol::MessageType::kOffsetCommit), request,
      &response, protocol::EncodeOffsetCommitRequest,
      protocol::DecodeOffsetCommitResponse);
  if (!status.ok()) {
    return status;
  }
  if (response.error_code != StatusCode::kOk) {
    return Status(response.error_code, response.message);
  }
  for (std::size_t i = 0; i < cursors->size(); ++i) {
    for (std::size_t j = 0; j < sent.size(); ++j) {
      if ((*cursors)[i].partition == sent[j]) {
        (*cursors)[i].pending_commit = false;
        (*cursors)[i].committed = (*cursors)[i].next_offset;
        (*cursors)[i].has_committed = true;
        break;
      }
    }
  }
  return Status();
}

// R8：提交失败退避重试并告警，返回是否最终成功；失败不影响继续消费。
bool CommitWithRetry(BrokerConnection* connection, const std::string& group,
                     const std::string& topic, std::vector<Cursor>* cursors,
                     EventSink* sink, int backoff_ms) {
  for (int attempt = 0; attempt <= kCommitRetries; ++attempt) {
    const Status status = CommitCursors(connection, group, topic, cursors);
    if (status.ok()) {
      return true;
    }
    std::string event = "{\"event\":\"commit_failed\",\"attempt\":";
    event += Number(static_cast<std::uint64_t>(attempt));
    event += ",\"error\":";
    event += Quote(common::StatusCodeToString(status.code()));
    event += ",\"message\":";
    event += Quote(status.message());
    event += "}";
    sink->WriteEvent(event);
    if (attempt == kCommitRetries) {
      break;
    }
    std::this_thread::sleep_for(
        std::chrono::milliseconds(backoff_ms * (attempt + 1)));
    if (!connection->connected()) {
      (void)connection->Reconnect();
    }
  }
  return false;
}

}  // namespace

int RunConsume(const std::vector<std::string>& tokens) {
  if (WantsHelp(tokens)) {
    PrintConsumeUsage();
    return 0;
  }
  EventSink sink;
  Args args;
  args.AddString("topic", "", true);
  args.AddString("group", "", true);
  args.AddString("consumer-index", "", true);
  args.AddString("consumer-count", "", true);
  args.AddString("from", common::kDefaultFrom);
  args.AddString("auto-commit-interval", "5s");
  args.AddBool("exit-at-end", false);
  args.AddInt("retry-backoff-ms", common::kDefaultRetryBackoffMs);
  const Status parsed = args.Parse(tokens);
  if (!parsed.ok()) {
    PrintConsumeUsage();
    sink.WriteError(parsed);
    return 2;
  }

  const std::string topic = args.GetString("topic");
  const std::string group = args.GetString("group");

  // Q-P2=A：槽位由部署者显式保证唯一，Broker 只做 0 ≤ i < n 校验。
  std::uint64_t consumer_index = 0;
  std::uint64_t consumer_count = 0;
  if (!ParseU64(args.GetString("consumer-index"), &consumer_index)) {
    sink.WriteError(Status(StatusCode::kUsage,
                           "--consumer-index must be a non-negative integer"));
    return 2;
  }
  if (!ParseU64(args.GetString("consumer-count"), &consumer_count) ||
      consumer_count == 0u || consumer_count > kMaxConsumerCount) {
    sink.WriteError(Status(StatusCode::kUsage,
                           "--consumer-count must be in 1..16"));
    return 2;
  }
  if (consumer_index >= consumer_count) {
    sink.WriteError(Status(StatusCode::kInvalidConsumerSlot,
                           "consumer index must be less than consumer count"));
    return 3;
  }

  FromSpec from;
  Status status = ParseFrom(args.GetString("from"), &from);
  if (!status.ok()) {
    sink.WriteError(status);
    return ExitCodeFor(status);
  }

  int commit_interval_s = 0;
  status = ParseIntervalSeconds(args.GetString("auto-commit-interval"),
                                &commit_interval_s);
  if (!status.ok()) {
    sink.WriteError(status);
    return ExitCodeFor(status);
  }
  const std::int64_t commit_interval_ms =
      static_cast<std::int64_t>(commit_interval_s) * 1000;
  const int backoff_ms = args.GetInt("retry-backoff-ms");
  if (backoff_ms < 0) {
    sink.WriteError(
        Status(StatusCode::kUsage, "--retry-backoff-ms must be >= 0"));
    return 2;
  }
  const bool exit_at_end = args.GetBool("exit-at-end");

  BrokerConnection connection;
  status = ConnectDefault(&connection);
  if (!status.ok()) {
    sink.WriteError(Status(StatusCode::kBrokerUnavailable,
                           "cannot connect to broker: " + status.message()));
    return 3;
  }

  // R13：加入组并请求分配（纯函数在 Broker 侧；组状态不落客户端）。
  protocol::OffsetFetchRequest assign_request;
  assign_request.group = group;
  assign_request.topic = topic;
  assign_request.consumer_index = static_cast<std::uint32_t>(consumer_index);
  assign_request.consumer_count = static_cast<std::uint32_t>(consumer_count);

  protocol::OffsetFetchResponse assign;
  status = connection.Invoke(
      static_cast<std::uint16_t>(protocol::MessageType::kOffsetFetch),
      assign_request, &assign, protocol::EncodeOffsetFetchRequest,
      protocol::DecodeOffsetFetchResponse);
  if (!status.ok()) {
    sink.WriteError(status);
    return 3;
  }
  if (assign.error_code != StatusCode::kOk) {
    sink.WriteError(Status(assign.error_code, assign.message));
    return 3;
  }
  if (assign.assigned_partitions.empty()) {
    // 总分区数 < consumer_count 时存在空槽位；按 Q-P2=A 空槽位不报错。
    sink.WriteEvent(
        "{\"event\":\"empty_assignment\",\"group\":" + Quote(group) +
        ",\"topic\":" + Quote(topic) +
        ",\"consumer_index\":" + Number(consumer_index) +
        ",\"consumer_count\":" + Number(consumer_count) + "}");
  }

  // 起始位点裁决（R11）：已提交位点优先，其次 --from。
  std::vector<Cursor> cursors;
  cursors.reserve(assign.assigned_partitions.size());
  for (std::size_t i = 0; i < assign.assigned_partitions.size(); ++i) {
    Cursor cursor;
    cursor.partition = assign.assigned_partitions[i];
    for (std::size_t j = 0; j < assign.entries.size(); ++j) {
      if (assign.entries[j].partition == cursor.partition &&
          assign.entries[j].has_committed) {
        cursor.has_committed = true;
        cursor.committed = assign.entries[j].committed_offset;
        break;
      }
    }

    if (cursor.has_committed) {
      cursor.next_offset = cursor.committed;
    } else if (from.mode == FromMode::kEarliest) {
      cursor.next_offset = 0;
    } else {
      // latest / 指定位点都需要知道分区末尾才能裁决。
      std::uint64_t log_end = 0;
      status = ProbeLogEnd(&connection, topic, cursor.partition, &log_end);
      if (!status.ok()) {
        sink.WriteError(status);
        return 3;
      }
      if (from.mode == FromMode::kLatest) {
        cursor.next_offset = log_end;
      } else if (from.offset > log_end) {
        // R11 允许指定位点，但越界位点无法读取：收敛到末尾并告警。
        cursor.next_offset = log_end;
        sink.WriteEvent("{\"event\":\"offset_reset\",\"topic\":" +
                        Quote(topic) + ",\"partition\":" +
                        Number(cursor.partition) + ",\"requested\":" +
                        Number(from.offset) + ",\"reset_to\":" +
                        Number(log_end) + ",\"reason\":" +
                        Quote("offset_out_of_range") + "}");
      } else {
        cursor.next_offset = from.offset;
      }
    }

    sink.WriteEvent("{\"event\":\"assigned\",\"group\":" + Quote(group) +
                    ",\"topic\":" + Quote(topic) + ",\"partition\":" +
                    Number(cursor.partition) + ",\"start_offset\":" +
                    Number(cursor.next_offset) + ",\"resumed\":" +
                    (cursor.has_committed ? "true" : "false") + "}");
    cursors.push_back(cursor);
  }

  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  std::int64_t last_commit = NowMs();
  int exit_code = 0;

  while (g_stop_requested == 0) {
    bool any_records = false;
    for (std::size_t i = 0; i < cursors.size(); ++i) {
      Cursor& cursor = cursors[i];
      protocol::FetchResponse response;
      status = FetchOnce(&connection, topic, cursor.partition,
                         cursor.next_offset, &response);
      if (!status.ok()) {
        // 连接/协议失败：告警 + 退避 + 重连，进程不退出（R8）。
        sink.WriteEvent("{\"event\":\"fetch_failed\",\"partition\":" +
                        Number(cursor.partition) + ",\"offset\":" +
                        Number(cursor.next_offset) + ",\"error\":" +
                        Quote(common::StatusCodeToString(status.code())) +
                        "}");
        std::this_thread::sleep_for(
            std::chrono::milliseconds(backoff_ms > 0 ? backoff_ms : 100));
        if (!connection.connected()) {
          (void)connection.Reconnect();
        }
        continue;
      }
      if (response.error_code == StatusCode::kOffsetOutOfRange) {
        // 位点越界（日志被回退/位点超前）：收敛到末尾并告警，不静默丢读。
        sink.WriteEvent("{\"event\":\"offset_reset\",\"topic\":" +
                        Quote(topic) + ",\"partition\":" +
                        Number(cursor.partition) + ",\"requested\":" +
                        Number(cursor.next_offset) + ",\"reset_to\":" +
                        Number(response.log_end_offset) + ",\"reason\":" +
                        Quote("offset_out_of_range") + "}");
        if (response.log_end_offset != cursor.next_offset) {
          cursor.pending_commit = true;
        }
        cursor.next_offset = response.log_end_offset;
        continue;
      }
      if (response.error_code != StatusCode::kOk) {
        sink.WriteEvent("{\"event\":\"fetch_failed\",\"partition\":" +
                        Number(cursor.partition) + ",\"offset\":" +
                        Number(cursor.next_offset) + ",\"error\":" +
                        Quote(common::StatusCodeToString(response.error_code)) +
                        ",\"message\":" + Quote(response.message) + "}");
        continue;
      }

      for (std::size_t r = 0; r < response.records.size(); ++r) {
        const protocol::FetchRecord& record = response.records[r];
        sink.WriteResult(
            RecordLine(topic, cursor.partition, record.offset,
                       (record.flags & storage::kFlagKeyPresent) != 0u,
                       record.key, record.value));
        if (!std::cout.good()) {
          // 输出通道已坏：不再推进位点（R6），以避免"提交了却没交付"。
          sink.Flush();
          sink.WriteError(Status(StatusCode::kInternalError,
                                 "stdout write failed, stopping consumer"));
          return 3;
        }
        // R6：只有成功写出后才推进"已处理位点"。
        cursor.next_offset = record.offset + 1u;
        cursor.pending_commit = true;
        any_records = true;
      }
    }

    // 自动提交只缩小重复窗口，不影响正确性（R6）。
    if (NowMs() - last_commit >= commit_interval_ms) {
      sink.Flush();
      (void)CommitWithRetry(&connection, group, topic, &cursors, &sink,
                            backoff_ms);
      last_commit = NowMs();
    }

    if (!any_records) {
      if (exit_at_end) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(kFollowSleepMs));
    }
  }

  // 03 §3.4：正常退出补一次提交。
  sink.Flush();
  if (!CommitWithRetry(&connection, group, topic, &cursors, &sink, backoff_ms)) {
    exit_code = 3;
  }
  sink.WriteEvent("{\"event\":\"consumer_stopped\",\"group\":" + Quote(group) +
                  ",\"topic\":" + Quote(topic) + "}");
  sink.Flush();
  return exit_code;
}

}  // namespace client
}  // namespace minikafka
