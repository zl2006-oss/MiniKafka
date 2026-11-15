#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "minikafka/client/cli.h"
#include "minikafka/client/connection.h"
#include "minikafka/client/input.h"
#include "minikafka/common/args.h"
#include "minikafka/common/defaults.h"
#include "minikafka/common/event_sink.h"
#include "minikafka/common/hash.h"
#include "minikafka/protocol/messages.h"

namespace minikafka {
namespace client {

using common::Args;
using common::EventSink;
using common::Status;
using common::StatusCode;

namespace {

struct EntryResult {
  std::uint32_t partition = 0;
  bool ok = false;
  std::uint64_t offset = 0;
  StatusCode code = StatusCode::kOk;
};

bool WantsHelp(const std::vector<std::string>& tokens) {
  for (std::size_t i = 0; i < tokens.size(); ++i) {
    if (tokens[i] == "--help" || tokens[i] == "-h") {
      return true;
    }
  }
  return false;
}

// 03 §3.4：显式 --partition 之外，produce/consume/replay 固定连默认 broker。
Status ConnectDefault(BrokerConnection* connection) {
  Address address;
  const Status parsed = ParseAddress(common::kDefaultBrokerAddress, &address);
  if (!parsed.ok()) {
    return parsed;
  }
  return connection->Connect(address.host, address.port);
}

// 可重试错误（R3/R24）：投递类；校验类不重试。
bool IsRetryable(StatusCode code) {
  switch (code) {
    case StatusCode::kDiskFull:
    case StatusCode::kBrokerUnavailable:
    case StatusCode::kStorageIoError:
    case StatusCode::kStorageUnavailable:
      return true;
    default:
      return false;
  }
}

std::vector<std::string> SplitLines(const std::string& text) {
  std::vector<std::string> lines;
  std::string current;
  for (std::size_t i = 0; i < text.size(); ++i) {
    const char ch = text[i];
    if (ch == '\n') {
      if (!current.empty()) {
        lines.push_back(current);
      }
      current.clear();
      continue;
    }
    if (ch != '\r') {
      current.push_back(ch);
    }
  }
  if (!current.empty()) {
    lines.push_back(current);
  }
  return lines;
}

// 输入优先级：--message > --file > stdin（R19/03 §3.4）。
Status ReadInputLines(const Args& args, std::vector<std::string>* lines) {
  lines->clear();
  if (args.Has("message")) {
    lines->push_back(args.GetString("message"));
    return Status();
  }
  if (args.Has("file")) {
    std::ifstream file(args.GetString("file").c_str());
    if (!file) {
      return Status(StatusCode::kUsage,
                    "cannot open file: " + args.GetString("file"));
    }
    std::string text;
    std::string line;
    while (std::getline(file, line)) {
      if (!line.empty() && line.back() == '\r') {
        line.pop_back();
      }
      if (!line.empty()) {
        text += line;
        text.push_back('\n');
      }
    }
    *lines = SplitLines(text);
    return Status();
  }

  std::string line;
  while (std::getline(std::cin, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    if (!line.empty()) {
      lines->push_back(line);
    }
  }
  if (lines->empty()) {
    return Status(StatusCode::kUsage,
                  "no input: use --message, --file or pipe JSON Lines on "
                  "stdin");
  }
  return Status();
}

// 发送一个分区块（≤1024 条），返回每条结果；失败条目按 R24 重试。
void ProduceChunk(BrokerConnection* connection, const std::string& topic,
                  std::uint32_t partition,
                  const std::vector<const RawMessage*>& chunk,
                  const std::size_t* global_indices, int retries,
                  int backoff_ms, int timeout_ms, EventSink* sink,
                  std::vector<EntryResult>* results, bool* exhausted) {
  std::vector<std::size_t> pending;
  for (std::size_t i = 0; i < chunk.size(); ++i) {
    pending.push_back(i);
  }

  int attempt = 0;
  const std::int64_t started = NowMs();

  while (!pending.empty()) {
    protocol::ProduceRequest request;
    request.topic = topic;
    request.partition = partition;
    request.entries.reserve(pending.size());
    for (std::size_t i = 0; i < pending.size(); ++i) {
      const RawMessage& message = *chunk[pending[i]];
      protocol::ProduceEntry entry;
      entry.flags = message.flags;
      entry.key = message.key;
      entry.value = message.value;
      request.entries.push_back(entry);
    }

    std::string payload;
    Status status = protocol::EncodeProduceRequest(request, &payload);
    protocol::ProduceResponse response;
    if (status.ok()) {
      protocol::Frame frame;
      status = connection->Call(
          static_cast<std::uint16_t>(protocol::MessageType::kProduce), payload,
          &frame);
      if (status.ok()) {
        status = protocol::DecodeProduceResponse(frame.payload, &response);
      }
    }

    if (!status.ok()) {
      // 连接/协议层失败：整块按可重试处理。
      if (attempt >= retries || NowMs() - started > timeout_ms) {
        for (std::size_t i = 0; i < pending.size(); ++i) {
          EntryResult result;
          result.partition = partition;
          result.ok = false;
          result.code = status.code();
          (*results)[global_indices[pending[i]]] = result;
        }
        *exhausted = true;
        return;
      }
      ++attempt;
      if (sink != nullptr) {
        sink->WriteEvent("{\"event\":\"retry\",\"index\":" +
                         Number(global_indices[pending.front()]) +
                         ",\"partition\":" + Number(partition) +
                         ",\"attempt\":" + Number(static_cast<std::uint64_t>(attempt)) +
                         ",\"error\":" + Quote("broker_unavailable") + "}");
      }
      std::this_thread::sleep_for(
          std::chrono::milliseconds(backoff_ms * attempt));
      if (!connection->connected()) {
        (void)connection->Reconnect();
      }
      continue;
    }

    if (response.error_code != StatusCode::kOk) {
      // 请求级错误（topic_not_found / invalid_partition）：不重试。
      for (std::size_t i = 0; i < pending.size(); ++i) {
        EntryResult result;
        result.partition = partition;
        result.ok = false;
        result.code = response.error_code;
        (*results)[global_indices[pending[i]]] = result;
      }
      return;
    }

    if (response.results.size() != pending.size()) {
      // 结果条数与请求不符属于协议异常：整块按失败处理，不做部分成功假设。
      for (std::size_t i = 0; i < pending.size(); ++i) {
        EntryResult result;
        result.partition = partition;
        result.ok = false;
        result.code = StatusCode::kInternalError;
        (*results)[global_indices[pending[i]]] = result;
      }
      return;
    }

    // 逐条结果：首个失败之后的条目是 not_attempted，需要随失败条目一起重试。
    std::size_t first_failed = pending.size();
    for (std::size_t i = 0; i < response.results.size() && i < pending.size();
         ++i) {
      const std::size_t global = global_indices[pending[i]];
      const protocol::ProduceResult& item = response.results[i];
      EntryResult result;
      result.partition = partition;
      if (item.status == 0u && item.error_code == StatusCode::kOk) {
        result.ok = true;
        result.offset = item.offset;
        (*results)[global] = result;
        continue;
      }
      if (first_failed == pending.size()) {
        first_failed = i;
      }
      result.ok = false;
      result.code = item.error_code;
      (*results)[global] = result;
    }

    if (first_failed == pending.size()) {
      return;  // 全部追加成功
    }

    const StatusCode failure_code = (*results)[global_indices[pending[first_failed]]].code;
    if (!IsRetryable(failure_code)) {
      // 校验类失败：本条与后续 not_attempted 都结束尝试（AD-14）。
      for (std::size_t i = first_failed + 1u; i < pending.size(); ++i) {
        EntryResult result;
        result.partition = partition;
        result.ok = false;
        result.code = StatusCode::kPartitionBlocked;
        (*results)[global_indices[pending[i]]] = result;
      }
      return;
    }
    if (attempt >= retries || NowMs() - started > timeout_ms) {
      for (std::size_t i = first_failed + 1u; i < pending.size(); ++i) {
        EntryResult result;
        result.partition = partition;
        result.ok = false;
        result.code = StatusCode::kPartitionBlocked;
        (*results)[global_indices[pending[i]]] = result;
      }
      *exhausted = true;
      return;
    }

    ++attempt;
    if (sink != nullptr) {
      sink->WriteEvent("{\"event\":\"retry\",\"index\":" +
                       Number(global_indices[pending[first_failed]]) +
                       ",\"partition\":" + Number(partition) +
                       ",\"attempt\":" +
                       Number(static_cast<std::uint64_t>(attempt)) +
                       ",\"error\":" +
                       Quote(common::StatusCodeToString(failure_code)) + "}");
    }
    std::this_thread::sleep_for(
        std::chrono::milliseconds(backoff_ms * attempt));

    std::vector<std::size_t> next;
    for (std::size_t i = first_failed; i < pending.size(); ++i) {
      next.push_back(pending[i]);
    }
    pending.swap(next);
  }
}

}  // namespace

int RunProduce(const std::vector<std::string>& tokens) {
  if (WantsHelp(tokens)) {
    PrintProduceUsage();
    return 0;
  }
  EventSink sink;
  Args args;
  args.AddString("topic", "", true);
  args.AddString("partition", "");
  args.AddInt("retries", common::kDefaultRetries);
  args.AddInt("retry-backoff-ms", common::kDefaultRetryBackoffMs);
  args.AddInt("retry-timeout-ms", common::kDefaultRetryTimeoutMs);
  args.AddString("message", "");
  args.AddString("file", "");
  const Status parsed = args.Parse(tokens);
  if (!parsed.ok()) {
    PrintProduceUsage();
    sink.WriteError(parsed);
    return 2;
  }

  const std::string topic = args.GetString("topic");
  const int retries = args.GetInt("retries");
  if (retries < 0 || args.GetInt("retry-backoff-ms") < 0 ||
      args.GetInt("retry-timeout-ms") <= 0) {
    sink.WriteError(Status(StatusCode::kUsage,
                           "retries/backoff must be >= 0 and timeout > 0"));
    return 2;
  }

  std::vector<std::string> lines;
  Status status = ReadInputLines(args, &lines);
  if (!status.ok()) {
    sink.WriteError(status);
    return 2;
  }

  // 解析每条输入；解析失败只影响该条（R23）。
  std::vector<RawMessage> messages;
  std::vector<EntryResult> results(lines.size());
  bool has_retry_exhausted = false;
  for (std::size_t i = 0; i < lines.size(); ++i) {
    RawMessage message;
    status = ParseMessageLine(lines[i], i, &message);
    if (!status.ok()) {
      EntryResult result;
      result.ok = false;
      result.code = status.code();
      results[i] = result;
      continue;
    }
    if (RecordTooLarge(message)) {
      EntryResult result;
      result.ok = false;
      result.code = StatusCode::kMessageTooLarge;
      results[i] = result;
      continue;
    }
    messages.push_back(message);
  }

  BrokerConnection connection;
  status = ConnectDefault(&connection);
  if (!status.ok()) {
    sink.WriteError(status);
    return 3;
  }

  // 分区数来自 ListTopics：同时完成 topic_not_found 校验。
  std::uint32_t partition_count = 0;
  {
    std::string payload;
    status = protocol::EncodeListTopicsRequest(&payload);
    protocol::Frame frame;
    if (status.ok()) {
      status = connection.Call(
          static_cast<std::uint16_t>(protocol::MessageType::kListTopics),
          payload, &frame);
    }
    protocol::ListTopicsResponse topics;
    if (status.ok()) {
      status = protocol::DecodeListTopicsResponse(frame.payload, &topics);
    }
    if (!status.ok()) {
      sink.WriteError(status);
      return 3;
    }
    bool found = false;
    for (std::size_t i = 0; i < topics.entries.size(); ++i) {
      if (topics.entries[i].topic == topic) {
        partition_count = topics.entries[i].partition_count;
        found = true;
        break;
      }
    }
    if (!found) {
      sink.WriteError(Status(StatusCode::kTopicNotFound,
                             "topic not found: " + topic));
      return 3;
    }
  }

  // 显式分区优先；越界是校验错误。
  bool has_partition = args.Has("partition");
  std::uint32_t fixed_partition = 0;
  if (has_partition) {
    std::uint64_t value = 0;
    const std::string text = args.GetString("partition");
    bool digits = !text.empty();
    for (std::size_t i = 0; i < text.size() && digits; ++i) {
      if (text[i] < '0' || text[i] > '9') {
        digits = false;
      } else {
        value = value * 10u + static_cast<std::uint64_t>(text[i] - '0');
      }
    }
    if (!digits) {
      sink.WriteError(Status(StatusCode::kUsage,
                             "--partition must be a non-negative integer"));
      return 2;
    }
    if (value >= partition_count) {
      sink.WriteError(Status(StatusCode::kInvalidPartition,
                             "partition out of range: " + text));
      return 3;
    }
    fixed_partition = static_cast<std::uint32_t>(value);
  }

  // R22：显式 > key 哈希 > 轮询。
  std::uint32_t round_robin = 0;
  std::map<std::uint32_t, std::vector<std::size_t>> by_partition;
  for (std::size_t i = 0; i < messages.size(); ++i) {
    std::uint32_t partition = 0;
    if (has_partition) {
      partition = fixed_partition;
    } else if ((messages[i].flags & storage::kFlagKeyPresent) != 0u) {
      partition = common::PartitionForKey(messages[i].key, partition_count);
    } else {
      partition = round_robin % partition_count;
      ++round_robin;
    }
    by_partition[partition].push_back(i);  // messages 内下标
    results[messages[i].index].partition = partition;
  }

  // 按分区（升序）分块发送，块内保持输入顺序（R2）。
  std::vector<std::size_t> global_indices(messages.size());
  for (std::size_t i = 0; i < messages.size(); ++i) {
    global_indices[i] = messages[i].index;
  }

  for (std::map<std::uint32_t, std::vector<std::size_t>>::const_iterator it =
           by_partition.begin();
       it != by_partition.end(); ++it) {
    const std::uint32_t partition = it->first;
    for (std::size_t start = 0; start < it->second.size();
         start += protocol::kMaxProduceEntries) {
      const std::size_t end =
          std::min<std::size_t>(it->second.size(),
                                start + protocol::kMaxProduceEntries);
      std::vector<const RawMessage*> chunk;
      std::vector<std::size_t> chunk_indices;
      chunk.reserve(end - start);
      chunk_indices.reserve(end - start);
      for (std::size_t i = start; i < end; ++i) {
        chunk.push_back(&messages[it->second[i]]);
        chunk_indices.push_back(messages[it->second[i]].index);
      }
      ProduceChunk(&connection, topic, partition, chunk, &chunk_indices[0],
                   retries, args.GetInt("retry-backoff-ms"),
                   args.GetInt("retry-timeout-ms"), &sink, &results,
                   &has_retry_exhausted);
    }
  }

  // 逐条结果按输入顺序输出（R23）。
  std::size_t succeeded = 0;
  std::size_t failed = 0;
  std::map<std::uint32_t, std::vector<std::uint64_t>> success_offsets;
  std::vector<std::size_t> failed_indices;
  for (std::size_t i = 0; i < results.size(); ++i) {
    if (results[i].ok) {
      ++succeeded;
      success_offsets[results[i].partition].push_back(results[i].offset);
      sink.WriteResult("{\"index\":" + Number(i) + ",\"partition\":" +
                       Number(results[i].partition) + ",\"offset\":" +
                       Number(results[i].offset) + ",\"status\":\"ok\"}");
    } else {
      ++failed;
      failed_indices.push_back(i);
      sink.WriteResult("{\"index\":" + Number(i) + ",\"partition\":" +
                       Number(results[i].partition) +
                       ",\"status\":\"error\",\"error\":" +
                       Quote(common::StatusCodeToString(results[i].code)) +
                       "}");
    }
  }

  if (failed != 0u) {
    std::string summary = "{\"event\":\"produce_summary\",\"status\":";
    summary += succeeded == 0u ? Quote("failed") : Quote("partial_failure");
    summary += ",\"succeeded\":" + Number(succeeded);
    summary += ",\"failed\":" + Number(failed);
    summary += ",\"success_offsets\":[";
    bool first_partition = true;
    for (std::map<std::uint32_t, std::vector<std::uint64_t>>::const_iterator it =
             success_offsets.begin();
         it != success_offsets.end(); ++it) {
      if (!first_partition) {
        summary += ",";
      }
      first_partition = false;
      summary += "{\"partition\":" + Number(it->first) + ",\"offsets\":[";
      for (std::size_t i = 0; i < it->second.size(); ++i) {
        if (i != 0u) {
          summary += ",";
        }
        summary += Number(it->second[i]);
      }
      summary += "]}";
    }
    summary += "],\"failed_indices\":[";
    for (std::size_t i = 0; i < failed_indices.size(); ++i) {
      if (i != 0u) {
        summary += ",";
      }
      summary += Number(failed_indices[i]);
    }
    summary += "],\"exit_reason\":";
    summary += has_retry_exhausted ? Quote("retry_exhausted")
                                   : Quote("validation_error");
    summary += "}";
    sink.WriteResult(summary);
  }
  sink.Flush();

  if (has_retry_exhausted) {
    return 4;
  }
  return failed == 0u ? 0 : 3;
}

}  // namespace client
}  // namespace minikafka
