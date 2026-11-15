#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "minikafka/client/cli.h"
#include "minikafka/client/connection.h"
#include "minikafka/common/args.h"
#include "minikafka/common/defaults.h"
#include "minikafka/common/event_sink.h"
#include "minikafka/protocol/frame.h"
#include "minikafka/protocol/messages.h"

namespace minikafka {
namespace client {

using common::Args;
using common::EventSink;
using common::Status;
using common::StatusCode;

namespace {

bool WantsHelp(const std::vector<std::string>& tokens) {
  for (std::size_t i = 0; i < tokens.size(); ++i) {
    if (tokens[i] == "--help" || tokens[i] == "-h") {
      return true;
    }
  }
  return false;
}

// topic/produce/consume/replay 的连接地址固定为默认 broker（03 §3.4 未开放参数）。
Status ConnectDefault(BrokerConnection* connection) {
  Address address;
  const Status parsed = ParseAddress(common::kDefaultBrokerAddress, &address);
  if (!parsed.ok()) {
    return parsed;
  }
  return connection->Connect(address.host, address.port);
}

bool ParseUint32(const std::string& text, std::uint32_t* out) {
  if (text.empty()) {
    return false;
  }
  std::uint64_t value = 0;
  for (std::size_t i = 0; i < text.size(); ++i) {
    const char ch = text[i];
    if (ch < '0' || ch > '9') {
      return false;
    }
    value = value * 10u + static_cast<std::uint64_t>(ch - '0');
    if (value > 0xFFFFFFFFull) {
      return false;
    }
  }
  *out = static_cast<std::uint32_t>(value);
  return true;
}

}  // namespace

int RunTopicCreate(const std::vector<std::string>& tokens) {
  if (WantsHelp(tokens)) {
    PrintTopicUsage();
    return 0;
  }
  EventSink sink;
  Args args;
  args.AddString("topic", "", true);
  // 分区数按字符串收，非法值要映射成 invalid_partitions（退出码 3）而不是用法错误。
  args.AddString("partitions", "", true);
  const Status parsed = args.Parse(tokens);
  if (!parsed.ok()) {
    PrintTopicUsage();
    sink.WriteError(parsed);
    return 2;
  }

  const std::string topic = args.GetString("topic");
  std::uint32_t partitions = 0;
  if (!ParseUint32(args.GetString("partitions"), &partitions) ||
      partitions < 1u || partitions > 16u) {
    sink.WriteError(Status(StatusCode::kInvalidPartitions,
                           "partitions must be an integer in 1..16"));
    return 3;
  }

  BrokerConnection connection;
  Status status = ConnectDefault(&connection);
  if (!status.ok()) {
    sink.WriteError(status);
    return 3;
  }

  protocol::CreateTopicRequest request;
  request.topic = topic;
  request.partition_count = partitions;
  protocol::CreateTopicResponse response;
  status = connection.Invoke(
      static_cast<std::uint16_t>(protocol::MessageType::kCreateTopic), request,
      &response, protocol::EncodeCreateTopicRequest,
      protocol::DecodeCreateTopicResponse);
  if (!status.ok()) {
    sink.WriteError(status);
    return 3;
  }
  if (response.error_code != StatusCode::kOk) {
    sink.WriteError(
        Status(response.error_code,
               response.message.empty() ? "create topic failed"
                                        : response.message));
    return 3;
  }

  sink.WriteResult("{\"topic\":" + Quote(response.topic) +
                   ",\"partitions\":" + Number(response.partition_count) +
                   ",\"status\":\"created\"}");
  sink.Flush();
  return 0;
}

int RunTopicList(const std::vector<std::string>& tokens) {
  if (WantsHelp(tokens)) {
    PrintTopicUsage();
    return 0;
  }
  EventSink sink;
  Args args;
  const Status parsed = args.Parse(tokens);
  if (!parsed.ok()) {
    PrintTopicUsage();
    sink.WriteError(parsed);
    return 2;
  }

  BrokerConnection connection;
  Status status = ConnectDefault(&connection);
  if (!status.ok()) {
    sink.WriteError(status);
    return 3;
  }

  std::string payload;
  status = protocol::EncodeListTopicsRequest(&payload);
  if (!status.ok()) {
    sink.WriteError(status);
    return 3;
  }
  protocol::Frame frame;
  status = connection.Call(
      static_cast<std::uint16_t>(protocol::MessageType::kListTopics), payload,
      &frame);
  if (!status.ok()) {
    sink.WriteError(status);
    return 3;
  }
  protocol::ListTopicsResponse response;
  status = protocol::DecodeListTopicsResponse(frame.payload, &response);
  if (!status.ok()) {
    sink.WriteError(status);
    return 3;
  }
  if (response.error_code != StatusCode::kOk) {
    sink.WriteError(Status(response.error_code, response.message));
    return 3;
  }

  if (response.entries.empty()) {
    sink.WriteResult("{\"topics\":[]}");
    sink.Flush();
    return 0;
  }
  for (std::size_t i = 0; i < response.entries.size(); ++i) {
    const protocol::TopicInfo& info = response.entries[i];
    std::string line = "{\"topic\":" + Quote(info.topic) +
                       ",\"partitions\":" + Number(info.partition_count) +
                       ",\"partition_ids\":[";
    for (std::uint32_t partition = 0; partition < info.partition_count;
         ++partition) {
      if (partition != 0u) {
        line += ",";
      }
      line += Number(partition);
    }
    line += "]}";
    sink.WriteResult(line);
  }
  sink.Flush();
  return 0;
}

}  // namespace client
}  // namespace minikafka
