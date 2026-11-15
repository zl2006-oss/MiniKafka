#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
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

}  // namespace

int RunReplay(const std::vector<std::string>& tokens) {
  if (WantsHelp(tokens)) {
    PrintReplayUsage();
    return 0;
  }
  EventSink sink;
  Args args;
  args.AddString("topic", "", true);
  args.AddString("partition", "0");
  args.AddString("offset", "0");
  args.AddString("count", "");
  const Status parsed = args.Parse(tokens);
  if (!parsed.ok()) {
    PrintReplayUsage();
    sink.WriteError(parsed);
    return 2;
  }

  const std::string topic = args.GetString("topic");

  std::uint64_t partition = 0;
  if (!ParseU64(args.GetString("partition"), &partition) ||
      partition > 0xFFFFFFFFu) {
    sink.WriteError(Status(StatusCode::kUsage,
                           "--partition must be a non-negative integer"));
    return 2;
  }
  std::uint64_t offset = 0;
  if (!ParseU64(args.GetString("offset"), &offset)) {
    sink.WriteError(Status(StatusCode::kUsage,
                           "--offset must be a non-negative integer"));
    return 2;
  }
  // count 缺省表示"读到末尾"：用 0 交给 Broker 解释（05 §3.8）。
  std::uint64_t remaining = 0;
  const bool has_count = args.Has("count");
  if (has_count && (!ParseU64(args.GetString("count"), &remaining) ||
                    remaining == 0u)) {
    sink.WriteError(
        Status(StatusCode::kUsage, "--count must be a positive integer"));
    return 2;
  }

  BrokerConnection connection;
  Status status = ConnectDefault(&connection);
  if (!status.ok()) {
    sink.WriteError(Status(StatusCode::kBrokerUnavailable,
                           "cannot connect to broker: " + status.message()));
    return 3;
  }

  std::uint64_t emitted = 0;
  while (true) {
    protocol::ReplayRequest request;
    request.topic = topic;
    request.partition = static_cast<std::uint32_t>(partition);
    request.offset = offset;
    request.count = 0;
    if (has_count) {
      const std::uint64_t left = remaining - emitted;
      // Broker 侧单次上限 500 条；这里按剩余量请求，超出由 Broker 截断。
      request.count = static_cast<std::uint32_t>(left > 0xFFFFFFFFu
                                                     ? 0xFFFFFFFFu
                                                     : left);
      if (request.count == 0u) {
        break;
      }
    }

    protocol::ReplayResponse response;
    status = connection.Invoke(
        static_cast<std::uint16_t>(protocol::MessageType::kReplay), request,
        &response, protocol::EncodeReplayRequest,
        protocol::DecodeReplayResponse);
    if (!status.ok()) {
      sink.WriteError(status);
      return 3;
    }
    if (response.error_code != StatusCode::kOk) {
      sink.WriteError(Status(response.error_code, response.message));
      return 3;
    }
    if (response.records.empty()) {
      break;  // 已到达末尾
    }

    for (std::size_t i = 0; i < response.records.size(); ++i) {
      const protocol::FetchRecord& record = response.records[i];
      sink.WriteResult(RecordLine(topic, static_cast<std::uint32_t>(partition),
                                  record.offset,
                                  (record.flags & storage::kFlagKeyPresent) != 0u,
                                  record.key, record.value));
      if (!std::cout.good()) {
        sink.Flush();
        sink.WriteError(Status(StatusCode::kInternalError,
                               "stdout write failed, stopping replay"));
        return 3;
      }
      offset = record.offset + 1u;
      ++emitted;
    }

    if (has_count && emitted >= remaining) {
      break;
    }
  }

  sink.Flush();
  return 0;
}

}  // namespace client
}  // namespace minikafka
