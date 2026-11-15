#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "minikafka/broker/broker.h"
#include "minikafka/common/args.h"
#include "minikafka/common/defaults.h"
#include "minikafka/common/event_sink.h"
#include "minikafka/common/status.h"

namespace {

using minikafka::broker::Broker;
using minikafka::broker::BrokerConfig;
using minikafka::common::Args;
using minikafka::common::EventSink;
using minikafka::common::Status;
using minikafka::common::StatusCode;

void PrintUsage() {
  std::cerr << "usage: minikafka <broker|topic|produce|consume|replay|bench> "
               "[options]\n";
}

// 事件行里的字符串转义（R19：每行一个 JSON 对象）。
std::string EscapeJson(const std::string& text) {
  std::string out;
  out.reserve(text.size() + 8u);
  for (std::size_t i = 0; i < text.size(); ++i) {
    const char ch = text[i];
    if (ch == '"' || ch == '\\') {
      out.push_back('\\');
      out.push_back(ch);
    } else if (ch == '\n') {
      out += "\\n";
    } else if (ch == '\r') {
      out += "\\r";
    } else if (ch == '\t') {
      out += "\\t";
    } else if (static_cast<unsigned char>(ch) < 0x20u) {
      continue;  // 控制字符直接丢弃，避免破坏 JSON Lines
    } else {
      out.push_back(ch);
    }
  }
  return out;
}

// mk broker [--port <port>] [--data-dir <dir>]
// 启动后前台常驻，打印 broker_started 与 ready 供脚本判定就绪（Q-P10）。
int RunBrokerCommand(const std::vector<std::string>& tokens) {
  EventSink sink;
  Args args;
  args.AddInt("port", minikafka::common::kDefaultBrokerPort);
  args.AddString("data-dir", minikafka::common::kDefaultDataDir);

  const Status parsed = args.Parse(tokens);
  if (!parsed.ok()) {
    sink.WriteError(parsed);
    return 2;
  }

  const int port = args.GetInt("port");
  if (port < 0 || port > 65535) {
    sink.WriteError(Status(StatusCode::kUsage, "port must be in 0..65535"));
    return 2;
  }

  BrokerConfig config;
  config.data_dir = args.GetString("data-dir");
  config.port = static_cast<std::uint16_t>(port);

  Broker broker(config);
  const Status started = broker.Start();
  if (!started.ok()) {
    sink.WriteError(started);
    return 1;
  }

  sink.WriteResult("{\"event\":\"broker_started\",\"port\":" +
                   std::to_string(broker.port()) + ",\"data_dir\":\"" +
                   EscapeJson(config.data_dir) +
                   "\",\"status\":\"running\"}");
  const std::vector<std::string> warnings = broker.warnings();
  for (std::size_t i = 0; i < warnings.size(); ++i) {
    sink.WriteEvent("{\"event\":\"recovery_warning\",\"detail\":\"" +
                    EscapeJson(warnings[i]) + "\"}");
  }
  sink.WriteResult("{\"event\":\"ready\"}");
  sink.Flush();

  const Status run = broker.Run();
  if (!run.ok()) {
    sink.WriteError(run);
    return 1;
  }
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    PrintUsage();
    return 2;
  }

  if (std::strcmp(argv[1], "broker") == 0) {
    std::vector<std::string> tokens;
    for (int i = 2; i < argc; ++i) {
      tokens.push_back(argv[i]);
    }
    return RunBrokerCommand(tokens);
  }

  const char* const kPending[] = {"topic", "produce", "consume", "replay",
                                  "bench"};
  for (const char* command : kPending) {
    if (std::strcmp(argv[1], command) == 0) {
      std::cerr << "not implemented: " << command << "\n";
      return 2;
    }
  }

  PrintUsage();
  return 2;
}
