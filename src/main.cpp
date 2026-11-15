#include <cstdint>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "minikafka/bench/bench.h"
#include "minikafka/broker/broker.h"
#include "minikafka/client/cli.h"
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
  std::cout
      << "usage: minikafka <command> [options]\n"
      << "  broker    前台启动 Broker（--port 9092 --data-dir ./data）\n"
      << "  topic     topic create|list\n"
      << "  produce   发送消息（--topic 必填）\n"
      << "  consume   消费消息（--topic/--group/--consumer-index/"
         "--consumer-count 必填）\n"
      << "  replay    按 offset 只读回放（--topic 必填）\n"
      << "  bench     压测（--topic/--messages 必填）\n"
      << "对任意子命令使用 --help 查看参数。\n";
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

bool WantsHelp(const std::vector<std::string>& tokens) {
  for (std::size_t i = 0; i < tokens.size(); ++i) {
    if (tokens[i] == "--help" || tokens[i] == "-h") {
      return true;
    }
  }
  return false;
}

std::vector<std::string> Tail(int argc, char** argv, int start_index) {
  std::vector<std::string> tokens;
  for (int i = start_index; i < argc; ++i) {
    tokens.push_back(argv[i]);
  }
  return tokens;
}

// mk broker [--port <port>] [--data-dir <dir>] ...
// 启动后前台常驻，打印 broker_started 与 ready 供脚本判定就绪（Q-P10）。
int RunBrokerCommand(const std::vector<std::string>& tokens) {
  EventSink sink;
  Args args;
  args.AddInt("port", minikafka::common::kDefaultBrokerPort);
  args.AddString("data-dir", minikafka::common::kDefaultDataDir);
  args.AddInt("io-threads", 0);       // 0 = 按 CPU 数取默认（03 §4.3）
  args.AddInt("storage-threads", 0);  // 同上
  args.AddInt("min-free-space-mb", minikafka::common::kDefaultMinFreeSpaceMb);
  args.AddString("segment-bytes",
                 std::to_string(minikafka::common::kDefaultSegmentBytes));
  args.AddString("log-level", minikafka::common::kDefaultLogLevel);

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

  const int io_threads = args.GetInt("io-threads");
  const int storage_threads = args.GetInt("storage-threads");
  if (io_threads < 0 || io_threads > 64 || storage_threads < 0 ||
      storage_threads > 64) {
    sink.WriteError(
        Status(StatusCode::kUsage, "io/storage threads must be in 0..64"));
    return 2;
  }

  const int min_free_mb = args.GetInt("min-free-space-mb");
  if (min_free_mb < 0) {
    sink.WriteError(
        Status(StatusCode::kUsage, "--min-free-space-mb must be >= 0"));
    return 2;
  }

  std::uint64_t segment_bytes = 0;
  if (!minikafka::client::ParseU64(args.GetString("segment-bytes"),
                                  &segment_bytes) ||
      segment_bytes <
          static_cast<std::uint64_t>(minikafka::common::kMinSegmentBytes)) {
    sink.WriteError(Status(StatusCode::kUsage,
                           "--segment-bytes must be >= 2097152"));
    return 2;
  }

  const std::string log_level = args.GetString("log-level");
  if (log_level != "warn" && log_level != "info" && log_level != "debug") {
    sink.WriteError(
        Status(StatusCode::kUsage, "--log-level must be warn, info or debug"));
    return 2;
  }

  BrokerConfig config;
  config.data_dir = args.GetString("data-dir");
  config.port = static_cast<std::uint16_t>(port);
  config.io_threads = static_cast<std::size_t>(io_threads);
  config.storage_workers = static_cast<std::size_t>(storage_threads);
  config.segment_bytes = segment_bytes;
  config.disk_threshold_bytes =
      static_cast<std::uint64_t>(min_free_mb) * 1024u * 1024u;

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

// mk topic <create|list> ...
int RunTopicCommand(const std::vector<std::string>& tokens) {
  if (tokens.empty()) {
    minikafka::client::PrintTopicUsage();
    return 2;
  }
  if (tokens[0] == "--help" || tokens[0] == "-h") {
    minikafka::client::PrintTopicUsage();
    return 0;
  }
  const std::vector<std::string> rest(tokens.begin() + 1, tokens.end());
  if (tokens[0] == "create") {
    return minikafka::client::RunTopicCreate(rest);
  }
  if (tokens[0] == "list") {
    return minikafka::client::RunTopicList(rest);
  }
  std::cerr << "unknown topic subcommand: " << tokens[0] << "\n";
  minikafka::client::PrintTopicUsage();
  return 2;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    PrintUsage();
    return 2;
  }

  const std::string command = argv[1];
  if (command == "--help" || command == "-h" || command == "help") {
    PrintUsage();
    return 0;
  }

  const std::vector<std::string> tokens = Tail(argc, argv, 2);

  if (command == "broker") {
    if (WantsHelp(tokens)) {
      std::cout << "usage: minikafka broker [options]\n"
                   "  --port 9092                 监听端口\n"
                   "  --data-dir ./data           数据目录\n"
                   "  --io-threads <n>            0 = min(4, max(1, cores/2))\n"
                   "  --storage-threads <n>       同上\n"
                   "  --min-free-space-mb 256     低于此值拒写（disk_full）\n"
                   "  --segment-bytes 268435456   分段大小，下限 2097152\n"
                   "  --log-level warn|info|debug\n";
      return 0;
    }
    return RunBrokerCommand(tokens);
  }
  if (command == "topic") {
    return RunTopicCommand(tokens);
  }
  if (command == "produce") {
    return minikafka::client::RunProduce(tokens);
  }
  if (command == "consume") {
    return minikafka::client::RunConsume(tokens);
  }
  if (command == "replay") {
    return minikafka::client::RunReplay(tokens);
  }
  if (command == "bench") {
    return minikafka::bench::RunBench(tokens);
  }

  std::cerr << "unknown command: " << command << "\n";
  PrintUsage();
  return 2;
}
