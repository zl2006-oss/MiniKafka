#include "minikafka/bench/bench.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "minikafka/bench/stats.h"
#include "minikafka/client/cli.h"
#include "minikafka/client/connection.h"
#include "minikafka/common/args.h"
#include "minikafka/common/defaults.h"
#include "minikafka/common/event_sink.h"
#include "minikafka/protocol/frame.h"
#include "minikafka/protocol/messages.h"
#include "minikafka/storage/record.h"

namespace minikafka {
namespace bench {

using client::Address;
using client::BrokerConnection;
using client::Number;
using client::ParseAddress;
using client::ParseU64;
using client::Quote;
using common::Args;
using common::EventSink;
using common::Status;
using common::StatusCode;

namespace {

constexpr std::uint64_t kMaxProducers = 64u;
constexpr std::uint64_t kDefaultBatch = 100u;

bool WantsHelp(const std::vector<std::string>& tokens) {
  for (std::size_t i = 0; i < tokens.size(); ++i) {
    if (tokens[i] == "--help" || tokens[i] == "-h") {
      return true;
    }
  }
  return false;
}

// 单个 Producer 线程的产出：成功条数、失败请求数、以及每个请求的 ack 延迟样本。
struct WorkerResult {
  std::uint64_t succeeded = 0;
  std::uint64_t failed_batches = 0;
  std::vector<double> latency_ms;
};

// R26：固定负载（payload 100B + key 8B）与固定分区轮转，保证可复现。
void ProduceWorker(const std::string& host, std::uint16_t port,
                   const std::string& topic, std::uint32_t partition_count,
                   std::uint64_t messages, std::uint32_t batch,
                   const std::string& key, const std::string& value,
                   std::uint32_t producer_index, WorkerResult* result) {
  BrokerConnection connection;
  const Status connected = connection.Connect(host, port);
  if (!connected.ok()) {
    result->failed_batches = 1u;
    return;
  }

  std::uint64_t sent = 0;
  std::uint32_t round = 0;
  while (sent < messages) {
    std::uint64_t count = messages - sent;
    if (count > batch) {
      count = batch;
    }

    protocol::ProduceRequest request;
    request.topic = topic;
    // 显式分区轮转：跨分区并行，避免单分区串行成为瓶颈（R5）。
    request.partition =
        partition_count == 0u
            ? 0u
            : static_cast<std::uint32_t>((producer_index + round) %
                                         partition_count);
    request.entries.reserve(static_cast<std::size_t>(count));
    for (std::uint64_t i = 0; i < count; ++i) {
      protocol::ProduceEntry entry;
      entry.flags = storage::kFlagKeyPresent;
      entry.key = key;
      entry.value = value;
      request.entries.push_back(entry);
    }

    const std::int64_t started = client::NowMs();
    protocol::ProduceResponse response;
    const Status status = connection.Invoke(
        static_cast<std::uint16_t>(protocol::MessageType::kProduce), request,
        &response, protocol::EncodeProduceRequest,
        protocol::DecodeProduceResponse);
    const std::int64_t elapsed = client::NowMs() - started;

    result->latency_ms.push_back(static_cast<double>(elapsed));

    if (!status.ok() || response.error_code != StatusCode::kOk) {
      // 单批次失败只记录，不中断整体压测（否则吞吐不可测）。
      ++result->failed_batches;
      if (!connection.connected()) {
        (void)connection.Reconnect();
      }
      sent += count;
      ++round;
      continue;
    }

    std::uint64_t appended = 0;
    for (std::size_t i = 0; i < response.results.size(); ++i) {
      if (response.results[i].status == 0u &&
          response.results[i].error_code == StatusCode::kOk) {
        ++appended;
      }
    }
    result->succeeded += appended;
    if (appended != count) {
      ++result->failed_batches;
    }
    sent += count;
    ++round;
  }
  connection.Close();
}

}  // namespace

void PrintBenchUsage() {
  std::cout
      << "usage: minikafka bench --topic <name> --messages <n> [options]\n"
      << "  --broker <host:port>         默认 127.0.0.1:9092\n"
      << "  --mode throughput|latency    默认 throughput\n"
      << "  --producers <n>              并发 Producer 数，默认 4（上限 64）\n"
      << "  --messages <n>               总消息数（必填）\n"
      << "  --batch <n>                  单请求条数，默认 100（≤1024）\n"
      << "  --payload-bytes <n>          value 字节数，默认 100\n"
      << "  --key-bytes <n>              key 字节数，默认 8\n";
}

int RunBench(const std::vector<std::string>& tokens) {
  if (WantsHelp(tokens)) {
    PrintBenchUsage();
    return 0;
  }
  EventSink sink;
  Args args;
  args.AddString("broker", common::kDefaultBrokerAddress);
  args.AddString("topic", "", true);
  args.AddString("mode", common::kDefaultBenchMode);
  args.AddString("producers", "4");
  args.AddString("messages", "", true);
  args.AddString("batch", "100");
  args.AddString("payload-bytes", "100");
  args.AddString("key-bytes", "8");
  const Status parsed = args.Parse(tokens);
  if (!parsed.ok()) {
    PrintBenchUsage();
    sink.WriteError(parsed);
    return 2;
  }

  Address address;
  Status status = ParseAddress(args.GetString("broker"), &address);
  if (!status.ok()) {
    sink.WriteError(status);
    return 2;
  }

  const std::string mode = args.GetString("mode");
  if (mode != "throughput" && mode != "latency") {
    sink.WriteError(
        Status(StatusCode::kUsage, "--mode must be throughput or latency"));
    return 2;
  }

  std::uint64_t producers = 0;
  std::uint64_t messages = 0;
  std::uint64_t batch = 0;
  std::uint64_t payload_bytes = 0;
  std::uint64_t key_bytes = 0;
  if (!ParseU64(args.GetString("producers"), &producers) || producers == 0u ||
      producers > kMaxProducers) {
    sink.WriteError(Status(StatusCode::kUsage,
                           "--producers must be in 1..64"));
    return 2;
  }
  if (!ParseU64(args.GetString("messages"), &messages) || messages == 0u) {
    sink.WriteError(
        Status(StatusCode::kUsage, "--messages must be a positive integer"));
    return 2;
  }
  const std::string batch_text = args.GetString("batch");
  if (batch_text.empty()) {
    batch = kDefaultBatch;
  } else if (!ParseU64(batch_text, &batch) || batch == 0u ||
             batch > protocol::kMaxProduceEntries) {
    sink.WriteError(Status(StatusCode::kUsage, "--batch must be in 1..1024"));
    return 2;
  }
  if (!ParseU64(args.GetString("payload-bytes"), &payload_bytes) ||
      payload_bytes > 1024u * 1024u) {
    sink.WriteError(Status(StatusCode::kUsage,
                           "--payload-bytes must be in 0..1048576"));
    return 2;
  }
  if (!ParseU64(args.GetString("key-bytes"), &key_bytes) ||
      key_bytes > 1024u * 1024u) {
    sink.WriteError(
        Status(StatusCode::kUsage, "--key-bytes must be in 0..1048576"));
    return 2;
  }
  // 延迟模式按 R4 的口径逐条测量，batch 固定为 1。
  if (mode == "latency") {
    batch = 1u;
  }
  if (producers > messages) {
    producers = messages;
  }

  const std::string topic = args.GetString("topic");
  BrokerConnection probe;
  status = probe.Connect(address.host, address.port);
  if (!status.ok()) {
    sink.WriteError(Status(StatusCode::kBrokerUnavailable,
                           "cannot connect to broker: " + status.message()));
    return 3;
  }

  // 分区数用于轮转；顺带完成 topic 存在性校验。
  std::uint32_t partition_count = 0;
  {
    std::string payload;
    status = protocol::EncodeListTopicsRequest(&payload);
    protocol::Frame frame;
    if (status.ok()) {
      status = probe.Call(
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
  probe.Close();

  const std::string key = std::string(static_cast<std::size_t>(key_bytes), 'k');
  const std::string value =
      std::string(static_cast<std::size_t>(payload_bytes), 'x');

  const std::uint64_t per_worker = messages / producers;
  const std::uint64_t remainder = messages % producers;

  std::vector<WorkerResult> results(static_cast<std::size_t>(producers));
  std::vector<std::thread> threads;
  threads.reserve(static_cast<std::size_t>(producers));

  const std::int64_t started = client::NowMs();
  for (std::uint64_t i = 0; i < producers; ++i) {
    const std::uint64_t quota = per_worker + (i < remainder ? 1u : 0u);
    WorkerResult* target = &results[static_cast<std::size_t>(i)];
    threads.push_back(std::thread(ProduceWorker, address.host, address.port,
                                  topic, partition_count, quota,
                                  static_cast<std::uint32_t>(batch), key, value,
                                  static_cast<std::uint32_t>(i), target));
  }
  for (std::size_t i = 0; i < threads.size(); ++i) {
    threads[i].join();
  }
  const std::int64_t duration_ms = client::NowMs() - started;

  std::uint64_t succeeded = 0;
  std::uint64_t failed_batches = 0;
  std::vector<double> samples;
  for (std::size_t i = 0; i < results.size(); ++i) {
    succeeded += results[i].succeeded;
    failed_batches += results[i].failed_batches;
    samples.insert(samples.end(), results[i].latency_ms.begin(),
                   results[i].latency_ms.end());
  }
  std::sort(samples.begin(), samples.end());

  const double seconds =
      duration_ms > 0 ? static_cast<double>(duration_ms) / 1000.0 : 0.001;
  const double throughput = static_cast<double>(succeeded) / seconds;

  std::string line = "{\"event\":\"bench_result\",\"mode\":" + Quote(mode);
  line += ",\"topic\":" + Quote(topic);
  line += ",\"partitions\":" + Number(partition_count);
  line += ",\"producers\":" + Number(producers);
  line += ",\"messages\":" + Number(messages);
  line += ",\"batch\":" + Number(batch);
  line += ",\"payload_bytes\":" + Number(payload_bytes);
  line += ",\"key_bytes\":" + Number(key_bytes);
  line += ",\"duration_ms\":" +
          Number(static_cast<std::uint64_t>(duration_ms > 0 ? duration_ms : 0));
  line += ",\"succeeded\":" + Number(succeeded);
  line += ",\"failed_batches\":" + Number(failed_batches);
  line += ",\"throughput_msg_per_sec\":" + Fixed(throughput);
  line += ",\"latency_p50_ms\":" + Fixed(Percentile(samples, 0.50));
  line += ",\"latency_p99_ms\":" + Fixed(Percentile(samples, 0.99));
  line += ",\"latency_max_ms\":" +
          Fixed(samples.empty() ? 0.0 : samples.back());
  line += ",\"samples\":" + Number(samples.size());
  line += "}";
  sink.WriteResult(line);
  sink.Flush();

  if (failed_batches != 0u) {
    sink.WriteError(Status(StatusCode::kDeliveryFailed,
                           "bench finished with failed batches: " +
                               std::to_string(failed_batches)));
    return 3;
  }
  return 0;
}

}  // namespace bench
}  // namespace minikafka
