#include "minikafka/broker/broker.h"

#include <netinet/in.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <iostream>
#include <mutex>
#include <thread>
#include <utility>

#include "minikafka/common/hash.h"
#include "minikafka/group/coordinator.h"
#include "minikafka/protocol/codec.h"
#include "minikafka/protocol/messages.h"

namespace minikafka {
namespace broker {

using common::Status;
using common::StatusCode;
using common::StatusOr;
using protocol::Frame;
using protocol::MessageType;

namespace {

// 03 §4.3：IO / Storage 线程默认 min(4, max(1, cores/2))。
std::size_t DefaultThreadCount() {
  const unsigned cores = std::thread::hardware_concurrency();
  if (cores == 0u) {
    return 1u;
  }
  const std::size_t half = cores / 2u;
  return std::min<std::size_t>(4u, half == 0u ? 1u : half);
}

std::uint16_t ResponseType(std::uint16_t request_type) {
  return static_cast<std::uint16_t>(request_type | 0x8000u);
}

// stderr 告警行的转义（R19：每行一个 JSON 对象）。
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
      continue;
    } else {
      out.push_back(ch);
    }
  }
  return out;
}

// 建 Topic 时等待各 worker 打开分区完成；用 shared_ptr 持有，避免任务晚于栈变量执行。
struct PendingOpens {
  explicit PendingOpens(std::uint32_t total) : remaining(total) {}

  void Finish() {
    {
      std::lock_guard<std::mutex> guard(mutex);
      if (remaining > 0u) {
        --remaining;
      }
    }
    cv.notify_all();
  }

  void Wait(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait_for(lock, timeout, [this] { return remaining == 0u; });
  }

  std::mutex mutex;
  std::condition_variable cv;
  std::uint32_t remaining;
};

}  // namespace

// AD-12：有界 MPSC（互斥量 + 条件变量）+ 高低水位。
// 满时投递方最多等待 queue_push_timeout_ms；超时不阻塞 IO 线程，交给上层回
// broker_unavailable（客户端按 R3 重试），避免"IO 线程等队列、worker 等 IO 队列"的环形阻塞。
class Broker::TaskQueue {
 public:
  TaskQueue(std::size_t high_watermark, std::size_t low_watermark)
      : high_watermark_(high_watermark == 0u ? 1u : high_watermark),
        low_watermark_(std::min(low_watermark, high_watermark)),
        stopped_(false) {}

  bool Push(std::function<void()> task, int timeout_ms) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (stopped_) {
      return false;
    }
    if (tasks_.size() >= high_watermark_) {
      const bool room = space_cv_.wait_for(
          lock, std::chrono::milliseconds(timeout_ms), [this] {
            return stopped_ || tasks_.size() <= low_watermark_;
          });
      if (!room || stopped_) {
        return false;
      }
    }
    tasks_.push_back(std::move(task));
    task_cv_.notify_one();
    return true;
  }

  // 阻塞到有任务或 Stop；Stop 之后仍把已入队任务执行完（优雅退出）。
  bool Pop(std::function<void()>* task) {
    std::unique_lock<std::mutex> lock(mutex_);
    task_cv_.wait(lock, [this] { return stopped_ || !tasks_.empty(); });
    if (tasks_.empty()) {
      return false;
    }
    *task = std::move(tasks_.front());
    tasks_.pop_front();
    space_cv_.notify_one();
    return true;
  }

  void Stop() {
    std::lock_guard<std::mutex> guard(mutex_);
    stopped_ = true;
    space_cv_.notify_all();
    task_cv_.notify_all();
  }

 private:
  std::mutex mutex_;
  std::condition_variable space_cv_;
  std::condition_variable task_cv_;
  std::deque<std::function<void()>> tasks_;
  std::size_t high_watermark_;
  std::size_t low_watermark_;
  bool stopped_;
};

struct Broker::WorkerContext {
  WorkerContext(std::size_t worker_index, std::size_t high, std::size_t low,
                const std::string& data_dir,
                group::GroupCoordinator::WarningSink warning_sink)
      : index(worker_index),
        queue(high, low),
        coordinator(data_dir, std::move(warning_sink)) {}

  std::size_t index;
  TaskQueue queue;
  // AD-17：位点状态只由该 group 分片到的 worker 访问，故每个 worker 一份协调器。
  group::GroupCoordinator coordinator;
  std::thread thread;
};

Broker::Broker(const BrokerConfig& config)
    : config_(config),
      started_(false),
      stopping_(false),
      stopped_(false),
      cleanup_started_(false),
      cleanup_done_(false) {
  if (config_.io_threads == 0u) {
    config_.io_threads = DefaultThreadCount();
  }
  if (config_.storage_workers == 0u) {
    config_.storage_workers = DefaultThreadCount();
  }
}

Broker::~Broker() { Stop(); }

common::Status Broker::Start() {
  if (started_.exchange(true)) {
    return Status(StatusCode::kInternalError, "broker already started");
  }
  if (config_.data_dir.empty()) {
    return Status(StatusCode::kInternalError, "empty data directory");
  }

  // R30：扫描 meta 重建 Topic；单个损坏跳过并告警，不阻止启动。
  StatusOr<TopicRegistry> registry = TopicRegistry::Load(config_.data_dir);
  if (!registry.ok()) {
    return registry.status();
  }
  registry_.reset(new TopicRegistry(std::move(registry).value()));

  partitions_.reset(new PartitionManager(config_.data_dir,
                                         config_.segment_bytes,
                                         config_.disk_threshold_bytes,
                                         config_.storage_workers,
                                         config_.max_record_bytes));
  service_.reset(new PartitionService(partitions_.get()));

  // R7：恢复完成才对外服务。此处仍在单线程内，登记分区归属 worker 是安全的。
  const std::vector<TopicEntry> topics = registry_->List();
  for (std::size_t i = 0; i < topics.size(); ++i) {
    const Status open_status =
        partitions_->OpenTopic(topics[i].name, topics[i].partition_count);
    if (!open_status.ok()) {
      return open_status;
    }
  }

  const std::size_t worker_count = partitions_->worker_count();
  for (std::size_t i = 0; i < worker_count; ++i) {
    workers_.push_back(std::unique_ptr<WorkerContext>(new WorkerContext(
        i, config_.queue_high_watermark, config_.queue_low_watermark,
        config_.data_dir,
        [this](const std::string& detail) { EmitWarning(detail); })));
  }
  for (std::size_t i = 0; i < worker_count; ++i) {
    workers_[i]->thread = std::thread([this, i] { RunWorker(i); });
  }

  reactors_.reset(new net::ReactorPool(config_.io_threads));
  sockaddr_in address;
  std::memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_ANY);
  address.sin_port = htons(config_.port);
  const Status net_status = reactors_->Start(
      address, [this](net::ConnectionId id, const protocol::Frame& frame) {
        OnFrame(id, frame);
      });
  if (!net_status.ok()) {
    Stop();
    return net_status;
  }
  return Status();
}

common::Status Broker::Run() {
  if (!reactors_) {
    return Status(StatusCode::kInternalError, "broker is not started");
  }
  const Status status = reactors_->Run();
  Stop();
  return status;
}

void Broker::Stop() {
  std::unique_lock<std::mutex> lock(stop_mutex_);
  if (cleanup_done_) {
    return;
  }
  if (cleanup_started_) {
    stop_cv_.wait(lock, [this] { return cleanup_done_; });
    return;
  }
  cleanup_started_ = true;
  lock.unlock();

  stopping_.store(true);
  // 先停网络：迟到的完成事件不再写回已关闭的连接。
  if (reactors_) {
    reactors_->Stop();
  }
  StopWorkers();

  lock.lock();
  cleanup_done_ = true;
  stopped_.store(true);
  stop_cv_.notify_all();
}

void Broker::StopWorkers() {
  for (std::size_t i = 0; i < workers_.size(); ++i) {
    workers_[i]->queue.Stop();
  }
  for (std::size_t i = 0; i < workers_.size(); ++i) {
    if (workers_[i]->thread.joinable()) {
      workers_[i]->thread.join();
    }
  }
}

void Broker::RunWorker(std::size_t index) {
  TaskQueue& queue = workers_[index]->queue;
  std::function<void()> task;
  while (queue.Pop(&task)) {
    task();
  }
}

bool Broker::Submit(std::uint32_t partition_key, std::function<void()> task) {
  const std::size_t index = partitions_->WorkerIndexFor(partition_key);
  if (index >= workers_.size()) {
    return false;
  }
  return workers_[index]->queue.Push(std::move(task),
                                     config_.queue_push_timeout_ms);
}

bool Broker::SubmitToWorker(std::size_t worker_index,
                           std::function<void()> task) {
  if (worker_index >= workers_.size()) {
    return false;
  }
  return workers_[worker_index]->queue.Push(
      std::move(task), config_.queue_push_timeout_ms);
}

// AD-17：hash(group) % M 决定该 group 的位点状态归哪个 worker。
std::size_t Broker::GroupWorkerIndex(const std::string& group) const {
  if (workers_.empty()) {
    return 0u;
  }
  return static_cast<std::size_t>(common::Fnv1a32(group)) % workers_.size();
}

void Broker::EmitWarning(const std::string& detail) {
  std::lock_guard<std::mutex> guard(warn_mutex_);
  std::cerr << "{\"event\":\"warning\",\"detail\":\"" << EscapeJson(detail)
            << "\"}" << std::endl;
}

std::uint16_t Broker::port() const {
  return reactors_ ? reactors_->port() : config_.port;
}

std::size_t Broker::topic_count() const {
  return registry_ ? registry_->size() : 0u;
}

std::vector<std::string> Broker::warnings() const {
  std::vector<std::string> all;
  if (registry_) {
    const std::vector<std::string>& registry_warnings = registry_->warnings();
    all.insert(all.end(), registry_warnings.begin(), registry_warnings.end());
  }
  if (partitions_) {
    const std::vector<std::string> partition_warnings = partitions_->warnings();
    all.insert(all.end(), partition_warnings.begin(), partition_warnings.end());
  }
  return all;
}

void Broker::OnFrame(net::ConnectionId id, const protocol::Frame& frame) {
  Dispatch(frame, [this, id](std::string data) {
    if (reactors_) {
      (void)reactors_->Send(id, std::move(data));
    }
  });
}

void Broker::Reply(const ResponseSink& sink, std::uint16_t msg_type,
                   std::uint64_t request_id, const std::string& payload) {
  protocol::Frame frame;
  frame.msg_type = msg_type;
  frame.request_id = request_id;
  frame.payload = payload;
  std::string bytes;
  if (protocol::EncodeFrame(frame, &bytes).ok()) {
    sink(std::move(bytes));
  }
}

void Broker::ReplyError(const ResponseSink& sink, std::uint16_t request_type,
                        std::uint64_t request_id, common::StatusCode code,
                        const std::string& message) {
  std::string payload;
  protocol::Encoder encoder(payload);
  if (!protocol::EncodeResponsePrefix(code, message, &encoder).ok()) {
    return;
  }
  Reply(sink, ResponseType(request_type), request_id, payload);
}

void Broker::Dispatch(const protocol::Frame& frame, const ResponseSink& sink) {
  switch (static_cast<MessageType>(frame.msg_type)) {
    case MessageType::kCreateTopic:
      HandleCreateTopic(frame, sink);
      return;
    case MessageType::kListTopics:
      HandleListTopics(frame, sink);
      return;
    case MessageType::kProduce:
      HandleProduce(frame, sink);
      return;
    case MessageType::kFetch:
      HandleFetch(frame, sink);
      return;
    case MessageType::kReplay:
      HandleReplay(frame, sink);
      return;
    case MessageType::kOffsetFetch:
    case MessageType::kOffsetCommit:
      HandleGroupRequest(frame, sink);
      return;
    default:
      break;
  }
  // 05 §3 / §5：未知 msg_type 无对应响应体，只回错误前缀；客户端据此不重试并关闭。
  // 收到 >=0x8000 的类型说明对端把响应当请求发来，属协议违规，直接丢弃。
  if (frame.msg_type < 0x8000u) {
    ReplyError(sink, frame.msg_type, frame.request_id,
               StatusCode::kUnsupportedMessageType,
               "unsupported msg_type " + std::to_string(frame.msg_type));
  }
}

void Broker::HandleCreateTopic(const protocol::Frame& frame,
                               const ResponseSink& sink) {
  protocol::CreateTopicRequest request;
  protocol::CreateTopicResponse response;
  const Status decode_status =
      protocol::DecodeCreateTopicRequest(frame.payload, &request);
  if (!decode_status.ok()) {
    response.error_code = decode_status.code();
    response.message = decode_status.message();
  } else {
    Status create_status;
    {
      std::lock_guard<std::mutex> guard(registry_mutex_);
      create_status = registry_->Create(request.topic, request.partition_count);
    }
    if (!create_status.ok()) {
      response.error_code = create_status.code();
      response.message = create_status.message();
    } else {
      // 分区必须在自己的 worker 线程上打开（AD-05）。建 Topic 是低频管理命令，
      // 因此同步等待打开完成，保证回 ok 之后该 Topic 立即可写。
      partitions_->PublishTopicCount(request.topic, request.partition_count);
      std::shared_ptr<PendingOpens> pending =
          std::make_shared<PendingOpens>(request.partition_count);
      for (std::uint32_t partition = 0; partition < request.partition_count;
           ++partition) {
        const std::uint32_t key = PartitionKeyFor(request.topic, partition);
        const std::string topic = request.topic;
        const std::uint32_t partition_count = request.partition_count;
        const bool submitted =
            Submit(key, [this, topic, partition, partition_count, pending] {
              (void)partitions_->OpenPartition(topic, partition,
                                               partition_count);
              pending->Finish();
            });
        if (!submitted) {
          pending->Finish();
        }
      }
      pending->Wait(std::chrono::milliseconds(5000));
      response.topic = request.topic;
      response.partition_count = request.partition_count;
    }
  }

  std::string payload;
  if (protocol::EncodeCreateTopicResponse(response, &payload).ok()) {
    Reply(sink, static_cast<std::uint16_t>(MessageType::kCreateTopicResponse),
          frame.request_id, payload);
  }
}

void Broker::HandleListTopics(const protocol::Frame& frame,
                              const ResponseSink& sink) {
  protocol::ListTopicsResponse response;
  const Status decode_status =
      protocol::DecodeListTopicsRequest(frame.payload);
  if (!decode_status.ok()) {
    response.error_code = decode_status.code();
    response.message = decode_status.message();
  } else {
    std::vector<TopicEntry> entries;
    {
      std::lock_guard<std::mutex> guard(registry_mutex_);
      entries = registry_->List();
    }
    response.entries.reserve(entries.size());
    for (std::size_t i = 0; i < entries.size(); ++i) {
      protocol::TopicInfo info;
      info.topic = entries[i].name;
      info.partition_count = entries[i].partition_count;
      response.entries.push_back(info);
    }
  }

  std::string payload;
  if (protocol::EncodeListTopicsResponse(response, &payload).ok()) {
    Reply(sink, static_cast<std::uint16_t>(MessageType::kListTopicsResponse),
          frame.request_id, payload);
  }
}

void Broker::HandleProduce(const protocol::Frame& frame,
                           const ResponseSink& sink) {
  const std::uint64_t request_id = frame.request_id;
  protocol::ProduceRequest request;
  const Status decode_status =
      protocol::DecodeProduceRequest(frame.payload, &request);
  if (decode_status.ok()) {
    const std::uint32_t key = PartitionKeyFor(request.topic, request.partition);
    const bool submitted = Submit(key, [this, request, request_id, sink] {
      const protocol::ProduceResponse response = service_->Produce(request);
      std::string payload;
      if (protocol::EncodeProduceResponse(response, &payload).ok()) {
        Reply(sink, static_cast<std::uint16_t>(MessageType::kProduceResponse),
              request_id, payload);
      }
    });
    if (submitted) {
      return;
    }
  }

  // 解码失败或 worker 队列拥塞：回请求级错误（status=2、result_count=0）。
  protocol::ProduceResponse response;
  response.error_code = decode_status.ok() ? StatusCode::kBrokerUnavailable
                                           : decode_status.code();
  response.message = decode_status.ok() ? "storage worker queue is full"
                                        : decode_status.message();
  response.status = 2u;
  std::string payload;
  if (protocol::EncodeProduceResponse(response, &payload).ok()) {
    Reply(sink, static_cast<std::uint16_t>(MessageType::kProduceResponse),
          request_id, payload);
  }
}

void Broker::HandleFetch(const protocol::Frame& frame,
                         const ResponseSink& sink) {
  const std::uint64_t request_id = frame.request_id;
  protocol::FetchRequest request;
  const Status decode_status =
      protocol::DecodeFetchRequest(frame.payload, &request);
  if (decode_status.ok()) {
    const std::uint32_t key = PartitionKeyFor(request.topic, request.partition);
    const bool submitted = Submit(key, [this, request, request_id, sink] {
      const protocol::FetchResponse response = service_->Fetch(request);
      std::string payload;
      if (protocol::EncodeFetchResponse(response, &payload).ok()) {
        Reply(sink, static_cast<std::uint16_t>(MessageType::kFetchResponse),
              request_id, payload);
      }
    });
    if (submitted) {
      return;
    }
  }

  protocol::FetchResponse response;
  response.error_code = decode_status.ok() ? StatusCode::kBrokerUnavailable
                                           : decode_status.code();
  response.message = decode_status.ok() ? "storage worker queue is full"
                                        : decode_status.message();
  std::string payload;
  if (protocol::EncodeFetchResponse(response, &payload).ok()) {
    Reply(sink, static_cast<std::uint16_t>(MessageType::kFetchResponse),
          request_id, payload);
  }
}

void Broker::HandleReplay(const protocol::Frame& frame,
                          const ResponseSink& sink) {
  const std::uint64_t request_id = frame.request_id;
  protocol::ReplayRequest request;
  const Status decode_status =
      protocol::DecodeReplayRequest(frame.payload, &request);
  if (decode_status.ok()) {
    const std::uint32_t key = PartitionKeyFor(request.topic, request.partition);
    const bool submitted = Submit(key, [this, request, request_id, sink] {
      const protocol::ReplayResponse response = service_->Replay(request);
      std::string payload;
      if (protocol::EncodeReplayResponse(response, &payload).ok()) {
        Reply(sink, static_cast<std::uint16_t>(MessageType::kReplayResponse),
              request_id, payload);
      }
    });
    if (submitted) {
      return;
    }
  }

  protocol::ReplayResponse response;
  response.error_code = decode_status.ok() ? StatusCode::kBrokerUnavailable
                                           : decode_status.code();
  response.message = decode_status.ok() ? "storage worker queue is full"
                                        : decode_status.message();
  std::string payload;
  if (protocol::EncodeReplayResponse(response, &payload).ok()) {
    Reply(sink, static_cast<std::uint16_t>(MessageType::kReplayResponse),
          request_id, payload);
  }
}

void Broker::HandleGroupRequest(const protocol::Frame& frame,
                                const ResponseSink& sink) {
  if (frame.msg_type ==
      static_cast<std::uint16_t>(MessageType::kOffsetFetch)) {
    HandleOffsetFetch(frame, sink);
    return;
  }
  HandleOffsetCommit(frame, sink);
}

// 05 §3.6：返回静态分配与已提交位点。分配是纯函数，位点状态在 group 分片 worker 上。
void Broker::HandleOffsetFetch(const protocol::Frame& frame,
                               const ResponseSink& sink) {
  const std::uint64_t request_id = frame.request_id;
  protocol::OffsetFetchRequest request;
  const Status decode_status =
      protocol::DecodeOffsetFetchRequest(frame.payload, &request);

  StatusOr<std::uint32_t> partition_count =
      decode_status.ok()
          ? TopicPartitionCount(request.topic)
          : StatusOr<std::uint32_t>(decode_status);
  if (!partition_count.ok()) {
    protocol::OffsetFetchResponse response;
    response.error_code = partition_count.status().code();
    response.message = partition_count.status().message();
    std::string payload;
    if (protocol::EncodeOffsetFetchResponse(response, &payload).ok()) {
      Reply(sink,
            static_cast<std::uint16_t>(MessageType::kOffsetFetchResponse),
            request_id, payload);
    }
    return;
  }

  const std::size_t worker = GroupWorkerIndex(request.group);
  const std::uint32_t count = partition_count.value();
  const bool submitted = SubmitToWorker(
      worker, [this, request, count, worker, request_id, sink] {
        protocol::OffsetFetchResponse response;
        StatusOr<group::FetchOutcome> outcome =
            workers_[worker]->coordinator.Fetch(
                request.group, request.topic, request.consumer_index,
                request.consumer_count, count);
        if (!outcome.ok()) {
          response.error_code = outcome.status().code();
          response.message = outcome.status().message();
        } else {
          response.assigned_partitions = outcome.value().assigned_partitions;
          response.entries.reserve(outcome.value().entries.size());
          for (std::size_t i = 0; i < outcome.value().entries.size(); ++i) {
            protocol::OffsetFetchEntry entry;
            entry.partition = outcome.value().entries[i].partition;
            entry.has_committed = outcome.value().entries[i].has_committed;
            entry.committed_offset =
                outcome.value().entries[i].committed_offset;
            response.entries.push_back(entry);
          }
        }
        std::string payload;
        if (protocol::EncodeOffsetFetchResponse(response, &payload).ok()) {
          Reply(sink,
                static_cast<std::uint16_t>(MessageType::kOffsetFetchResponse),
                request_id, payload);
        }
      });
  if (submitted) {
    return;
  }

  protocol::OffsetFetchResponse response;
  response.error_code = StatusCode::kBrokerUnavailable;
  response.message = "storage worker queue is full";
  std::string payload;
  if (protocol::EncodeOffsetFetchResponse(response, &payload).ok()) {
    Reply(sink, static_cast<std::uint16_t>(MessageType::kOffsetFetchResponse),
          request_id, payload);
  }
}

// 05 §3.7 + S-13：全量快照合并、同分区取 max 不回退、任一越界整体不应用。
void Broker::HandleOffsetCommit(const protocol::Frame& frame,
                                const ResponseSink& sink) {
  const std::uint64_t request_id = frame.request_id;
  protocol::OffsetCommitRequest request;
  const Status decode_status =
      protocol::DecodeOffsetCommitRequest(frame.payload, &request);

  StatusOr<std::uint32_t> partition_count =
      decode_status.ok()
          ? TopicPartitionCount(request.topic)
          : StatusOr<std::uint32_t>(decode_status);
  if (!partition_count.ok()) {
    protocol::OffsetCommitResponse response;
    response.error_code = partition_count.status().code();
    response.message = partition_count.status().message();
    std::string payload;
    if (protocol::EncodeOffsetCommitResponse(response, &payload).ok()) {
      Reply(sink,
            static_cast<std::uint16_t>(MessageType::kOffsetCommitResponse),
            request_id, payload);
    }
    return;
  }

  std::vector<storage::OffsetEntry> updates;
  updates.reserve(request.entries.size());
  for (std::size_t i = 0; i < request.entries.size(); ++i) {
    storage::OffsetEntry entry;
    entry.partition = request.entries[i].partition;
    entry.committed_offset = request.entries[i].committed_offset;
    updates.push_back(entry);
  }

  const std::size_t worker = GroupWorkerIndex(request.group);
  const std::uint32_t count = partition_count.value();
  const bool submitted = SubmitToWorker(
      worker, [this, request, updates, count, worker, request_id, sink] {
        protocol::OffsetCommitResponse response;
        StatusOr<std::uint32_t> applied = workers_[worker]->coordinator.Commit(
            request.group, request.topic, updates, count);
        if (!applied.ok()) {
          response.error_code = applied.status().code();
          response.message = applied.status().message();
        } else {
          response.applied_count = applied.value();
        }
        std::string payload;
        if (protocol::EncodeOffsetCommitResponse(response, &payload).ok()) {
          Reply(sink,
                static_cast<std::uint16_t>(MessageType::kOffsetCommitResponse),
                request_id, payload);
        }
      });
  if (submitted) {
    return;
  }

  protocol::OffsetCommitResponse response;
  response.error_code = StatusCode::kBrokerUnavailable;
  response.message = "storage worker queue is full";
  std::string payload;
  if (protocol::EncodeOffsetCommitResponse(response, &payload).ok()) {
    Reply(sink, static_cast<std::uint16_t>(MessageType::kOffsetCommitResponse),
          request_id, payload);
  }
}

common::StatusOr<std::uint32_t> Broker::TopicPartitionCount(
    const std::string& topic) const {
  std::lock_guard<std::mutex> guard(registry_mutex_);
  return registry_->PartitionCount(topic);
}

common::Status Broker::DispatchSync(const protocol::Frame& request,
                                    protocol::Frame* response) {
  if (response == nullptr) {
    return Status(StatusCode::kInternalError, "null response output");
  }
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  std::string bytes;
  Dispatch(request, [&mutex, &cv, &done, &bytes](std::string data) {
    {
      std::lock_guard<std::mutex> guard(mutex);
      bytes = std::move(data);
      done = true;
    }
    cv.notify_all();
  });

  std::unique_lock<std::mutex> lock(mutex);
  if (!cv.wait_for(lock, std::chrono::milliseconds(5000),
                   [&done] { return done; })) {
    return Status(StatusCode::kInternalError, "dispatch timed out");
  }
  const std::string payload = bytes;
  lock.unlock();

  protocol::FrameDecoder decoder;
  const Status feed_status = decoder.Feed(payload);
  if (!feed_status.ok()) {
    return feed_status;
  }
  bool has_frame = false;
  const Status next_status = decoder.Next(response, &has_frame);
  if (!next_status.ok()) {
    return next_status;
  }
  if (!has_frame) {
    return Status(StatusCode::kInternalError, "no response frame produced");
  }
  return Status();
}

}  // namespace broker
}  // namespace minikafka
