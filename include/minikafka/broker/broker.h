#ifndef MINIKAFKA_BROKER_BROKER_H_
#define MINIKAFKA_BROKER_BROKER_H_

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "minikafka/broker/partition_manager.h"
#include "minikafka/broker/service.h"
#include "minikafka/broker/topic_registry.h"
#include "minikafka/common/status.h"
#include "minikafka/net/reactor_pool.h"
#include "minikafka/protocol/frame.h"

namespace minikafka {
namespace broker {

// 03 §3.4 B4 / 04 §2 的默认值集合；M7 的 CLI 直接覆盖这里。
struct BrokerConfig {
  std::string data_dir = "./data";
  std::uint16_t port = 9092;
  std::size_t io_threads = 0;       // 0 = 按 CPU 数取默认
  std::size_t storage_workers = 0;  // 0 = 按 CPU 数取默认
  std::uint64_t segment_bytes = 256u * 1024u * 1024u;
  std::uint64_t disk_threshold_bytes = 256u * 1024u * 1024u;  // Q-P11
  std::size_t queue_high_watermark = 4096;
  std::size_t queue_low_watermark = 1024;
  int queue_push_timeout_ms = 200;  // 队列满时投递等待上限，超时回 broker_unavailable
};

// M5 Broker 核心：启动编排（R7/R30）、Topic 管理（R14）、读写路径编排（R1/R2/R16/R23/R27）。
// 线程结构见 03 §4.3：
//   Main Reactor + N IO Reactor（M3）+ M Storage Worker（M4/M5）+ 本类的请求分发。
// 所有分区存储访问都发生在该分区所属 Storage Worker 线程内（AD-05/AD-13），
// 因此 PartitionStore/Segment 无需加锁；跨线程只传"请求 → 响应字节"。
class Broker {
 public:
  // 响应出口：网络路径写回连接；测试路径收进本地缓冲。
  using ResponseSink = std::function<void(std::string)>;

  explicit Broker(const BrokerConfig& config);
  ~Broker();

  Broker(const Broker&) = delete;
  Broker& operator=(const Broker&) = delete;

  // 恢复编排：扫描 meta 重建 Topic → 逐分区恢复 → 起 Storage Worker 线程 → 起网络。
  // 返回 ok 时已可对外服务（R7：恢复完成才 serving）。
  common::Status Start();

  // 在调用线程跑 Main Reactor，直到 Stop() 或 SIGINT/SIGTERM；返回前完成清理。
  common::Status Run();

  // 幂等优雅退出：停 accept/关连接 → 排空并停止 Storage Worker → join。
  void Stop();

  std::uint16_t port() const;
  bool running() const { return started_.load() && !stopped_.load(); }
  std::size_t topic_count() const;
  std::vector<std::string> warnings() const;

  // 测试/诊断：不经网络同步处理一次请求（走同一条分发与执行路径）。
  common::Status DispatchSync(const protocol::Frame& request,
                              protocol::Frame* response);

 private:
  class TaskQueue;
  struct WorkerContext;

  void OnFrame(net::ConnectionId id, const protocol::Frame& frame);
  void Dispatch(const protocol::Frame& frame, const ResponseSink& sink);

  void HandleCreateTopic(const protocol::Frame& frame,
                         const ResponseSink& sink);
  void HandleListTopics(const protocol::Frame& frame, const ResponseSink& sink);
  void HandleProduce(const protocol::Frame& frame, const ResponseSink& sink);
  void HandleFetch(const protocol::Frame& frame, const ResponseSink& sink);
  void HandleReplay(const protocol::Frame& frame, const ResponseSink& sink);
  void HandleGroupRequest(const protocol::Frame& frame,
                          const ResponseSink& sink);

  // 投递到 partition_key 所属 worker；队列满超时返回 false（不阻塞 IO 线程）。
  bool Submit(std::uint32_t partition_key, std::function<void()> task);
  void RunWorker(std::size_t index);
  void StopWorkers();

  static void Reply(const ResponseSink& sink, std::uint16_t msg_type,
                    std::uint64_t request_id, const std::string& payload);
  static void ReplyError(const ResponseSink& sink, std::uint16_t request_type,
                         std::uint64_t request_id, common::StatusCode code,
                         const std::string& message);

  BrokerConfig config_;
  std::unique_ptr<TopicRegistry> registry_;
  std::unique_ptr<PartitionManager> partitions_;
  std::unique_ptr<PartitionService> service_;
  std::unique_ptr<net::ReactorPool> reactors_;
  std::vector<std::unique_ptr<WorkerContext>> workers_;

  // 串行化 Topic 管理（创建/列表）；分区数不可变，故读路径不需要它。
  mutable std::mutex registry_mutex_;

  std::atomic<bool> started_;
  std::atomic<bool> stopping_;
  std::atomic<bool> stopped_;

  // Stop 可能被 Run 线程与外部线程同时触发，用这对标志保证只清理一次。
  std::mutex stop_mutex_;
  std::condition_variable stop_cv_;
  bool cleanup_started_;
  bool cleanup_done_;
};

}  // namespace broker
}  // namespace minikafka

#endif  // MINIKAFKA_BROKER_BROKER_H_
