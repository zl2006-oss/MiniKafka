#ifndef MINIKAFKA_NET_EVENT_LOOP_H_
#define MINIKAFKA_NET_EVENT_LOOP_H_

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>

#include "minikafka/common/status.h"
#include "minikafka/net/socket.h"

namespace minikafka {
namespace net {

using common::Status;
using common::StatusCode;

// epoll LT + 非阻塞 fd + eventfd 唤醒 + 有界跨线程任务队列。
// fd 注册/修改必须在 Loop 启动前或 Loop 线程内完成；连接对象由 Loop 线程独占。
// 析构前必须保证 Loop() 已返回且运行线程已 join。
class EventLoop {
 public:
  using Task = std::function<void()>;
  using IoCallback = std::function<void(std::uint32_t events)>;

  explicit EventLoop(std::size_t high_watermark = 4096u,
                     std::size_t low_watermark = 1024u);
  ~EventLoop();

  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  // 运行直到 Quit()；返回初始化或 epoll 错误。
  Status Loop();
  void Quit();

  // RunInLoop 在 Loop 线程内立即执行，否则转 QueueInLoop。
  bool RunInLoop(Task task);
  // 队列达到高水位时阻塞调用线程；Loop 线程内请使用 RunInLoop。
  bool QueueInLoop(Task task);
  bool IsInLoopThread() const;

  Status AddFd(int fd, std::uint32_t events, IoCallback callback);
  Status ModifyFd(int fd, std::uint32_t events);
  Status RemoveFd(int fd);

 private:
  void Wakeup();
  void DrainWakeup();
  void RunPendingTasks();
  Status RegisterWakeupFd();

  UniqueFd epoll_fd_;
  UniqueFd wakeup_fd_;
  Status init_status_;

  mutable std::mutex thread_id_mutex_;
  std::thread::id loop_thread_id_;

  std::atomic<bool> quit_;
  std::atomic<bool> stopped_;
  std::atomic<bool> started_;

  std::size_t high_watermark_;
  std::size_t low_watermark_;

  std::mutex mutex_;
  std::condition_variable space_cv_;
  std::deque<Task> tasks_;
  std::unordered_map<int, IoCallback> callbacks_;
};

}  // namespace net
}  // namespace minikafka

#endif  // MINIKAFKA_NET_EVENT_LOOP_H_
