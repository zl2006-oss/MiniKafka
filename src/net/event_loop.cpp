#include "minikafka/net/event_loop.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <utility>
#include <vector>

namespace minikafka {
namespace net {

EventLoop::EventLoop(std::size_t high_watermark,
                     std::size_t low_watermark)
    : epoll_fd_(::epoll_create1(EPOLL_CLOEXEC)),
      wakeup_fd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)),
      init_status_(),
      thread_id_mutex_(),
      loop_thread_id_(),
      quit_(false),
      stopped_(false),
      started_(false),
      high_watermark_(high_watermark),
      low_watermark_(low_watermark),
      mutex_(),
      space_cv_(),
      tasks_(),
      callbacks_() {
  if (high_watermark_ == 0) {
    high_watermark_ = 4096u;
  }
  if (low_watermark_ >= high_watermark_) {
    low_watermark_ = high_watermark_ / 2u;
  }

  if (!epoll_fd_.Valid()) {
    init_status_ =
        Status(StatusCode::kInternalError, "epoll_create1 failed");
    stopped_.store(true);
    return;
  }
  if (!wakeup_fd_.Valid()) {
    init_status_ =
        Status(StatusCode::kInternalError, "eventfd creation failed");
    stopped_.store(true);
    return;
  }

  init_status_ = RegisterWakeupFd();
  if (!init_status_.ok()) {
    stopped_.store(true);
  }
}

EventLoop::~EventLoop() {
  Quit();
  stopped_.store(true);
  space_cv_.notify_all();
}

Status EventLoop::Loop() {
  if (!init_status_.ok()) {
    stopped_.store(true);
    space_cv_.notify_all();
    return init_status_;
  }

  bool expected = false;
  if (!started_.compare_exchange_strong(expected, true)) {
    return Status(StatusCode::kInternalError, "event loop already started");
  }

  {
    std::lock_guard<std::mutex> lock(thread_id_mutex_);
    loop_thread_id_ = std::this_thread::get_id();
  }

  std::vector<epoll_event> events(64);
  while (!quit_.load()) {
    RunPendingTasks();
    if (quit_.load()) {
      break;
    }

    const int ready = ::epoll_wait(epoll_fd_.Get(), events.data(),
                                   static_cast<int>(events.size()), -1);
    if (ready < 0) {
      if (errno == EINTR) {
        continue;
      }
      stopped_.store(true);
      space_cv_.notify_all();
      return Status(StatusCode::kInternalError, "epoll_wait failed");
    }

    for (int i = 0; i < ready; ++i) {
      const epoll_event& event = events[static_cast<std::size_t>(i)];
      if (event.data.fd == wakeup_fd_.Get()) {
        DrainWakeup();
        continue;
      }
      const auto callback_it = callbacks_.find(event.data.fd);
      if (callback_it != callbacks_.end()) {
        IoCallback callback = callback_it->second;
        if (callback) {
          callback(event.events);
        }
      }
    }

    RunPendingTasks();
  }

  stopped_.store(true);
  space_cv_.notify_all();
  return Status();
}

void EventLoop::Quit() {
  quit_.store(true);
  space_cv_.notify_all();
  Wakeup();
}

bool EventLoop::RunInLoop(Task task) {
  if (!task) {
    return false;
  }
  if (quit_.load() || stopped_.load()) {
    return false;
  }
  if (IsInLoopThread()) {
    task();
    return true;
  }
  return QueueInLoop(std::move(task));
}

bool EventLoop::QueueInLoop(Task task) {
  if (!task) {
    return false;
  }
  if (quit_.load() || stopped_.load()) {
    return false;
  }

  std::unique_lock<std::mutex> lock(mutex_);
  while (tasks_.size() >= high_watermark_ && !quit_.load() &&
         !stopped_.load()) {
    space_cv_.wait(lock);
  }
  if (quit_.load() || stopped_.load()) {
    return false;
  }
  tasks_.push_back(std::move(task));
  lock.unlock();
  Wakeup();
  return true;
}

bool EventLoop::IsInLoopThread() const {
  std::lock_guard<std::mutex> lock(thread_id_mutex_);
  return std::this_thread::get_id() == loop_thread_id_;
}

Status EventLoop::AddFd(int fd, std::uint32_t events, IoCallback callback) {
  if (!init_status_.ok()) {
    return init_status_;
  }
  if (fd < 0 || !callback) {
    return Status(StatusCode::kInternalError, "invalid fd registration");
  }
  if (stopped_.load()) {
    return Status(StatusCode::kInternalError, "event loop stopped");
  }
  if (started_.load() && !IsInLoopThread()) {
    return Status(StatusCode::kInternalError,
                  "fd registration must run in loop thread");
  }

  const auto inserted = callbacks_.emplace(fd, std::move(callback));
  if (!inserted.second) {
    return Status(StatusCode::kInternalError, "fd already registered");
  }

  epoll_event event;
  std::memset(&event, 0, sizeof(event));
  event.events = events;
  event.data.fd = fd;
  if (::epoll_ctl(epoll_fd_.Get(), EPOLL_CTL_ADD, fd, &event) != 0) {
    callbacks_.erase(fd);
    return Status(StatusCode::kInternalError, "epoll_ctl(add) failed");
  }
  return Status();
}

Status EventLoop::ModifyFd(int fd, std::uint32_t events) {
  if (!init_status_.ok()) {
    return init_status_;
  }
  if (fd < 0) {
    return Status(StatusCode::kInternalError, "invalid fd");
  }
  if (stopped_.load()) {
    return Status(StatusCode::kInternalError, "event loop stopped");
  }
  if (started_.load() && !IsInLoopThread()) {
    return Status(StatusCode::kInternalError,
                  "fd modification must run in loop thread");
  }
  if (callbacks_.find(fd) == callbacks_.end()) {
    return Status(StatusCode::kInternalError, "fd is not registered");
  }

  epoll_event event;
  std::memset(&event, 0, sizeof(event));
  event.events = events;
  event.data.fd = fd;
  if (::epoll_ctl(epoll_fd_.Get(), EPOLL_CTL_MOD, fd, &event) != 0) {
    return Status(StatusCode::kInternalError, "epoll_ctl(mod) failed");
  }
  return Status();
}

Status EventLoop::RemoveFd(int fd) {
  if (!init_status_.ok()) {
    return init_status_;
  }
  if (fd < 0) {
    return Status(StatusCode::kInternalError, "invalid fd");
  }
  if (started_.load() && !stopped_.load() && !IsInLoopThread()) {
    return Status(StatusCode::kInternalError,
                  "fd removal must run in loop thread");
  }

  const auto it = callbacks_.find(fd);
  if (it == callbacks_.end()) {
    return Status(StatusCode::kInternalError, "fd is not registered");
  }
  if (::epoll_ctl(epoll_fd_.Get(), EPOLL_CTL_DEL, fd, nullptr) != 0 &&
      errno != ENOENT && errno != EBADF) {
    return Status(StatusCode::kInternalError, "epoll_ctl(del) failed");
  }
  callbacks_.erase(it);
  return Status();
}

void EventLoop::Wakeup() {
  if (!wakeup_fd_.Valid()) {
    return;
  }
  const std::uint64_t one = 1u;
  ssize_t result = -1;
  do {
    result = ::write(wakeup_fd_.Get(), &one, sizeof(one));
  } while (result < 0 && errno == EINTR);
}

void EventLoop::DrainWakeup() {
  if (!wakeup_fd_.Valid()) {
    return;
  }
  std::uint64_t value = 0;
  while (true) {
    const ssize_t result =
        ::read(wakeup_fd_.Get(), &value, sizeof(value));
    if (result == static_cast<ssize_t>(sizeof(value))) {
      continue;
    }
    if (result < 0 && errno == EINTR) {
      continue;
    }
    break;
  }
}

void EventLoop::RunPendingTasks() {
  std::deque<Task> batch;
  const std::size_t batch_limit =
      high_watermark_ > low_watermark_ ? high_watermark_ - low_watermark_ : 1u;

  while (true) {
    batch.clear();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const std::size_t take =
          tasks_.size() > batch_limit ? batch_limit : tasks_.size();
      for (std::size_t i = 0; i < take; ++i) {
        batch.push_back(std::move(tasks_.front()));
        tasks_.pop_front();
      }
      if (tasks_.size() <= low_watermark_) {
        space_cv_.notify_all();
      }
    }

    if (batch.empty()) {
      break;
    }
    for (Task& task : batch) {
      if (task) {
        task();
      }
    }
  }
}

Status EventLoop::RegisterWakeupFd() {
  epoll_event event;
  std::memset(&event, 0, sizeof(event));
  event.events = EPOLLIN;
  event.data.fd = wakeup_fd_.Get();
  if (::epoll_ctl(epoll_fd_.Get(), EPOLL_CTL_ADD, wakeup_fd_.Get(),
                  &event) != 0) {
    return Status(StatusCode::kInternalError,
                  "epoll_ctl(add wakeup) failed");
  }
  return Status();
}

}  // namespace net
}  // namespace minikafka
