#include "minikafka/net/reactor_pool.h"

#include <pthread.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>

namespace minikafka {
namespace net {
namespace {

struct SendState {
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  bool cancelled = false;
  Status status;
};

}  // namespace

class ReactorPool::Reactor {
 public:
  Reactor(ReactorPool::FrameCallback frame_callback,
          ReactorPool::ConnectionCallback connection_callback,
          std::size_t write_high_watermark)
      : frame_callback_(std::move(frame_callback)),
        connection_callback_(std::move(connection_callback)),
        write_high_watermark_(write_high_watermark),
        loop_(),
        thread_(),
        connections_(),
        pending_close_(),
        drain_scheduled_(false),
        connection_count_(0) {}

  ~Reactor() { StopAndJoin(); }

  void Start() {
    thread_ = std::thread([this] { (void)loop_.Loop(); });
  }

  bool AddConnection(ConnectionId id, Socket socket,
                     const sockaddr_in& peer) {
    std::shared_ptr<Socket> holder =
        std::make_shared<Socket>(std::move(socket));
    return loop_.QueueInLoop(
        [this, id, holder, peer] { HandleAddConnection(id, holder, peer); });
  }

  Status PostSend(ConnectionId id, std::string data) {
    if (loop_.IsInLoopThread()) {
      const auto it = connections_.find(id);
      if (it == connections_.end()) {
        return Status(StatusCode::kInternalError, "connection not found");
      }
      return it->second->Send(data);
    }

    std::shared_ptr<SendState> state = std::make_shared<SendState>();
    std::shared_ptr<std::string> payload =
        std::make_shared<std::string>(std::move(data));
    const bool queued = loop_.QueueInLoop([this, id, payload, state] {
      {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->cancelled) {
          state->done = true;
          state->cv.notify_all();
          return;
        }
      }

      Status status;
      const auto it = connections_.find(id);
      if (it == connections_.end()) {
        status = Status(StatusCode::kInternalError, "connection not found");
      } else {
        status = it->second->Send(*payload);
      }

      {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->status = status;
        state->done = true;
      }
      state->cv.notify_all();
    });
    if (!queued) {
      return Status(StatusCode::kInternalError,
                    "connection loop unavailable");
    }

    std::unique_lock<std::mutex> lock(state->mutex);
    if (!state->cv.wait_for(lock, std::chrono::seconds(5),
                            [&state] { return state->done; })) {
      state->cancelled = true;
      return Status(StatusCode::kInternalError, "send timed out");
    }
    return state->status;
  }

  void StopAndJoin() {
    if (thread_.joinable()) {
      (void)loop_.QueueInLoop([this] { CloseAllAndQuit(); });
      thread_.join();
    }
    connections_.clear();
    pending_close_.clear();
    drain_scheduled_ = false;
    connection_count_.store(0);
  }

  bool IsInLoopThread() const { return loop_.IsInLoopThread(); }

  std::size_t connection_count() const { return connection_count_.load(); }

 private:
  void HandleAddConnection(ConnectionId id,
                           const std::shared_ptr<Socket>& holder,
                           const sockaddr_in& peer) {
    if (connections_.find(id) != connections_.end()) {
      return;
    }

    Socket socket = std::move(*holder);
    std::unique_ptr<Connection> connection(
        new Connection(&loop_, id, std::move(socket), peer,
                       write_high_watermark_));
    connection->set_frame_callback(frame_callback_);
    connection->set_close_callback(
        [this](ConnectionId closed_id) { OnConnectionClosed(closed_id); });

    const Status status = connection->Start();
    if (!status.ok()) {
      return;
    }

    const auto inserted = connections_.emplace(id, std::move(connection));
    if (!inserted.second) {
      return;
    }
    connection_count_.fetch_add(1);

    if (connection_callback_) {
      connection_callback_(id, peer);
    }
  }

  void OnConnectionClosed(ConnectionId id) {
    const auto it = connections_.find(id);
    if (it == connections_.end()) {
      return;
    }
    pending_close_.push_back(std::move(it->second));
    connections_.erase(it);
    const std::size_t count = connection_count_.load();
    if (count > 0) {
      connection_count_.fetch_sub(1);
    }

    if (!drain_scheduled_) {
      drain_scheduled_ = true;
      (void)loop_.QueueInLoop([this] { DrainPendingClose(); });
    }
  }

  void DrainPendingClose() {
    drain_scheduled_ = false;
    pending_close_.clear();
  }

  void CloseAllAndQuit() {
    std::vector<ConnectionId> ids;
    ids.reserve(connections_.size());
    for (const auto& entry : connections_) {
      ids.push_back(entry.first);
    }
    for (ConnectionId id : ids) {
      const auto it = connections_.find(id);
      if (it != connections_.end()) {
        it->second->Close();
      }
    }
    pending_close_.clear();
    drain_scheduled_ = false;
    connection_count_.store(0);
    loop_.Quit();
  }

  ReactorPool::FrameCallback frame_callback_;
  ReactorPool::ConnectionCallback connection_callback_;
  std::size_t write_high_watermark_;
  EventLoop loop_;
  std::thread thread_;
  std::unordered_map<ConnectionId, std::unique_ptr<Connection>> connections_;
  std::vector<std::unique_ptr<Connection>> pending_close_;
  bool drain_scheduled_;
  std::atomic<std::size_t> connection_count_;
};

ReactorPool::ReactorPool(std::size_t io_threads,
                         std::size_t write_high_watermark)
    : io_threads_(io_threads == 0 ? DefaultIoThreads() : io_threads),
      write_high_watermark_(
          write_high_watermark == 0 ? kDefaultConnectionWriteHighWatermark
                                    : write_high_watermark),
      main_loop_(),
      acceptor_(),
      reactors_(),
      frame_callback_(),
      connection_callback_(),
      started_(false),
      stopping_(false),
      stopped_(false),
      main_running_(false),
      stop_mutex_(),
      stop_cv_(),
      next_reactor_(0),
      next_sequence_(0),
      port_(0),
      signal_fd_(),
      signal_set_(),
      old_signal_mask_(),
      signal_mask_saved_(false) {
  if (io_threads_ == 0) {
    io_threads_ = 1;
  }
  sigemptyset(&signal_set_);
  sigemptyset(&old_signal_mask_);
}

ReactorPool::~ReactorPool() {
  Stop();
  acceptor_.reset();
  reactors_.clear();

  if (signal_fd_.Valid()) {
    (void)main_loop_.RemoveFd(signal_fd_.Get());
    signal_fd_.Reset();
  }
  if (signal_mask_saved_) {
    (void)pthread_sigmask(SIG_SETMASK, &old_signal_mask_, nullptr);
    signal_mask_saved_ = false;
  }
}

std::size_t ReactorPool::DefaultIoThreads() {
  unsigned cores = std::thread::hardware_concurrency();
  if (cores == 0) {
    cores = 1;
  }
  std::size_t count = static_cast<std::size_t>(cores) / 2u;
  if (count < 1u) {
    count = 1u;
  }
  if (count > 4u) {
    count = 4u;
  }
  return count;
}

Status ReactorPool::Start(const sockaddr_in& listen_address,
                          FrameCallback frame_callback,
                          ConnectionCallback connection_callback) {
  bool expected = false;
  if (!started_.compare_exchange_strong(expected, true)) {
    return Status(StatusCode::kInternalError, "reactor pool already started");
  }
  if (io_threads_ == 0u) {
    started_.store(false);
    return Status(StatusCode::kInternalError, "io_threads must be positive");
  }

  frame_callback_ = std::move(frame_callback);
  connection_callback_ = std::move(connection_callback);

  sigemptyset(&signal_set_);
  sigaddset(&signal_set_, SIGINT);
  sigaddset(&signal_set_, SIGTERM);
  if (pthread_sigmask(SIG_BLOCK, &signal_set_, &old_signal_mask_) == 0) {
    signal_mask_saved_ = true;
  }

  acceptor_.reset(new Acceptor(&main_loop_, listen_address));
  Status status = acceptor_->Start(
      [this](Socket&& socket, const sockaddr_in& peer) {
        DispatchConnection(std::move(socket), peer);
      });
  if (!status.ok()) {
    acceptor_.reset();
    if (signal_mask_saved_) {
      (void)pthread_sigmask(SIG_SETMASK, &old_signal_mask_, nullptr);
      signal_mask_saved_ = false;
    }
    started_.store(false);
    return status;
  }

  reactors_.reserve(io_threads_);
  for (std::size_t i = 0; i < io_threads_; ++i) {
    reactors_.push_back(std::unique_ptr<Reactor>(
        new Reactor(frame_callback_, connection_callback_,
                    write_high_watermark_)));
  }
  for (std::unique_ptr<Reactor>& reactor : reactors_) {
    reactor->Start();
  }

  port_ = acceptor_->port();
  return Status();
}

Status ReactorPool::Run() {
  if (!started_.load()) {
    return Status(StatusCode::kInternalError, "reactor pool not started");
  }
  if (stopping_.load() || stopped_.load()) {
    return Status(StatusCode::kInternalError, "reactor pool stopping");
  }

  bool expected = false;
  if (!main_running_.compare_exchange_strong(expected, true)) {
    return Status(StatusCode::kInternalError,
                  "main reactor already running");
  }

  if (signal_mask_saved_) {
    (void)pthread_sigmask(SIG_BLOCK, &signal_set_, nullptr);
    signal_fd_ =
        UniqueFd(::signalfd(-1, &signal_set_, SFD_NONBLOCK | SFD_CLOEXEC));
    if (signal_fd_.Valid()) {
      const Status status = main_loop_.AddFd(
          signal_fd_.Get(), static_cast<std::uint32_t>(EPOLLIN),
          [this](std::uint32_t events) {
            (void)events;
            HandleSignal();
          });
      if (!status.ok()) {
        signal_fd_.Reset();
      }
    }
  }

  const Status status = main_loop_.Loop();

  if (signal_fd_.Valid()) {
    (void)main_loop_.RemoveFd(signal_fd_.Get());
    signal_fd_.Reset();
  }
  if (signal_mask_saved_) {
    (void)pthread_sigmask(SIG_SETMASK, &old_signal_mask_, nullptr);
  }
  main_running_.store(false);
  return status;
}

void ReactorPool::Stop() {
  if (!started_.load()) {
    return;
  }

  const bool first = !stopping_.exchange(true);
  if (!first) {
    if (main_loop_.IsInLoopThread()) {
      return;
    }
    for (const std::unique_ptr<Reactor>& reactor : reactors_) {
      if (reactor->IsInLoopThread()) {
        return;
      }
    }
    WaitForStopped();
    return;
  }

  const bool in_main = main_loop_.IsInLoopThread();
  bool in_io = false;
  for (const std::unique_ptr<Reactor>& reactor : reactors_) {
    if (reactor->IsInLoopThread()) {
      in_io = true;
      break;
    }
  }

  if (in_main) {
    DoStop();
    return;
  }
  if (in_io) {
    (void)main_loop_.QueueInLoop([this] { DoStop(); });
    return;
  }
  if (!main_running_.load()) {
    DoStop();
    return;
  }
  if (!main_loop_.QueueInLoop([this] { DoStop(); })) {
    DoStop();
    return;
  }
  WaitForStopped();
}

void ReactorPool::DispatchConnection(Socket&& socket,
                                     const sockaddr_in& peer) {
  if (reactors_.empty()) {
    return;
  }
  const std::size_t index = next_reactor_ % reactors_.size();
  ++next_reactor_;
  const std::uint64_t sequence = ++next_sequence_;
  const ConnectionId id =
      (static_cast<std::uint64_t>(index) << 32) |
      (sequence & 0xFFFFFFFFu);
  (void)reactors_[index]->AddConnection(id, std::move(socket), peer);
}

std::uint16_t ReactorPool::port() const { return port_; }

Status ReactorPool::Send(ConnectionId connection_id, std::string data) {
  const std::size_t index =
      static_cast<std::size_t>(connection_id >> 32);
  if (index >= reactors_.size()) {
    return Status(StatusCode::kInternalError, "invalid connection id");
  }
  return reactors_[index]->PostSend(connection_id, std::move(data));
}

std::size_t ReactorPool::connection_count() const {
  std::size_t total = 0;
  for (const std::unique_ptr<Reactor>& reactor : reactors_) {
    total += reactor->connection_count();
  }
  return total;
}

void ReactorPool::DoStop() {
  if (acceptor_) {
    acceptor_->Close();
  }
  for (std::unique_ptr<Reactor>& reactor : reactors_) {
    reactor->StopAndJoin();
  }
  main_loop_.Quit();
  stopped_.store(true);
  stop_cv_.notify_all();
}

void ReactorPool::HandleSignal() {
  if (!signal_fd_.Valid()) {
    return;
  }

  while (true) {
    signalfd_siginfo info;
    const ssize_t count =
        ::read(signal_fd_.Get(), &info, sizeof(info));
    if (count == static_cast<ssize_t>(sizeof(info))) {
      if (info.ssi_signo == static_cast<std::uint32_t>(SIGINT) ||
          info.ssi_signo == static_cast<std::uint32_t>(SIGTERM)) {
        Stop();
      }
      continue;
    }
    if (count < 0 && errno == EINTR) {
      continue;
    }
    break;
  }
}

void ReactorPool::WaitForStopped() {
  std::unique_lock<std::mutex> lock(stop_mutex_);
  stop_cv_.wait(lock, [this] { return stopped_.load(); });
}

}  // namespace net
}  // namespace minikafka
