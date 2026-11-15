#include "test_common.h"

#include <atomic>
#include <chrono>
#include <thread>

#include "minikafka/net/event_loop.h"

namespace {

bool WaitFor(const std::atomic<bool>& flag) {
  for (int i = 0; i < 2000; ++i) {
    if (flag.load()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return flag.load();
}

}  // namespace

void TestEventLoop() {
  using minikafka::common::Status;
  using minikafka::net::EventLoop;

  {
    EventLoop loop;
    std::atomic<bool> loop_running(false);
    std::atomic<bool> task_ran(false);
    Status loop_status;
    std::thread loop_thread([&loop, &loop_status] { loop_status = loop.Loop(); });

    const bool first_queued =
        loop.QueueInLoop([&loop_running] { loop_running.store(true); });
    MK_EXPECT_TRUE(first_queued);
    MK_EXPECT_TRUE(WaitFor(loop_running));

    const bool second_queued = loop.QueueInLoop(
        [&task_ran, &loop] {
          task_ran.store(true);
          loop.Quit();
        });
    MK_EXPECT_TRUE(second_queued);

    loop_thread.join();
    MK_EXPECT_TRUE(task_ran.load());
    MK_EXPECT_TRUE(loop_status.ok());
  }

  {
    EventLoop loop;
    std::atomic<bool> inner(false);
    std::atomic<bool> outer(false);
    Status loop_status;
    std::thread loop_thread([&loop, &loop_status] { loop_status = loop.Loop(); });

    const bool queued = loop.QueueInLoop([&inner, &outer, &loop] {
      const bool ran = loop.RunInLoop([&inner] { inner.store(true); });
      outer.store(ran && inner.load());
      loop.Quit();
    });
    MK_EXPECT_TRUE(queued);

    loop_thread.join();
    MK_EXPECT_TRUE(inner.load());
    MK_EXPECT_TRUE(outer.load());
    MK_EXPECT_TRUE(loop_status.ok());
  }

  {
    EventLoop loop;
    loop.Quit();
    MK_EXPECT_FALSE(loop.QueueInLoop([] {}));
    MK_EXPECT_FALSE(loop.RunInLoop([] {}));
  }
}
