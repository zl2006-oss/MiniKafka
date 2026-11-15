#include "test_common.h"

#include <chrono>
#include <cstdint>

#include "minikafka/common/clock.h"

void TestClock() {
  using minikafka::common::Clock;

  const Clock::TimePoint start = Clock::Now();
  const std::uint64_t first = Clock::NowMillis();
  const std::uint64_t second = Clock::NowMillis();
  MK_EXPECT_TRUE(second >= first);

  MK_EXPECT_EQ(Clock::ElapsedMillis(start, start),
               static_cast<std::uint64_t>(0));
  MK_EXPECT_EQ(
      Clock::ElapsedMillis(start, start + std::chrono::milliseconds(5)),
      static_cast<std::uint64_t>(5));
  MK_EXPECT_EQ(
      Clock::ElapsedMillis(start, start - std::chrono::milliseconds(5)),
      static_cast<std::uint64_t>(0));
  MK_EXPECT_EQ(
      Clock::ElapsedMillis(start, start + std::chrono::milliseconds(7)),
      static_cast<std::uint64_t>(7));

  (void)Clock::ElapsedMillis(start);
}
