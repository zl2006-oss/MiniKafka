#include "test_common.h"

#include <cstdint>

#include "minikafka/common/backoff.h"

void TestBackoff() {
  using minikafka::common::Backoff;

  Backoff backoff;
  MK_EXPECT_EQ(backoff.NextDelayMs(), static_cast<std::uint64_t>(100));
  MK_EXPECT_EQ(backoff.NextDelayMs(), static_cast<std::uint64_t>(200));
  MK_EXPECT_EQ(backoff.NextDelayMs(), static_cast<std::uint64_t>(400));
  MK_EXPECT_EQ(backoff.NextDelayMs(), static_cast<std::uint64_t>(800));
  MK_EXPECT_EQ(backoff.NextDelayMs(), static_cast<std::uint64_t>(1000));
  MK_EXPECT_EQ(backoff.NextDelayMs(), static_cast<std::uint64_t>(1000));
  MK_EXPECT_EQ(backoff.attempts(), static_cast<std::uint32_t>(6));

  backoff.Reset();
  MK_EXPECT_EQ(backoff.attempts(), static_cast<std::uint32_t>(0));
  MK_EXPECT_EQ(backoff.NextDelayMs(), static_cast<std::uint64_t>(100));

  Backoff custom(50, 300);
  MK_EXPECT_EQ(custom.NextDelayMs(), static_cast<std::uint64_t>(50));
  MK_EXPECT_EQ(custom.NextDelayMs(), static_cast<std::uint64_t>(100));
  MK_EXPECT_EQ(custom.NextDelayMs(), static_cast<std::uint64_t>(200));
  MK_EXPECT_EQ(custom.NextDelayMs(), static_cast<std::uint64_t>(300));
  MK_EXPECT_EQ(custom.NextDelayMs(), static_cast<std::uint64_t>(300));

  Backoff max_below_initial(100, 10);
  MK_EXPECT_EQ(max_below_initial.NextDelayMs(), static_cast<std::uint64_t>(100));
  MK_EXPECT_EQ(max_below_initial.NextDelayMs(), static_cast<std::uint64_t>(100));

  Backoff zero(0, 0);
  MK_EXPECT_EQ(zero.NextDelayMs(), static_cast<std::uint64_t>(0));
  MK_EXPECT_EQ(zero.NextDelayMs(), static_cast<std::uint64_t>(0));

  Backoff capped(1000, 1000);
  MK_EXPECT_EQ(capped.NextDelayMs(), static_cast<std::uint64_t>(1000));
  MK_EXPECT_EQ(capped.NextDelayMs(), static_cast<std::uint64_t>(1000));
}
