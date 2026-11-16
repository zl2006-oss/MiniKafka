#include "test_common.h"

#include <string>
#include <vector>

#include "minikafka/bench/stats.h"

namespace {

using minikafka::bench::Fixed;
using minikafka::bench::Percentile;

void TestPercentile() {
  const std::vector<double> empty;
  MK_EXPECT_EQ(Percentile(empty, 0.99), 0.0);

  std::vector<double> one(1u, 7.0);
  MK_EXPECT_EQ(Percentile(one, 0.50), 7.0);
  MK_EXPECT_EQ(Percentile(one, 0.99), 7.0);

  // 0..99：p50 落在 50，p99 落在 98（按最近秩取整）。
  std::vector<double> hundred;
  for (int i = 0; i < 100; ++i) {
    hundred.push_back(static_cast<double>(i));
  }
  MK_EXPECT_EQ(Percentile(hundred, 0.50), 50.0);
  MK_EXPECT_EQ(Percentile(hundred, 0.99), 98.0);
  MK_EXPECT_EQ(Percentile(hundred, 0.0), 0.0);
  MK_EXPECT_EQ(Percentile(hundred, 1.0), 99.0);
}

void TestFixed() {
  MK_EXPECT_EQ(Fixed(0.0), std::string("0.000"));
  MK_EXPECT_EQ(Fixed(12.3456), std::string("12.346"));
  MK_EXPECT_EQ(Fixed(1000000.0), std::string("1000000.000"));
}

}  // namespace

void TestBenchStats() {
  TestPercentile();
  TestFixed();
}
