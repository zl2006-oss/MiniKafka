#include "test_common.h"

#include <initializer_list>
#include <string>
#include <vector>

#include "minikafka/common/args.h"
#include "minikafka/common/defaults.h"

namespace {

using minikafka::common::Args;
using minikafka::common::Status;
using minikafka::common::StatusCode;

std::vector<std::string> Tokens(std::initializer_list<const char*> values) {
  std::vector<std::string> tokens;
  for (const char* value : values) {
    tokens.emplace_back(value);
  }
  return tokens;
}

void ExpectUsage(const Status& status) {
  MK_EXPECT_FALSE(status.ok());
  MK_EXPECT_EQ(status.code(), StatusCode::kUsage);
}

}  // namespace

void TestArgs() {
  Args args;
  args.AddString("topic", "orders", true);
  args.AddInt("partitions", 4);
  args.AddBool("exit-at-end", false);

  Status status = args.Parse(Tokens({"--topic", "events"}));
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_EQ(args.GetString("topic"), std::string("events"));
  MK_EXPECT_EQ(args.GetInt("partitions"), 4);
  MK_EXPECT_FALSE(args.GetBool("exit-at-end"));
  MK_EXPECT_TRUE(args.Has("topic"));
  MK_EXPECT_FALSE(args.Has("partitions"));
  MK_EXPECT_FALSE(args.Has("missing"));

  status = args.Parse(Tokens({"--topic", "x", "--partitions", "8",
                              "--exit-at-end"}));
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_EQ(args.GetInt("partitions"), 8);
  MK_EXPECT_TRUE(args.GetBool("exit-at-end"));

  Args required;
  required.AddString("topic", "", true);
  ExpectUsage(required.Parse(Tokens({})));

  Args unknown;
  unknown.AddString("topic", "", false);
  ExpectUsage(unknown.Parse(Tokens({"--nope"})));

  Args missing_value;
  missing_value.AddString("topic", "", false);
  ExpectUsage(missing_value.Parse(Tokens({"--topic"})));

  Args bool_with_value;
  bool_with_value.AddBool("exit-at-end", false);
  ExpectUsage(bool_with_value.Parse(Tokens({"--exit-at-end", "true"})));

  Args duplicate;
  duplicate.AddString("topic", "", false);
  ExpectUsage(
      duplicate.Parse(Tokens({"--topic", "a", "--topic", "b"})));

  Args ints;
  ints.AddInt("partitions", 1);
  ExpectUsage(ints.Parse(Tokens({"--partitions", "12x"})));
  ExpectUsage(ints.Parse(Tokens({"--partitions", ""})));
  ExpectUsage(ints.Parse(Tokens({"--partitions", "+1"})));
  ExpectUsage(ints.Parse(Tokens({"--partitions", "99999999999999999999"})));

  Status int_status = ints.Parse(Tokens({"--partitions", "-1"}));
  MK_EXPECT_TRUE(int_status.ok());
  MK_EXPECT_EQ(ints.GetInt("partitions"), -1);

  int_status = ints.Parse(Tokens({"--partitions", "-2147483648"}));
  MK_EXPECT_TRUE(int_status.ok());
  MK_EXPECT_EQ(ints.GetInt("partitions"), -2147483647 - 1);

  Args message;
  message.AddString("message", "", false);
  status = message.Parse(Tokens({"--message", "--hello"}));
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_EQ(message.GetString("message"), std::string("--hello"));

  Args empty_value;
  empty_value.AddString("topic", "default", false);
  status = empty_value.Parse(Tokens({"--topic", ""}));
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_EQ(empty_value.GetString("topic"), std::string());

  const std::string long_value(100000u, 'x');
  Args long_args;
  long_args.AddString("topic", "", false);
  std::vector<std::string> long_tokens;
  long_tokens.push_back("--topic");
  long_tokens.push_back(long_value);
  status = long_args.Parse(long_tokens);
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_EQ(long_args.GetString("topic").size(), long_value.size());

  const char* argv[] = {"minikafka", "produce", "--topic", "x"};
  Args argv_args;
  argv_args.AddString("topic", "", true);
  status = argv_args.Parse(4, argv, 2);
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_EQ(argv_args.GetString("topic"), std::string("x"));
  ExpectUsage(argv_args.Parse(4, argv, 5));

  Args kebab;
  kebab.AddString("data-dir", "./data", false);
  status = kebab.Parse(Tokens({"--data-dir", "/tmp/data"}));
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_EQ(kebab.GetString("data-dir"), std::string("/tmp/data"));

  Args equals;
  equals.AddString("topic", "", false);
  ExpectUsage(equals.Parse(Tokens({"--topic=orders"})));

  Args end_marker;
  end_marker.AddString("topic", "", false);
  ExpectUsage(end_marker.Parse(Tokens({"--"})));

  Args default_true;
  default_true.AddBool("flag", true);
  status = default_true.Parse(Tokens({}));
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_TRUE(default_true.GetBool("flag"));
  status = default_true.Parse(Tokens({"--flag"}));
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_TRUE(default_true.GetBool("flag"));

  MK_EXPECT_EQ(minikafka::common::kDefaultBrokerPort, 9092);
  MK_EXPECT_EQ(std::string(minikafka::common::kDefaultDataDir),
               std::string("./data"));
  MK_EXPECT_EQ(minikafka::common::kDefaultMinFreeSpaceMb, 256);
  MK_EXPECT_EQ(minikafka::common::kDefaultSegmentBytes, 268435456LL);
  MK_EXPECT_EQ(minikafka::common::kMinSegmentBytes, 2097152LL);
  MK_EXPECT_EQ(minikafka::common::kDefaultRetries, 3);
  MK_EXPECT_EQ(minikafka::common::kDefaultRetryBackoffMs, 100);
  MK_EXPECT_EQ(minikafka::common::kDefaultRetryTimeoutMs, 5000);
  MK_EXPECT_EQ(std::string(minikafka::common::kDefaultFrom),
               std::string("earliest"));
  MK_EXPECT_EQ(minikafka::common::kDefaultAutoCommitIntervalMs, 5000);
  MK_EXPECT_EQ(minikafka::common::kDefaultReplayPartition, 0);
  MK_EXPECT_EQ(minikafka::common::kDefaultReplayOffset, 0LL);
  MK_EXPECT_EQ(std::string(minikafka::common::kDefaultBrokerAddress),
               std::string("127.0.0.1:9092"));
  MK_EXPECT_EQ(std::string(minikafka::common::kDefaultBenchMode),
               std::string("throughput"));
  MK_EXPECT_EQ(minikafka::common::kDefaultProducers, 4);
  MK_EXPECT_EQ(minikafka::common::kDefaultPayloadBytes, 100);
  MK_EXPECT_EQ(minikafka::common::kDefaultKeyBytes, 8);
  MK_EXPECT_EQ(minikafka::common::kMinPartitions, 1);
  MK_EXPECT_EQ(minikafka::common::kMaxPartitions, 16);
}
