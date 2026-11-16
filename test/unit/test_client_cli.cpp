#include "test_common.h"

#include <cstdint>
#include <string>

#include "minikafka/client/cli.h"
#include "minikafka/client/input.h"
#include "minikafka/common/status.h"
#include "minikafka/storage/record.h"

namespace {

using minikafka::client::Address;
using minikafka::client::ExitCodeFor;
using minikafka::client::FromMode;
using minikafka::client::FromSpec;
using minikafka::client::IsRetryable;
using minikafka::client::kMaxRecordBytes;
using minikafka::client::Number;
using minikafka::client::ParseAddress;
using minikafka::client::ParseFrom;
using minikafka::client::ParseIntervalSeconds;
using minikafka::client::ParseMessageLine;
using minikafka::client::ParseU64;
using minikafka::client::Quote;
using minikafka::client::RawMessage;
using minikafka::client::RecordLine;
using minikafka::client::RecordTooLarge;
using minikafka::common::Status;
using minikafka::common::StatusCode;

void TestParseAddress() {
  Address address;
  MK_EXPECT_TRUE(ParseAddress("127.0.0.1:19092", &address).ok());
  MK_EXPECT_EQ(address.host, std::string("127.0.0.1"));
  MK_EXPECT_EQ(address.port, 19092);

  // 缺省端口按 03 §3.4 固定为 9092。
  MK_EXPECT_TRUE(ParseAddress("broker.local", &address).ok());
  MK_EXPECT_EQ(address.host, std::string("broker.local"));
  MK_EXPECT_EQ(address.port, 9092);

  MK_EXPECT_FALSE(ParseAddress("", &address).ok());
  MK_EXPECT_FALSE(ParseAddress("host:0", &address).ok());
  MK_EXPECT_FALSE(ParseAddress("host:65536", &address).ok());
  MK_EXPECT_FALSE(ParseAddress("host:abc", &address).ok());
  MK_EXPECT_FALSE(ParseAddress(":9092", &address).ok());
}

void TestParseFrom() {
  FromSpec spec;
  MK_EXPECT_TRUE(ParseFrom("earliest", &spec).ok());
  MK_EXPECT_TRUE(spec.mode == FromMode::kEarliest);

  MK_EXPECT_TRUE(ParseFrom("latest", &spec).ok());
  MK_EXPECT_TRUE(spec.mode == FromMode::kLatest);

  MK_EXPECT_TRUE(ParseFrom("42", &spec).ok());
  MK_EXPECT_TRUE(spec.mode == FromMode::kOffset);
  MK_EXPECT_EQ(spec.offset, static_cast<std::uint64_t>(42));

  // 05 §3.4：非 earliest/latest/非负整数 → invalid_from。
  const Status bad = ParseFrom("-1", &spec);
  MK_EXPECT_FALSE(bad.ok());
  MK_EXPECT_TRUE(bad.code() == StatusCode::kInvalidFrom);
  MK_EXPECT_TRUE(ParseFrom("", &spec).code() == StatusCode::kInvalidFrom);
}

void TestParseInterval() {
  int seconds = 0;
  MK_EXPECT_TRUE(ParseIntervalSeconds("5s", &seconds).ok());
  MK_EXPECT_EQ(seconds, 5);
  MK_EXPECT_TRUE(ParseIntervalSeconds("30", &seconds).ok());
  MK_EXPECT_EQ(seconds, 30);

  // 非正数与非法字符都是 invalid_interval（退出码 2）。
  MK_EXPECT_TRUE(ParseIntervalSeconds("0", &seconds).code() ==
                 StatusCode::kInvalidInterval);
  MK_EXPECT_TRUE(ParseIntervalSeconds("0s", &seconds).code() ==
                 StatusCode::kInvalidInterval);
  MK_EXPECT_TRUE(ParseIntervalSeconds("abc", &seconds).code() ==
                 StatusCode::kInvalidInterval);
  MK_EXPECT_TRUE(ParseIntervalSeconds("86401", &seconds).code() ==
                 StatusCode::kInvalidInterval);
}

void TestParseU64() {
  std::uint64_t value = 0;
  MK_EXPECT_TRUE(ParseU64("0", &value));
  MK_EXPECT_EQ(value, static_cast<std::uint64_t>(0));
  MK_EXPECT_TRUE(ParseU64("18446744073709551615", &value));
  MK_EXPECT_EQ(value, ~static_cast<std::uint64_t>(0));
  MK_EXPECT_FALSE(ParseU64("18446744073709551616", &value));
  MK_EXPECT_FALSE(ParseU64("", &value));
  MK_EXPECT_FALSE(ParseU64("12a", &value));
}

void TestQuoteAndRecordLine() {
  MK_EXPECT_EQ(Quote("plain"), std::string("\"plain\""));
  MK_EXPECT_EQ(Quote("a\"b"), std::string("\"a\\\"b\""));
  MK_EXPECT_EQ(Quote("a\\b"), std::string("\"a\\\\b\""));
  MK_EXPECT_EQ(Quote("a\nb"), std::string("\"a\\nb\""));
  MK_EXPECT_EQ(Quote("a\tb"), std::string("\"a\\tb\""));
  MK_EXPECT_EQ(Quote(std::string(1, '\x01')), std::string("\"\\u0001\""));
  MK_EXPECT_EQ(Number(0), std::string("0"));
  MK_EXPECT_EQ(Number(1234567890), std::string("1234567890"));

  // R146：至少包含 partition / offset / key / value。
  MK_EXPECT_EQ(
      RecordLine("t", 1, 7, true, "k", "v"),
      std::string(
          "{\"topic\":\"t\",\"partition\":1,\"offset\":7,\"key\":\"k\","
          "\"value\":\"v\"}"));
  MK_EXPECT_EQ(
      RecordLine("t", 0, 0, false, std::string(), "v"),
      std::string(
          "{\"topic\":\"t\",\"partition\":0,\"offset\":0,\"key\":null,"
          "\"value\":\"v\"}"));
}

void TestExitCodeFor() {
  MK_EXPECT_EQ(ExitCodeFor(Status()), 0);
  MK_EXPECT_EQ(ExitCodeFor(Status(StatusCode::kUsage, "x")), 2);
  MK_EXPECT_EQ(ExitCodeFor(Status(StatusCode::kInvalidInterval, "x")), 2);
  MK_EXPECT_EQ(ExitCodeFor(Status(StatusCode::kInvalidFrom, "x")), 2);
  MK_EXPECT_EQ(ExitCodeFor(Status(StatusCode::kTopicNotFound, "x")), 3);
  MK_EXPECT_EQ(ExitCodeFor(Status(StatusCode::kInvalidPartition, "x")), 3);
}

void TestParseMessageLine() {
  RawMessage message;
  MK_EXPECT_TRUE(ParseMessageLine("{\"value\":\"v\"}", 3, &message).ok());
  MK_EXPECT_EQ(message.index, static_cast<std::size_t>(3));
  MK_EXPECT_EQ(message.value, std::string("v"));
  MK_EXPECT_TRUE((message.flags & minikafka::storage::kFlagKeyPresent) == 0u);

  MK_EXPECT_TRUE(
      ParseMessageLine("{\"key\":\"k\",\"value\":\"v\"}", 0, &message).ok());
  MK_EXPECT_TRUE((message.flags & minikafka::storage::kFlagKeyPresent) != 0u);
  MK_EXPECT_EQ(message.key, std::string("k"));

  // key 显式 null 与缺省等价。
  MK_EXPECT_TRUE(
      ParseMessageLine("{\"key\":null,\"value\":\"v\"}", 0, &message).ok());
  MK_EXPECT_TRUE((message.flags & minikafka::storage::kFlagKeyPresent) == 0u);

  // value 必填；key 必须是字符串。
  MK_EXPECT_FALSE(ParseMessageLine("{\"key\":\"k\"}", 0, &message).ok());
  MK_EXPECT_TRUE(ParseMessageLine("{\"key\":1,\"value\":\"v\"}", 0, &message)
                     .code() == StatusCode::kInvalidValueType);
  MK_EXPECT_FALSE(ParseMessageLine("not json", 0, &message).ok());
}

void TestRecordTooLarge() {
  RawMessage message;
  message.value.assign(kMaxRecordBytes - 24u, 'x');
  MK_EXPECT_FALSE(RecordTooLarge(message));

  message.value.push_back('x');
  MK_EXPECT_TRUE(RecordTooLarge(message));

  RawMessage keyed;
  keyed.key.assign(24u, 'k');
  keyed.value.assign(kMaxRecordBytes - 24u - 24u, 'x');
  MK_EXPECT_FALSE(RecordTooLarge(keyed));
  keyed.value.push_back('x');
  MK_EXPECT_TRUE(RecordTooLarge(keyed));

  // R17：客户端可按 Broker 配置调低上限（默认 1 MiB）。
  RawMessage small;
  small.value.assign(100u, 'x');
  MK_EXPECT_FALSE(RecordTooLarge(small, 1024u));
  MK_EXPECT_TRUE(RecordTooLarge(small, 100u));
}

void TestIsRetryable() {
  MK_EXPECT_TRUE(IsRetryable(StatusCode::kDiskFull));
  MK_EXPECT_TRUE(IsRetryable(StatusCode::kBrokerUnavailable));
  MK_EXPECT_TRUE(IsRetryable(StatusCode::kStorageIoError));
  MK_EXPECT_TRUE(IsRetryable(StatusCode::kStorageUnavailable));
  MK_EXPECT_FALSE(IsRetryable(StatusCode::kMessageTooLarge));
  MK_EXPECT_FALSE(IsRetryable(StatusCode::kInvalidPartition));
  MK_EXPECT_FALSE(IsRetryable(StatusCode::kTopicNotFound));
}

}  // namespace

void TestClientCli() {
  TestParseAddress();
  TestParseFrom();
  TestParseInterval();
  TestParseU64();
  TestQuoteAndRecordLine();
  TestExitCodeFor();
  TestParseMessageLine();
  TestRecordTooLarge();
  TestIsRetryable();
}
