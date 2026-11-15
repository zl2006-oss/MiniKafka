#include "test_common.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

#include "minikafka/common/status.h"

namespace {

struct CodeExpectation {
  minikafka::common::StatusCode code;
  std::uint16_t wire;
  const char* name;
};

}  // namespace

void TestStatus() {
  using minikafka::common::Status;
  using minikafka::common::StatusCode;
  using minikafka::common::StatusCodeFromString;
  using minikafka::common::StatusCodeToString;
  using minikafka::common::StatusOr;

  const CodeExpectation kTable[] = {
      {StatusCode::kOk, 0x0000, "ok"},
      {StatusCode::kInvalidFrame, 0x0100, "invalid_frame"},
      {StatusCode::kFrameCrcError, 0x0101, "frame_crc_error"},
      {StatusCode::kUnsupportedVersion, 0x0102, "unsupported_version"},
      {StatusCode::kUnsupportedMessageType, 0x0103,
       "unsupported_message_type"},
      {StatusCode::kPayloadDecodeError, 0x0104, "payload_decode_error"},
      {StatusCode::kRequestTooLarge, 0x0105, "request_too_large"},
      {StatusCode::kInvalidTopicName, 0x0201, "invalid_topic_name"},
      {StatusCode::kInvalidGroupName, 0x0202, "invalid_group_name"},
      {StatusCode::kTopicNotFound, 0x0203, "topic_not_found"},
      {StatusCode::kTopicExists, 0x0204, "topic_exists"},
      {StatusCode::kInvalidPartitions, 0x0205, "invalid_partitions"},
      {StatusCode::kInvalidPartition, 0x0206, "invalid_partition"},
      {StatusCode::kInvalidConsumerSlot, 0x0207, "invalid_consumer_slot"},
      {StatusCode::kMessageTooLarge, 0x0301, "message_too_large"},
      {StatusCode::kInvalidValueType, 0x0302, "invalid_value_type"},
      {StatusCode::kInvalidJson, 0x0303, "invalid_json"},
      {StatusCode::kInvalidUtf8, 0x0304, "invalid_utf8"},
      {StatusCode::kInvalidBatch, 0x0305, "invalid_batch"},
      {StatusCode::kPartitionBlocked, 0x0306, "partition_blocked"},
      {StatusCode::kOffsetOutOfRange, 0x0401, "offset_out_of_range"},
      {StatusCode::kInvalidOffset, 0x0402, "invalid_offset"},
      {StatusCode::kInvalidCount, 0x0403, "invalid_count"},
      {StatusCode::kInvalidFrom, 0x0404, "invalid_from"},
      {StatusCode::kInvalidInterval, 0x0405, "invalid_interval"},
      {StatusCode::kBrokerUnavailable, 0x0601, "broker_unavailable"},
      {StatusCode::kInternalError, 0x0602, "internal_error"},
      {StatusCode::kDiskFull, 0x0701, "disk_full"},
      {StatusCode::kStorageUnavailable, 0x0702, "storage_unavailable"},
      {StatusCode::kStorageIoError, 0x0703, "storage_io_error"},
      {StatusCode::kDeliveryFailed, 0x0901, "delivery_failed"},
      {StatusCode::kUsage, 0xFFFF, "usage"},
  };

  for (const CodeExpectation& expectation : kTable) {
    MK_EXPECT_EQ(static_cast<std::uint16_t>(expectation.code),
                 expectation.wire);
    MK_EXPECT_EQ(std::string(StatusCodeToString(expectation.code)),
                 std::string(expectation.name));

    StatusCode parsed = StatusCode::kOk;
    MK_EXPECT_TRUE(StatusCodeFromString(expectation.name, &parsed));
    MK_EXPECT_EQ(parsed, expectation.code);
  }

  StatusCode unknown = StatusCode::kOk;
  MK_EXPECT_FALSE(StatusCodeFromString("not_a_code", &unknown));
  MK_EXPECT_FALSE(StatusCodeFromString("ok", nullptr));
  MK_EXPECT_EQ(
      std::string(StatusCodeToString(static_cast<StatusCode>(0x1234))),
      std::string("unknown"));

  const Status ok;
  MK_EXPECT_TRUE(ok.ok());
  MK_EXPECT_TRUE(static_cast<bool>(ok));
  MK_EXPECT_EQ(ok.code(), StatusCode::kOk);
  MK_EXPECT_EQ(ok.message(), std::string());

  const Status error(StatusCode::kDiskFull, "no space");
  MK_EXPECT_FALSE(error.ok());
  MK_EXPECT_FALSE(static_cast<bool>(error));
  MK_EXPECT_EQ(error.code(), StatusCode::kDiskFull);
  MK_EXPECT_EQ(error.message(), std::string("no space"));
  MK_EXPECT_TRUE(error.ToString().find("disk_full") != std::string::npos);

  const Status ok_with_message(StatusCode::kOk, "ignored");
  MK_EXPECT_TRUE(ok_with_message.ok());
  MK_EXPECT_EQ(ok_with_message.message(), std::string());

  StatusOr<int> value(7);
  MK_EXPECT_TRUE(value.ok());
  MK_EXPECT_EQ(value.value(), 7);
  MK_EXPECT_TRUE(value.value_if() != nullptr);
  MK_EXPECT_EQ(*value.value_if(), 7);
  MK_EXPECT_EQ(*value, 7);

  StatusOr<int> failed(Status(StatusCode::kInvalidJson, "bad json"));
  MK_EXPECT_FALSE(failed.ok());
  MK_EXPECT_EQ(failed.status().code(), StatusCode::kInvalidJson);
  MK_EXPECT_TRUE(failed.value_if() == nullptr);

  StatusOr<int> copied(value);
  MK_EXPECT_TRUE(copied.ok());
  MK_EXPECT_EQ(copied.value(), 7);

  StatusOr<int> moved(std::move(copied));
  MK_EXPECT_TRUE(moved.ok());
  MK_EXPECT_EQ(moved.value(), 7);

  StatusOr<int> copied_assignment(1);
  copied_assignment = value;
  MK_EXPECT_TRUE(copied_assignment.ok());
  MK_EXPECT_EQ(copied_assignment.value(), 7);

  StatusOr<int> moved_assignment(2);
  moved_assignment = std::move(moved);
  MK_EXPECT_TRUE(moved_assignment.ok());
  MK_EXPECT_EQ(moved_assignment.value(), 7);

  StatusOr<std::string> text(std::string("abc"));
  MK_EXPECT_TRUE(text.ok());
  MK_EXPECT_EQ(text.value(), std::string("abc"));
  MK_EXPECT_EQ(text->size(), static_cast<std::size_t>(3));

  StatusOr<std::string> const_text(std::string("xyz"));
  const StatusOr<std::string>& const_reference = const_text;
  MK_EXPECT_EQ(const_reference.value(), std::string("xyz"));
  MK_EXPECT_TRUE(const_reference.value_if() != nullptr);

  const Status ok_status;
  StatusOr<int> from_ok_status(ok_status);
  MK_EXPECT_FALSE(from_ok_status.ok());
  MK_EXPECT_EQ(from_ok_status.status().code(), StatusCode::kInternalError);
}
