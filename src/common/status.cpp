#include "minikafka/common/status.h"

#include <utility>

namespace minikafka {
namespace common {
namespace {

struct CodeName {
  StatusCode code;
  const char* name;
};

const CodeName kCodeNames[] = {
    {StatusCode::kOk, "ok"},
    {StatusCode::kInvalidFrame, "invalid_frame"},
    {StatusCode::kFrameCrcError, "frame_crc_error"},
    {StatusCode::kUnsupportedVersion, "unsupported_version"},
    {StatusCode::kUnsupportedMessageType, "unsupported_message_type"},
    {StatusCode::kPayloadDecodeError, "payload_decode_error"},
    {StatusCode::kRequestTooLarge, "request_too_large"},
    {StatusCode::kInvalidTopicName, "invalid_topic_name"},
    {StatusCode::kInvalidGroupName, "invalid_group_name"},
    {StatusCode::kTopicNotFound, "topic_not_found"},
    {StatusCode::kTopicExists, "topic_exists"},
    {StatusCode::kInvalidPartitions, "invalid_partitions"},
    {StatusCode::kInvalidPartition, "invalid_partition"},
    {StatusCode::kInvalidConsumerSlot, "invalid_consumer_slot"},
    {StatusCode::kMessageTooLarge, "message_too_large"},
    {StatusCode::kInvalidValueType, "invalid_value_type"},
    {StatusCode::kInvalidJson, "invalid_json"},
    {StatusCode::kInvalidUtf8, "invalid_utf8"},
    {StatusCode::kInvalidBatch, "invalid_batch"},
    {StatusCode::kPartitionBlocked, "partition_blocked"},
    {StatusCode::kOffsetOutOfRange, "offset_out_of_range"},
    {StatusCode::kInvalidOffset, "invalid_offset"},
    {StatusCode::kInvalidCount, "invalid_count"},
    {StatusCode::kInvalidFrom, "invalid_from"},
    {StatusCode::kInvalidInterval, "invalid_interval"},
    {StatusCode::kBrokerUnavailable, "broker_unavailable"},
    {StatusCode::kInternalError, "internal_error"},
    {StatusCode::kDiskFull, "disk_full"},
    {StatusCode::kStorageUnavailable, "storage_unavailable"},
    {StatusCode::kStorageIoError, "storage_io_error"},
    {StatusCode::kDeliveryFailed, "delivery_failed"},
    {StatusCode::kUsage, "usage"},
};

}  // namespace

const char* StatusCodeToString(StatusCode code) {
  for (const CodeName& entry : kCodeNames) {
    if (entry.code == code) {
      return entry.name;
    }
  }
  return "unknown";
}

bool StatusCodeFromString(const std::string& name, StatusCode* code) {
  if (code == nullptr) {
    return false;
  }
  for (const CodeName& entry : kCodeNames) {
    if (name == entry.name) {
      *code = entry.code;
      return true;
    }
  }
  return false;
}

Status::Status(StatusCode code, std::string message)
    : code_(code),
      message_(code == StatusCode::kOk ? std::string() : std::move(message)) {}

std::string Status::ToString() const {
  std::string result = StatusCodeToString(code_);
  if (!message_.empty()) {
    result.append(": ");
    result.append(message_);
  }
  return result;
}

}  // namespace common
}  // namespace minikafka
