#include "minikafka/protocol/messages.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace minikafka {
namespace protocol {
namespace {

const std::size_t kMaxU32AsSizeT = static_cast<std::size_t>(0xFFFFFFFFu);
const std::size_t kStorageRecordHeaderBytes = 24u;

bool IsWireStatusCode(common::StatusCode code) {
  if (code == common::StatusCode::kUsage) {
    return false;
  }
  return std::string(common::StatusCodeToString(code)) != "unknown";
}

Status EncodeWireStatusCode(common::StatusCode code, Encoder* encoder) {
  if (encoder == nullptr) {
    return Status(common::StatusCode::kInternalError, "null encoder");
  }
  if (!IsWireStatusCode(code)) {
    return Status(common::StatusCode::kInternalError,
                  "error code is not valid on the wire");
  }
  return encoder->WriteU16(static_cast<std::uint16_t>(code));
}

Status DecodeWireStatusCode(Decoder* decoder, common::StatusCode* code) {
  if (decoder == nullptr || code == nullptr) {
    return Status(common::StatusCode::kInternalError, "null output");
  }
  std::uint16_t wire_code = 0;
  Status status = decoder->ReadU16(&wire_code);
  if (!status.ok()) {
    return status;
  }
  if (static_cast<unsigned int>(wire_code) == 0xFFFFu) {
    return Status(common::StatusCode::kPayloadDecodeError,
                  "usage is not a wire error code");
  }
  const common::StatusCode parsed =
      static_cast<common::StatusCode>(wire_code);
  if (!IsWireStatusCode(parsed)) {
    return Status(common::StatusCode::kPayloadDecodeError,
                  "unknown wire error code");
  }
  *code = parsed;
  return Status();
}

bool ValidKeyFlags(std::uint16_t flags, const std::string& key) {
  if ((flags & 0xFFFEu) != 0u) {
    return false;
  }
  if ((flags & 0x0001u) == 0u && !key.empty()) {
    return false;
  }
  return true;
}

Status ValidateRecordSize(const std::string& key, const std::string& value) {
  const std::size_t record_size =
      kStorageRecordHeaderBytes + key.size() + value.size();
  if (record_size > kMaxRecordSize) {
    return Status(common::StatusCode::kMessageTooLarge,
                  "record_size exceeds 1 MiB");
  }
  return Status();
}

Status Structural(bool encoding, const std::string& message) {
  return Status(encoding ? common::StatusCode::kInternalError
                         : common::StatusCode::kPayloadDecodeError,
                message);
}

Status ValidateProduceResults(const std::vector<ProduceResult>& results,
                              std::uint8_t response_status, bool encoding) {
  if (response_status > 2) {
    return Structural(encoding, "invalid produce response status");
  }
  if (results.size() > kMaxProduceEntries) {
    return Structural(encoding, "produce result_count exceeds 1024");
  }

  std::size_t appended = 0;
  std::size_t not_appended = 0;
  for (std::size_t i = 0; i < results.size(); ++i) {
    const ProduceResult& result = results[i];
    if (result.index != i) {
      return Structural(encoding, "produce result index is out of order");
    }
    if (result.status > 2) {
      return Structural(encoding, "invalid produce result status");
    }
    if (result.status == 0) {
      if (result.error_code != common::StatusCode::kOk) {
        return Structural(encoding,
                          "appended result must carry ok error code");
      }
      ++appended;
    } else if (result.status == 1) {
      if (result.error_code == common::StatusCode::kOk) {
        return Structural(encoding,
                          "failed result must carry a non-ok error code");
      }
      if (result.offset != 0u) {
        return Structural(encoding, "failed result offset must be zero");
      }
      ++not_appended;
    } else {
      if (result.error_code != common::StatusCode::kPartitionBlocked) {
        return Structural(encoding,
                          "not_attempted result must be partition_blocked");
      }
      if (result.offset != 0u) {
        return Structural(encoding,
                          "not_attempted result offset must be zero");
      }
      ++not_appended;
    }
  }

  if (response_status == 0 && (appended == 0u || not_appended != 0u)) {
    return Structural(encoding, "status 0 requires all results appended");
  }
  if (response_status == 1 && (appended == 0u || not_appended == 0u)) {
    return Structural(encoding,
                      "status 1 requires both appended and not-appended");
  }
  if (response_status == 2 && appended != 0u) {
    return Structural(encoding, "status 2 must not contain appended results");
  }
  return Status();
}

Status ValidateOffsetFetchResponse(const OffsetFetchResponse& response,
                                   bool encoding) {
  if (response.assigned_partitions.size() != response.entries.size()) {
    return Structural(encoding,
                      "assigned_count must equal entry_count");
  }
  for (std::size_t i = 0; i < response.assigned_partitions.size(); ++i) {
    if (response.assigned_partitions[i] != response.entries[i].partition) {
      return Structural(encoding,
                        "assigned partitions and entries must match");
    }
    if (i != 0u &&
        response.assigned_partitions[i] <=
            response.assigned_partitions[i - 1u]) {
      return Structural(encoding,
                        "assigned partitions must be strictly ascending");
    }
  }
  return Status();
}

}  // namespace

bool IsKnownMessageType(std::uint16_t msg_type) {
  return (msg_type >= 0x0001u && msg_type <= 0x0007u) ||
         (msg_type >= 0x8001u && msg_type <= 0x8007u);
}

Status EncodeResponsePrefix(common::StatusCode error_code,
                            const std::string& message,
                            Encoder* encoder) {
  if (encoder == nullptr) {
    return Status(common::StatusCode::kInternalError, "null encoder");
  }
  if (error_code == common::StatusCode::kOk && !message.empty()) {
    return Status(common::StatusCode::kInternalError,
                  "ok response must not carry a message");
  }
  Status status = EncodeWireStatusCode(error_code, encoder);
  if (!status.ok()) {
    return status;
  }
  if (error_code == common::StatusCode::kOk) {
    return Status();
  }
  return encoder->WriteMessage(message);
}

Status DecodeResponsePrefix(Decoder* decoder, common::StatusCode* error_code,
                            std::string* message) {
  if (decoder == nullptr || error_code == nullptr || message == nullptr) {
    return Status(common::StatusCode::kInternalError, "null output");
  }
  Status status = DecodeWireStatusCode(decoder, error_code);
  if (!status.ok()) {
    return status;
  }
  message->clear();
  if (*error_code == common::StatusCode::kOk) {
    return Status();
  }
  return decoder->ReadMessage(message);
}

Status EncodeCreateTopicRequest(const CreateTopicRequest& request,
                                std::string* payload) {
  if (payload == nullptr) {
    return Status(common::StatusCode::kInternalError, "null payload output");
  }
  if (request.partition_count < 1u ||
      request.partition_count > kMaxPartitionCount) {
    return Status(common::StatusCode::kInvalidPartitions,
                  "partition_count must be in 1..16");
  }

  std::string buffer;
  Encoder encoder(buffer);
  Status status = encoder.WriteStr(request.topic);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU32(request.partition_count);
  if (!status.ok()) {
    return status;
  }
  *payload = std::move(buffer);
  return Status();
}

Status DecodeCreateTopicRequest(const std::string& payload,
                                CreateTopicRequest* request) {
  if (request == nullptr) {
    return Status(common::StatusCode::kInternalError, "null output");
  }
  Decoder decoder(payload);
  CreateTopicRequest result;
  Status status = decoder.ReadStr(&result.topic);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadU32(&result.partition_count);
  if (!status.ok()) {
    return status;
  }
  if (result.partition_count < 1u ||
      result.partition_count > kMaxPartitionCount) {
    return Status(common::StatusCode::kInvalidPartitions,
                  "partition_count must be in 1..16");
  }
  status = decoder.Finish();
  if (!status.ok()) {
    return status;
  }
  *request = std::move(result);
  return Status();
}

Status EncodeCreateTopicResponse(const CreateTopicResponse& response,
                                 std::string* payload) {
  if (payload == nullptr) {
    return Status(common::StatusCode::kInternalError, "null payload output");
  }
  std::string buffer;
  Encoder encoder(buffer);
  Status status =
      EncodeResponsePrefix(response.error_code, response.message, &encoder);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteStr(response.topic);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU32(response.partition_count);
  if (!status.ok()) {
    return status;
  }
  *payload = std::move(buffer);
  return Status();
}

Status DecodeCreateTopicResponse(const std::string& payload,
                                 CreateTopicResponse* response) {
  if (response == nullptr) {
    return Status(common::StatusCode::kInternalError, "null output");
  }
  Decoder decoder(payload);
  CreateTopicResponse result;
  Status status =
      DecodeResponsePrefix(&decoder, &result.error_code, &result.message);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadStr(&result.topic);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadU32(&result.partition_count);
  if (!status.ok()) {
    return status;
  }
  status = decoder.Finish();
  if (!status.ok()) {
    return status;
  }
  *response = std::move(result);
  return Status();
}

Status EncodeListTopicsRequest(std::string* payload) {
  if (payload == nullptr) {
    return Status(common::StatusCode::kInternalError, "null payload output");
  }
  payload->clear();
  return Status();
}

Status DecodeListTopicsRequest(const std::string& payload) {
  Decoder decoder(payload);
  return decoder.Finish();
}

Status EncodeListTopicsResponse(const ListTopicsResponse& response,
                                std::string* payload) {
  if (payload == nullptr) {
    return Status(common::StatusCode::kInternalError, "null payload output");
  }
  if (response.entries.size() > kMaxU32AsSizeT) {
    return Status(common::StatusCode::kInternalError,
                  "topic_count exceeds u32");
  }

  std::string buffer;
  Encoder encoder(buffer);
  Status status =
      EncodeResponsePrefix(response.error_code, response.message, &encoder);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU32(
      static_cast<std::uint32_t>(response.entries.size()));
  if (!status.ok()) {
    return status;
  }
  for (const TopicInfo& entry : response.entries) {
    status = encoder.WriteStr(entry.topic);
    if (!status.ok()) {
      return status;
    }
    status = encoder.WriteU32(entry.partition_count);
    if (!status.ok()) {
      return status;
    }
  }
  *payload = std::move(buffer);
  return Status();
}

Status DecodeListTopicsResponse(const std::string& payload,
                                ListTopicsResponse* response) {
  if (response == nullptr) {
    return Status(common::StatusCode::kInternalError, "null output");
  }
  Decoder decoder(payload);
  ListTopicsResponse result;
  Status status =
      DecodeResponsePrefix(&decoder, &result.error_code, &result.message);
  if (!status.ok()) {
    return status;
  }
  std::uint32_t topic_count = 0;
  status = decoder.ReadU32(&topic_count);
  if (!status.ok()) {
    return status;
  }
  if (static_cast<std::size_t>(topic_count) > decoder.remaining() / 8u) {
    return Status(common::StatusCode::kPayloadDecodeError,
                  "topic_count exceeds remaining payload");
  }
  for (std::uint32_t i = 0; i < topic_count; ++i) {
    TopicInfo entry;
    status = decoder.ReadStr(&entry.topic);
    if (!status.ok()) {
      return status;
    }
    status = decoder.ReadU32(&entry.partition_count);
    if (!status.ok()) {
      return status;
    }
    result.entries.push_back(std::move(entry));
  }
  status = decoder.Finish();
  if (!status.ok()) {
    return status;
  }
  *response = std::move(result);
  return Status();
}

Status EncodeProduceRequest(const ProduceRequest& request,
                            std::string* payload) {
  if (payload == nullptr) {
    return Status(common::StatusCode::kInternalError, "null payload output");
  }
  if (request.entries.empty() ||
      request.entries.size() > kMaxProduceEntries) {
    return Status(common::StatusCode::kInvalidBatch,
                  "entry_count must be in 1..1024");
  }

  std::string buffer;
  Encoder encoder(buffer);
  Status status = encoder.WriteStr(request.topic);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU32(request.partition);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU32(
      static_cast<std::uint32_t>(request.entries.size()));
  if (!status.ok()) {
    return status;
  }
  for (const ProduceEntry& entry : request.entries) {
    if (!ValidKeyFlags(entry.flags, entry.key)) {
      return Status(common::StatusCode::kInternalError,
                    "invalid produce entry flags/key combination");
    }
    status = ValidateRecordSize(entry.key, entry.value);
    if (!status.ok()) {
      return status;
    }
    status = encoder.WriteU16(entry.flags);
    if (!status.ok()) {
      return status;
    }
    status = encoder.WriteUtf8Blob(entry.key);
    if (!status.ok()) {
      return status;
    }
    status = encoder.WriteUtf8Blob(entry.value);
    if (!status.ok()) {
      return status;
    }
  }
  *payload = std::move(buffer);
  return Status();
}

Status DecodeProduceRequest(const std::string& payload,
                            ProduceRequest* request) {
  if (request == nullptr) {
    return Status(common::StatusCode::kInternalError, "null output");
  }
  Decoder decoder(payload);
  ProduceRequest result;
  Status status = decoder.ReadStr(&result.topic);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadU32(&result.partition);
  if (!status.ok()) {
    return status;
  }
  std::uint32_t entry_count = 0;
  status = decoder.ReadU32(&entry_count);
  if (!status.ok()) {
    return status;
  }
  if (entry_count < 1u || entry_count > kMaxProduceEntries) {
    return Status(common::StatusCode::kInvalidBatch,
                  "entry_count must be in 1..1024");
  }
  if (static_cast<std::size_t>(entry_count) > decoder.remaining() / 10u) {
    return Status(common::StatusCode::kPayloadDecodeError,
                  "entry_count exceeds remaining payload");
  }
  for (std::uint32_t i = 0; i < entry_count; ++i) {
    ProduceEntry entry;
    status = decoder.ReadU16(&entry.flags);
    if (!status.ok()) {
      return status;
    }
    status = decoder.ReadUtf8Blob(&entry.key);
    if (!status.ok()) {
      return status;
    }
    status = decoder.ReadUtf8Blob(&entry.value);
    if (!status.ok()) {
      return status;
    }
    if (!ValidKeyFlags(entry.flags, entry.key)) {
      return Status(common::StatusCode::kPayloadDecodeError,
                    "invalid produce entry flags/key combination");
    }
    status = ValidateRecordSize(entry.key, entry.value);
    if (!status.ok()) {
      return status;
    }
    result.entries.push_back(std::move(entry));
  }
  status = decoder.Finish();
  if (!status.ok()) {
    return status;
  }
  *request = std::move(result);
  return Status();
}

Status EncodeProduceResponse(const ProduceResponse& response,
                             std::string* payload) {
  if (payload == nullptr) {
    return Status(common::StatusCode::kInternalError, "null payload output");
  }
  Status status = ValidateProduceResults(response.results, response.status, true);
  if (!status.ok()) {
    return status;
  }

  std::string buffer;
  Encoder encoder(buffer);
  status = EncodeResponsePrefix(response.error_code, response.message, &encoder);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU8(response.status);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU32(
      static_cast<std::uint32_t>(response.results.size()));
  if (!status.ok()) {
    return status;
  }
  for (const ProduceResult& result : response.results) {
    status = encoder.WriteU32(result.index);
    if (!status.ok()) {
      return status;
    }
    status = encoder.WriteU8(result.status);
    if (!status.ok()) {
      return status;
    }
    status = EncodeWireStatusCode(result.error_code, &encoder);
    if (!status.ok()) {
      return status;
    }
    status = encoder.WriteU64(result.offset);
    if (!status.ok()) {
      return status;
    }
  }
  *payload = std::move(buffer);
  return Status();
}

Status DecodeProduceResponse(const std::string& payload,
                             ProduceResponse* response) {
  if (response == nullptr) {
    return Status(common::StatusCode::kInternalError, "null output");
  }
  Decoder decoder(payload);
  ProduceResponse result;
  Status status =
      DecodeResponsePrefix(&decoder, &result.error_code, &result.message);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadU8(&result.status);
  if (!status.ok()) {
    return status;
  }
  std::uint32_t result_count = 0;
  status = decoder.ReadU32(&result_count);
  if (!status.ok()) {
    return status;
  }
  if (result_count > kMaxProduceEntries) {
    return Status(common::StatusCode::kPayloadDecodeError,
                  "result_count exceeds 1024");
  }
  if (static_cast<std::size_t>(result_count) > decoder.remaining() / 15u) {
    return Status(common::StatusCode::kPayloadDecodeError,
                  "result_count exceeds remaining payload");
  }
  for (std::uint32_t i = 0; i < result_count; ++i) {
    ProduceResult entry;
    status = decoder.ReadU32(&entry.index);
    if (!status.ok()) {
      return status;
    }
    status = decoder.ReadU8(&entry.status);
    if (!status.ok()) {
      return status;
    }
    status = DecodeWireStatusCode(&decoder, &entry.error_code);
    if (!status.ok()) {
      return status;
    }
    status = decoder.ReadU64(&entry.offset);
    if (!status.ok()) {
      return status;
    }
    result.results.push_back(std::move(entry));
  }
  status = decoder.Finish();
  if (!status.ok()) {
    return status;
  }
  status = ValidateProduceResults(result.results, result.status, false);
  if (!status.ok()) {
    return status;
  }
  *response = std::move(result);
  return Status();
}

Status EncodeFetchRequest(const FetchRequest& request, std::string* payload) {
  if (payload == nullptr) {
    return Status(common::StatusCode::kInternalError, "null payload output");
  }
  if (request.max_bytes > kMaxFetchBytes) {
    return Status(common::StatusCode::kInternalError,
                  "max_bytes exceeds 4 MiB");
  }

  std::string buffer;
  Encoder encoder(buffer);
  Status status = encoder.WriteStr(request.topic);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU32(request.partition);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU64(request.offset);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU32(request.max_bytes);
  if (!status.ok()) {
    return status;
  }
  *payload = std::move(buffer);
  return Status();
}

Status DecodeFetchRequest(const std::string& payload, FetchRequest* request) {
  if (request == nullptr) {
    return Status(common::StatusCode::kInternalError, "null output");
  }
  Decoder decoder(payload);
  FetchRequest result;
  Status status = decoder.ReadStr(&result.topic);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadU32(&result.partition);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadU64(&result.offset);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadU32(&result.max_bytes);
  if (!status.ok()) {
    return status;
  }
  if (result.max_bytes > kMaxFetchBytes) {
    return Status(common::StatusCode::kPayloadDecodeError,
                  "max_bytes exceeds 4 MiB");
  }
  status = decoder.Finish();
  if (!status.ok()) {
    return status;
  }
  *request = std::move(result);
  return Status();
}

Status EncodeFetchResponse(const FetchResponse& response,
                           std::string* payload) {
  if (payload == nullptr) {
    return Status(common::StatusCode::kInternalError, "null payload output");
  }
  if (response.records.size() > kMaxU32AsSizeT) {
    return Status(common::StatusCode::kInternalError,
                  "record_count exceeds u32");
  }

  std::string buffer;
  Encoder encoder(buffer);
  Status status =
      EncodeResponsePrefix(response.error_code, response.message, &encoder);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteStr(response.topic);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU32(response.partition);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU64(response.log_end_offset);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU32(
      static_cast<std::uint32_t>(response.records.size()));
  if (!status.ok()) {
    return status;
  }
  for (const FetchRecord& record : response.records) {
    if (!ValidKeyFlags(record.flags, record.key)) {
      return Status(common::StatusCode::kInternalError,
                    "invalid fetch record flags/key combination");
    }
    status = encoder.WriteU64(record.offset);
    if (!status.ok()) {
      return status;
    }
    status = encoder.WriteU16(record.flags);
    if (!status.ok()) {
      return status;
    }
    status = encoder.WriteUtf8Blob(record.key);
    if (!status.ok()) {
      return status;
    }
    status = encoder.WriteUtf8Blob(record.value);
    if (!status.ok()) {
      return status;
    }
  }
  *payload = std::move(buffer);
  return Status();
}

Status DecodeFetchResponse(const std::string& payload,
                           FetchResponse* response) {
  if (response == nullptr) {
    return Status(common::StatusCode::kInternalError, "null output");
  }
  Decoder decoder(payload);
  FetchResponse result;
  Status status =
      DecodeResponsePrefix(&decoder, &result.error_code, &result.message);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadStr(&result.topic);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadU32(&result.partition);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadU64(&result.log_end_offset);
  if (!status.ok()) {
    return status;
  }
  std::uint32_t record_count = 0;
  status = decoder.ReadU32(&record_count);
  if (!status.ok()) {
    return status;
  }
  if (static_cast<std::size_t>(record_count) > decoder.remaining() / 18u) {
    return Status(common::StatusCode::kPayloadDecodeError,
                  "record_count exceeds remaining payload");
  }
  for (std::uint32_t i = 0; i < record_count; ++i) {
    FetchRecord record;
    status = decoder.ReadU64(&record.offset);
    if (!status.ok()) {
      return status;
    }
    status = decoder.ReadU16(&record.flags);
    if (!status.ok()) {
      return status;
    }
    status = decoder.ReadUtf8Blob(&record.key);
    if (!status.ok()) {
      return status;
    }
    status = decoder.ReadUtf8Blob(&record.value);
    if (!status.ok()) {
      return status;
    }
    if (!ValidKeyFlags(record.flags, record.key)) {
      return Status(common::StatusCode::kPayloadDecodeError,
                    "invalid fetch record flags/key combination");
    }
    result.records.push_back(std::move(record));
  }
  status = decoder.Finish();
  if (!status.ok()) {
    return status;
  }
  *response = std::move(result);
  return Status();
}

Status EncodeOffsetFetchRequest(const OffsetFetchRequest& request,
                                std::string* payload) {
  if (payload == nullptr) {
    return Status(common::StatusCode::kInternalError, "null payload output");
  }
  if (request.consumer_count < 1u ||
      request.consumer_count > kMaxPartitionCount ||
      request.consumer_index >= request.consumer_count) {
    return Status(common::StatusCode::kInvalidConsumerSlot,
                  "invalid consumer slot");
  }

  std::string buffer;
  Encoder encoder(buffer);
  Status status = encoder.WriteStr(request.group);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteStr(request.topic);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU32(request.consumer_index);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU32(request.consumer_count);
  if (!status.ok()) {
    return status;
  }
  *payload = std::move(buffer);
  return Status();
}

Status DecodeOffsetFetchRequest(const std::string& payload,
                                OffsetFetchRequest* request) {
  if (request == nullptr) {
    return Status(common::StatusCode::kInternalError, "null output");
  }
  Decoder decoder(payload);
  OffsetFetchRequest result;
  Status status = decoder.ReadStr(&result.group);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadStr(&result.topic);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadU32(&result.consumer_index);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadU32(&result.consumer_count);
  if (!status.ok()) {
    return status;
  }
  if (result.consumer_count < 1u ||
      result.consumer_count > kMaxPartitionCount ||
      result.consumer_index >= result.consumer_count) {
    return Status(common::StatusCode::kInvalidConsumerSlot,
                  "invalid consumer slot");
  }
  status = decoder.Finish();
  if (!status.ok()) {
    return status;
  }
  *request = std::move(result);
  return Status();
}

Status EncodeOffsetFetchResponse(const OffsetFetchResponse& response,
                                 std::string* payload) {
  if (payload == nullptr) {
    return Status(common::StatusCode::kInternalError, "null payload output");
  }
  if (response.assigned_partitions.size() > kMaxU32AsSizeT ||
      response.entries.size() > kMaxU32AsSizeT) {
    return Status(common::StatusCode::kInternalError,
                  "offset fetch counts exceed u32");
  }
  Status status = ValidateOffsetFetchResponse(response, true);
  if (!status.ok()) {
    return status;
  }

  std::string buffer;
  Encoder encoder(buffer);
  status = EncodeResponsePrefix(response.error_code, response.message, &encoder);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU32(
      static_cast<std::uint32_t>(response.assigned_partitions.size()));
  if (!status.ok()) {
    return status;
  }
  for (std::uint32_t partition : response.assigned_partitions) {
    status = encoder.WriteU32(partition);
    if (!status.ok()) {
      return status;
    }
  }
  status = encoder.WriteU32(
      static_cast<std::uint32_t>(response.entries.size()));
  if (!status.ok()) {
    return status;
  }
  for (const OffsetFetchEntry& entry : response.entries) {
    status = encoder.WriteU32(entry.partition);
    if (!status.ok()) {
      return status;
    }
    status = encoder.WriteU8(entry.has_committed ? 1u : 0u);
    if (!status.ok()) {
      return status;
    }
    status = encoder.WriteU64(entry.committed_offset);
    if (!status.ok()) {
      return status;
    }
  }
  *payload = std::move(buffer);
  return Status();
}

Status DecodeOffsetFetchResponse(const std::string& payload,
                                 OffsetFetchResponse* response) {
  if (response == nullptr) {
    return Status(common::StatusCode::kInternalError, "null output");
  }
  Decoder decoder(payload);
  OffsetFetchResponse result;
  Status status =
      DecodeResponsePrefix(&decoder, &result.error_code, &result.message);
  if (!status.ok()) {
    return status;
  }
  std::uint32_t assigned_count = 0;
  status = decoder.ReadU32(&assigned_count);
  if (!status.ok()) {
    return status;
  }
  if (static_cast<std::size_t>(assigned_count) > decoder.remaining() / 4u) {
    return Status(common::StatusCode::kPayloadDecodeError,
                  "assigned_count exceeds remaining payload");
  }
  for (std::uint32_t i = 0; i < assigned_count; ++i) {
    std::uint32_t partition = 0;
    status = decoder.ReadU32(&partition);
    if (!status.ok()) {
      return status;
    }
    result.assigned_partitions.push_back(partition);
  }
  std::uint32_t entry_count = 0;
  status = decoder.ReadU32(&entry_count);
  if (!status.ok()) {
    return status;
  }
  if (static_cast<std::size_t>(entry_count) > decoder.remaining() / 13u) {
    return Status(common::StatusCode::kPayloadDecodeError,
                  "entry_count exceeds remaining payload");
  }
  for (std::uint32_t i = 0; i < entry_count; ++i) {
    OffsetFetchEntry entry;
    status = decoder.ReadU32(&entry.partition);
    if (!status.ok()) {
      return status;
    }
    std::uint8_t has_committed = 0;
    status = decoder.ReadU8(&has_committed);
    if (!status.ok()) {
      return status;
    }
    if (has_committed > 1) {
      return Status(common::StatusCode::kPayloadDecodeError,
                    "has_committed must be 0 or 1");
    }
    entry.has_committed = has_committed != 0;
    status = decoder.ReadU64(&entry.committed_offset);
    if (!status.ok()) {
      return status;
    }
    result.entries.push_back(std::move(entry));
  }
  status = decoder.Finish();
  if (!status.ok()) {
    return status;
  }
  status = ValidateOffsetFetchResponse(result, false);
  if (!status.ok()) {
    return status;
  }
  *response = std::move(result);
  return Status();
}

Status EncodeOffsetCommitRequest(const OffsetCommitRequest& request,
                                 std::string* payload) {
  if (payload == nullptr) {
    return Status(common::StatusCode::kInternalError, "null payload output");
  }
  if (request.entries.size() > kMaxOffsetCommitEntries) {
    return Status(common::StatusCode::kInternalError,
                  "entry_count exceeds 16");
  }

  std::string buffer;
  Encoder encoder(buffer);
  Status status = encoder.WriteStr(request.group);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteStr(request.topic);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU32(
      static_cast<std::uint32_t>(request.entries.size()));
  if (!status.ok()) {
    return status;
  }
  for (const OffsetCommitEntry& entry : request.entries) {
    status = encoder.WriteU32(entry.partition);
    if (!status.ok()) {
      return status;
    }
    status = encoder.WriteU64(entry.committed_offset);
    if (!status.ok()) {
      return status;
    }
  }
  *payload = std::move(buffer);
  return Status();
}

Status DecodeOffsetCommitRequest(const std::string& payload,
                                 OffsetCommitRequest* request) {
  if (request == nullptr) {
    return Status(common::StatusCode::kInternalError, "null output");
  }
  Decoder decoder(payload);
  OffsetCommitRequest result;
  Status status = decoder.ReadStr(&result.group);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadStr(&result.topic);
  if (!status.ok()) {
    return status;
  }
  std::uint32_t entry_count = 0;
  status = decoder.ReadU32(&entry_count);
  if (!status.ok()) {
    return status;
  }
  if (entry_count > kMaxOffsetCommitEntries) {
    return Status(common::StatusCode::kPayloadDecodeError,
                  "entry_count exceeds 16");
  }
  if (static_cast<std::size_t>(entry_count) > decoder.remaining() / 12u) {
    return Status(common::StatusCode::kPayloadDecodeError,
                  "entry_count exceeds remaining payload");
  }
  for (std::uint32_t i = 0; i < entry_count; ++i) {
    OffsetCommitEntry entry;
    status = decoder.ReadU32(&entry.partition);
    if (!status.ok()) {
      return status;
    }
    status = decoder.ReadU64(&entry.committed_offset);
    if (!status.ok()) {
      return status;
    }
    result.entries.push_back(std::move(entry));
  }
  status = decoder.Finish();
  if (!status.ok()) {
    return status;
  }
  *request = std::move(result);
  return Status();
}

Status EncodeOffsetCommitResponse(const OffsetCommitResponse& response,
                                  std::string* payload) {
  if (payload == nullptr) {
    return Status(common::StatusCode::kInternalError, "null payload output");
  }
  std::string buffer;
  Encoder encoder(buffer);
  Status status =
      EncodeResponsePrefix(response.error_code, response.message, &encoder);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU32(response.applied_count);
  if (!status.ok()) {
    return status;
  }
  *payload = std::move(buffer);
  return Status();
}

Status DecodeOffsetCommitResponse(const std::string& payload,
                                  OffsetCommitResponse* response) {
  if (response == nullptr) {
    return Status(common::StatusCode::kInternalError, "null output");
  }
  Decoder decoder(payload);
  OffsetCommitResponse result;
  Status status =
      DecodeResponsePrefix(&decoder, &result.error_code, &result.message);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadU32(&result.applied_count);
  if (!status.ok()) {
    return status;
  }
  status = decoder.Finish();
  if (!status.ok()) {
    return status;
  }
  *response = std::move(result);
  return Status();
}

Status EncodeReplayRequest(const ReplayRequest& request, std::string* payload) {
  if (payload == nullptr) {
    return Status(common::StatusCode::kInternalError, "null payload output");
  }
  std::string buffer;
  Encoder encoder(buffer);
  Status status = encoder.WriteStr(request.topic);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU32(request.partition);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU64(request.offset);
  if (!status.ok()) {
    return status;
  }
  status = encoder.WriteU32(request.count);
  if (!status.ok()) {
    return status;
  }
  *payload = std::move(buffer);
  return Status();
}

Status DecodeReplayRequest(const std::string& payload,
                           ReplayRequest* request) {
  if (request == nullptr) {
    return Status(common::StatusCode::kInternalError, "null output");
  }
  Decoder decoder(payload);
  ReplayRequest result;
  Status status = decoder.ReadStr(&result.topic);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadU32(&result.partition);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadU64(&result.offset);
  if (!status.ok()) {
    return status;
  }
  status = decoder.ReadU32(&result.count);
  if (!status.ok()) {
    return status;
  }
  status = decoder.Finish();
  if (!status.ok()) {
    return status;
  }
  *request = std::move(result);
  return Status();
}

Status EncodeReplayResponse(const ReplayResponse& response,
                            std::string* payload) {
  return EncodeFetchResponse(response, payload);
}

Status DecodeReplayResponse(const std::string& payload,
                            ReplayResponse* response) {
  return DecodeFetchResponse(payload, response);
}

}  // namespace protocol
}  // namespace minikafka
