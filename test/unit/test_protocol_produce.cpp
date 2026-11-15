#include "test_common.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "minikafka/protocol/codec.h"
#include "minikafka/protocol/messages.h"
#include "test_protocol_util.h"

namespace {

using minikafka::common::StatusCode;
using minikafka::protocol::Encoder;
using minikafka::protocol::ProduceEntry;
using minikafka::protocol::ProduceRequest;
using minikafka::protocol::ProduceResponse;
using minikafka::protocol::ProduceResult;
using mk_protocol_test::ExpectOk;
using mk_protocol_test::ExpectStatus;

void AppendOkPrefix(std::string* payload) {
  Encoder encoder(*payload);
  ExpectOk(minikafka::protocol::EncodeResponsePrefix(StatusCode::kOk, "",
                                                    &encoder));
}

void AppendResult(Encoder* encoder, std::uint32_t index,
                  std::uint8_t status, StatusCode error_code,
                  std::uint64_t offset) {
  ExpectOk(encoder->WriteU32(index));
  ExpectOk(encoder->WriteU8(status));
  ExpectOk(encoder->WriteU16(static_cast<std::uint16_t>(error_code)));
  ExpectOk(encoder->WriteU64(offset));
}

std::string InvalidUtf8() {
  std::string value;
  value.push_back(static_cast<char>(0xC3));
  value.push_back('(');
  return value;
}

void TestProduceRequest() {
  ProduceRequest request;
  request.topic = "orders";
  request.partition = 1u;

  ProduceEntry empty_key;
  empty_key.flags = 1u;
  empty_key.value = "v";
  ProduceEntry no_key;
  no_key.flags = 0u;
  no_key.value = "";
  ProduceEntry with_key;
  with_key.flags = 1u;
  with_key.key = "k";
  with_key.value = "hello";
  request.entries.push_back(empty_key);
  request.entries.push_back(no_key);
  request.entries.push_back(with_key);

  std::string payload;
  ExpectOk(minikafka::protocol::EncodeProduceRequest(request, &payload));
  ProduceRequest decoded;
  ExpectOk(minikafka::protocol::DecodeProduceRequest(payload, &decoded));
  MK_EXPECT_EQ(decoded.topic, request.topic);
  MK_EXPECT_EQ(decoded.partition, request.partition);
  MK_EXPECT_EQ(decoded.entries.size(), static_cast<std::size_t>(3));
  MK_EXPECT_EQ(decoded.entries[0].flags, 1);
  MK_EXPECT_EQ(decoded.entries[0].key, std::string());
  MK_EXPECT_EQ(decoded.entries[0].value, std::string("v"));
  MK_EXPECT_EQ(decoded.entries[1].flags, 0);
  MK_EXPECT_EQ(decoded.entries[1].key, std::string());
  MK_EXPECT_EQ(decoded.entries[2].key, std::string("k"));
  MK_EXPECT_EQ(decoded.entries[2].value, std::string("hello"));

  ProduceRequest bad_flags = request;
  bad_flags.entries[0].flags = 2u;
  ExpectStatus(minikafka::protocol::EncodeProduceRequest(bad_flags, &payload),
               StatusCode::kInternalError);

  ProduceRequest missing_key_flag = request;
  missing_key_flag.entries[1].key = "k";
  ExpectStatus(
      minikafka::protocol::EncodeProduceRequest(missing_key_flag, &payload),
      StatusCode::kInternalError);

  std::string bad_flags_payload;
  Encoder bad_flags_encoder(bad_flags_payload);
  ExpectOk(bad_flags_encoder.WriteStr("orders"));
  ExpectOk(bad_flags_encoder.WriteU32(1u));
  ExpectOk(bad_flags_encoder.WriteU32(1u));
  ExpectOk(bad_flags_encoder.WriteU16(2u));
  ExpectOk(bad_flags_encoder.WriteU32(0u));
  ExpectOk(bad_flags_encoder.WriteU32(0u));
  ExpectStatus(
      minikafka::protocol::DecodeProduceRequest(bad_flags_payload, &decoded),
      StatusCode::kPayloadDecodeError);

  std::string missing_key_payload;
  Encoder missing_key_encoder(missing_key_payload);
  ExpectOk(missing_key_encoder.WriteStr("orders"));
  ExpectOk(missing_key_encoder.WriteU32(1u));
  ExpectOk(missing_key_encoder.WriteU32(1u));
  ExpectOk(missing_key_encoder.WriteU16(0u));
  ExpectOk(missing_key_encoder.WriteBlob("k"));
  ExpectOk(missing_key_encoder.WriteU32(0u));
  ExpectStatus(
      minikafka::protocol::DecodeProduceRequest(missing_key_payload, &decoded),
      StatusCode::kPayloadDecodeError);

  std::string zero_entries_payload;
  Encoder zero_entries_encoder(zero_entries_payload);
  ExpectOk(zero_entries_encoder.WriteStr("orders"));
  ExpectOk(zero_entries_encoder.WriteU32(0u));
  ExpectOk(zero_entries_encoder.WriteU32(0u));
  ExpectStatus(minikafka::protocol::DecodeProduceRequest(zero_entries_payload,
                                                         &decoded),
               StatusCode::kInvalidBatch);

  std::string too_many_entries_payload;
  Encoder too_many_encoder(too_many_entries_payload);
  ExpectOk(too_many_encoder.WriteStr("orders"));
  ExpectOk(too_many_encoder.WriteU32(0u));
  ExpectOk(too_many_encoder.WriteU32(1025u));
  ExpectStatus(minikafka::protocol::DecodeProduceRequest(
                   too_many_entries_payload, &decoded),
               StatusCode::kInvalidBatch);

  ProduceRequest empty_entries = request;
  empty_entries.entries.clear();
  ExpectStatus(
      minikafka::protocol::EncodeProduceRequest(empty_entries, &payload),
      StatusCode::kInvalidBatch);

  const std::size_t oversized_value_size =
      minikafka::protocol::kMaxRecordSize - 24u + 1u;
  ProduceRequest oversized = request;
  oversized.entries.clear();
  ProduceEntry oversized_entry;
  oversized_entry.flags = 1u;
  oversized_entry.value.assign(oversized_value_size, 'a');
  oversized.entries.push_back(oversized_entry);
  ExpectStatus(
      minikafka::protocol::EncodeProduceRequest(oversized, &payload),
      StatusCode::kMessageTooLarge);

  std::string oversized_payload;
  Encoder oversized_encoder(oversized_payload);
  ExpectOk(oversized_encoder.WriteStr("orders"));
  ExpectOk(oversized_encoder.WriteU32(0u));
  ExpectOk(oversized_encoder.WriteU32(1u));
  ExpectOk(oversized_encoder.WriteU16(1u));
  ExpectOk(oversized_encoder.WriteU32(0u));
  ExpectOk(oversized_encoder.WriteUtf8Blob(oversized_entry.value));
  ExpectStatus(minikafka::protocol::DecodeProduceRequest(oversized_payload,
                                                         &decoded),
               StatusCode::kMessageTooLarge);

  std::string invalid_utf8_payload;
  Encoder invalid_utf8_encoder(invalid_utf8_payload);
  ExpectOk(invalid_utf8_encoder.WriteStr("orders"));
  ExpectOk(invalid_utf8_encoder.WriteU32(0u));
  ExpectOk(invalid_utf8_encoder.WriteU32(1u));
  ExpectOk(invalid_utf8_encoder.WriteU16(1u));
  ExpectOk(invalid_utf8_encoder.WriteBlob(InvalidUtf8()));
  ExpectOk(invalid_utf8_encoder.WriteU32(0u));
  ExpectStatus(minikafka::protocol::DecodeProduceRequest(
                   invalid_utf8_payload, &decoded),
               StatusCode::kInvalidUtf8);

  std::string trailing_payload;
  ExpectOk(minikafka::protocol::EncodeProduceRequest(request,
                                                      &trailing_payload));
  trailing_payload.push_back('\0');
  ExpectStatus(minikafka::protocol::DecodeProduceRequest(trailing_payload,
                                                         &decoded),
               StatusCode::kPayloadDecodeError);
}

void TestProduceResponse() {
  ProduceResponse response;
  response.status = 1u;
  ProduceResult appended;
  appended.index = 0u;
  appended.status = 0u;
  appended.error_code = StatusCode::kOk;
  appended.offset = 10u;
  ProduceResult failed;
  failed.index = 1u;
  failed.status = 1u;
  failed.error_code = StatusCode::kDiskFull;
  failed.offset = 0u;
  ProduceResult blocked;
  blocked.index = 2u;
  blocked.status = 2u;
  blocked.error_code = StatusCode::kPartitionBlocked;
  blocked.offset = 0u;
  response.results.push_back(appended);
  response.results.push_back(failed);
  response.results.push_back(blocked);

  std::string payload;
  ExpectOk(minikafka::protocol::EncodeProduceResponse(response, &payload));
  ProduceResponse decoded;
  ExpectOk(minikafka::protocol::DecodeProduceResponse(payload, &decoded));
  MK_EXPECT_EQ(decoded.status, 1);
  MK_EXPECT_EQ(decoded.results.size(), static_cast<std::size_t>(3));
  MK_EXPECT_EQ(decoded.results[0].index, 0u);
  MK_EXPECT_EQ(decoded.results[0].status, 0);
  MK_EXPECT_EQ(decoded.results[0].offset, 10u);
  MK_EXPECT_EQ(decoded.results[1].index, 1u);
  MK_EXPECT_EQ(decoded.results[1].error_code, StatusCode::kDiskFull);
  MK_EXPECT_EQ(decoded.results[1].offset, 0u);
  MK_EXPECT_EQ(decoded.results[2].index, 2u);
  MK_EXPECT_EQ(decoded.results[2].error_code,
               StatusCode::kPartitionBlocked);

  ProduceResponse all_ok;
  all_ok.status = 0u;
  all_ok.results.push_back(appended);
  ExpectOk(minikafka::protocol::EncodeProduceResponse(all_ok, &payload));
  ExpectOk(minikafka::protocol::DecodeProduceResponse(payload, &decoded));
  MK_EXPECT_EQ(decoded.status, 0);

  ProduceResponse request_failed;
  request_failed.error_code = StatusCode::kDiskFull;
  request_failed.message = "no space";
  request_failed.status = 2u;
  ExpectOk(
      minikafka::protocol::EncodeProduceResponse(request_failed, &payload));
  ExpectOk(minikafka::protocol::DecodeProduceResponse(payload, &decoded));
  MK_EXPECT_EQ(decoded.error_code, StatusCode::kDiskFull);
  MK_EXPECT_EQ(decoded.message, std::string("no space"));
  MK_EXPECT_EQ(decoded.status, 2);
  MK_EXPECT_TRUE(decoded.results.empty());

  std::string bad_order;
  AppendOkPrefix(&bad_order);
  Encoder bad_order_encoder(bad_order);
  ExpectOk(bad_order_encoder.WriteU8(0u));
  ExpectOk(bad_order_encoder.WriteU32(2u));
  AppendResult(&bad_order_encoder, 1u, 0u, StatusCode::kOk, 10u);
  AppendResult(&bad_order_encoder, 0u, 0u, StatusCode::kOk, 11u);
  ExpectStatus(
      minikafka::protocol::DecodeProduceResponse(bad_order, &decoded),
      StatusCode::kPayloadDecodeError);

  std::string bad_blocked;
  AppendOkPrefix(&bad_blocked);
  Encoder bad_blocked_encoder(bad_blocked);
  ExpectOk(bad_blocked_encoder.WriteU8(2u));
  ExpectOk(bad_blocked_encoder.WriteU32(1u));
  AppendResult(&bad_blocked_encoder, 0u, 2u, StatusCode::kDiskFull, 0u);
  ExpectStatus(
      minikafka::protocol::DecodeProduceResponse(bad_blocked, &decoded),
      StatusCode::kPayloadDecodeError);

  std::string bad_failed_offset;
  AppendOkPrefix(&bad_failed_offset);
  Encoder bad_failed_offset_encoder(bad_failed_offset);
  ExpectOk(bad_failed_offset_encoder.WriteU8(1u));
  ExpectOk(bad_failed_offset_encoder.WriteU32(2u));
  AppendResult(&bad_failed_offset_encoder, 0u, 0u, StatusCode::kOk, 1u);
  AppendResult(&bad_failed_offset_encoder, 1u, 1u, StatusCode::kDiskFull, 5u);
  ExpectStatus(
      minikafka::protocol::DecodeProduceResponse(bad_failed_offset, &decoded),
      StatusCode::kPayloadDecodeError);

  std::string bad_response_status;
  AppendOkPrefix(&bad_response_status);
  Encoder bad_response_status_encoder(bad_response_status);
  ExpectOk(bad_response_status_encoder.WriteU8(0u));
  ExpectOk(bad_response_status_encoder.WriteU32(1u));
  AppendResult(&bad_response_status_encoder, 0u, 1u, StatusCode::kDiskFull,
               0u);
  ExpectStatus(minikafka::protocol::DecodeProduceResponse(bad_response_status,
                                                          &decoded),
               StatusCode::kPayloadDecodeError);

  std::string unknown_result_code;
  AppendOkPrefix(&unknown_result_code);
  Encoder unknown_result_encoder(unknown_result_code);
  ExpectOk(unknown_result_encoder.WriteU8(1u));
  ExpectOk(unknown_result_encoder.WriteU32(2u));
  AppendResult(&unknown_result_encoder, 0u, 0u, StatusCode::kOk, 1u);
  ExpectOk(unknown_result_encoder.WriteU32(1u));
  ExpectOk(unknown_result_encoder.WriteU8(1u));
  ExpectOk(unknown_result_encoder.WriteU16(0x1234u));
  ExpectOk(unknown_result_encoder.WriteU64(0u));
  ExpectStatus(
      minikafka::protocol::DecodeProduceResponse(unknown_result_code, &decoded),
      StatusCode::kPayloadDecodeError);

  std::string truncated_results;
  AppendOkPrefix(&truncated_results);
  Encoder truncated_encoder(truncated_results);
  ExpectOk(truncated_encoder.WriteU8(0u));
  ExpectOk(truncated_encoder.WriteU32(2u));
  AppendResult(&truncated_encoder, 0u, 0u, StatusCode::kOk, 1u);
  ExpectStatus(
      minikafka::protocol::DecodeProduceResponse(truncated_results, &decoded),
      StatusCode::kPayloadDecodeError);

  std::string too_many_results;
  AppendOkPrefix(&too_many_results);
  Encoder too_many_results_encoder(too_many_results);
  ExpectOk(too_many_results_encoder.WriteU8(0u));
  ExpectOk(too_many_results_encoder.WriteU32(1025u));
  ExpectStatus(
      minikafka::protocol::DecodeProduceResponse(too_many_results, &decoded),
      StatusCode::kPayloadDecodeError);

  ProduceResponse invalid_encode;
  invalid_encode.status = 3u;
  ExpectStatus(
      minikafka::protocol::EncodeProduceResponse(invalid_encode, &payload),
      StatusCode::kInternalError);

  ProduceResponse invalid_appended;
  invalid_appended.status = 0u;
  ProduceResult invalid_appended_result;
  invalid_appended_result.index = 0u;
  invalid_appended_result.status = 0u;
  invalid_appended_result.error_code = StatusCode::kDiskFull;
  invalid_appended.results.push_back(invalid_appended_result);
  ExpectStatus(
      minikafka::protocol::EncodeProduceResponse(invalid_appended, &payload),
      StatusCode::kInternalError);

  ProduceResponse status_two_with_appended;
  status_two_with_appended.status = 2u;
  status_two_with_appended.results.push_back(appended);
  ExpectStatus(minikafka::protocol::EncodeProduceResponse(
                   status_two_with_appended, &payload),
               StatusCode::kInternalError);

  ProduceResponse partial_with_only_appended;
  partial_with_only_appended.status = 1u;
  partial_with_only_appended.results.push_back(appended);
  ExpectStatus(minikafka::protocol::EncodeProduceResponse(
                   partial_with_only_appended, &payload),
               StatusCode::kInternalError);
}

}  // namespace

void TestProduce() {
  TestProduceRequest();
  TestProduceResponse();
}
