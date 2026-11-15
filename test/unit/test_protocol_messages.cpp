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
using minikafka::protocol::CreateTopicRequest;
using minikafka::protocol::CreateTopicResponse;
using minikafka::protocol::Decoder;
using minikafka::protocol::Encoder;
using minikafka::protocol::FetchRecord;
using minikafka::protocol::FetchRequest;
using minikafka::protocol::FetchResponse;
using minikafka::protocol::ListTopicsResponse;
using minikafka::protocol::OffsetCommitEntry;
using minikafka::protocol::OffsetCommitRequest;
using minikafka::protocol::OffsetCommitResponse;
using minikafka::protocol::OffsetFetchEntry;
using minikafka::protocol::OffsetFetchRequest;
using minikafka::protocol::OffsetFetchResponse;
using minikafka::protocol::ReplayRequest;
using minikafka::protocol::ReplayResponse;
using minikafka::protocol::TopicInfo;
using mk_protocol_test::ExpectOk;
using mk_protocol_test::ExpectStatus;

void AppendOkPrefix(std::string* payload) {
  Encoder encoder(*payload);
  ExpectOk(minikafka::protocol::EncodeResponsePrefix(StatusCode::kOk, "",
                                                    &encoder));
}

std::string InvalidUtf8() {
  std::string value;
  value.push_back(static_cast<char>(0xC3));
  value.push_back('(');
  return value;
}

void TestResponsePrefix() {
  std::string payload;
  Encoder encoder(payload);
  ExpectOk(minikafka::protocol::EncodeResponsePrefix(StatusCode::kOk, "",
                                                    &encoder));
  MK_EXPECT_EQ(payload.size(), static_cast<std::size_t>(2));

  Decoder decoder(payload);
  StatusCode code = StatusCode::kDeliveryFailed;
  std::string message = "old";
  ExpectOk(minikafka::protocol::DecodeResponsePrefix(&decoder, &code, &message));
  MK_EXPECT_EQ(code, StatusCode::kOk);
  MK_EXPECT_TRUE(message.empty());
  ExpectOk(decoder.Finish());

  std::string error_payload;
  Encoder error_encoder(error_payload);
  ExpectOk(minikafka::protocol::EncodeResponsePrefix(
      StatusCode::kDiskFull, "no space", &error_encoder));
  Decoder error_decoder(error_payload);
  ExpectOk(
      minikafka::protocol::DecodeResponsePrefix(&error_decoder, &code, &message));
  MK_EXPECT_EQ(code, StatusCode::kDiskFull);
  MK_EXPECT_EQ(message, std::string("no space"));
  ExpectOk(error_decoder.Finish());

  std::string ok_message_payload;
  Encoder ok_message_encoder(ok_message_payload);
  ExpectStatus(minikafka::protocol::EncodeResponsePrefix(
                   StatusCode::kOk, "unexpected", &ok_message_encoder),
               StatusCode::kInternalError);

  std::string usage_payload;
  Encoder usage_encoder(usage_payload);
  ExpectStatus(minikafka::protocol::EncodeResponsePrefix(
                   StatusCode::kUsage, "", &usage_encoder),
               StatusCode::kInternalError);

  std::string unknown_wire;
  Encoder unknown_encoder(unknown_wire);
  ExpectOk(unknown_encoder.WriteU16(0x1234u));
  Decoder unknown_decoder(unknown_wire);
  ExpectStatus(minikafka::protocol::DecodeResponsePrefix(
                   &unknown_decoder, &code, &message),
               StatusCode::kPayloadDecodeError);

  std::string usage_wire;
  Encoder usage_wire_encoder(usage_wire);
  ExpectOk(usage_wire_encoder.WriteU16(0xFFFFu));
  Decoder usage_wire_decoder(usage_wire);
  ExpectStatus(minikafka::protocol::DecodeResponsePrefix(
                   &usage_wire_decoder, &code, &message),
               StatusCode::kPayloadDecodeError);

  const std::string long_message(513u, 'a');
  std::string long_message_payload;
  Encoder long_message_encoder(long_message_payload);
  ExpectStatus(minikafka::protocol::EncodeResponsePrefix(
                   StatusCode::kDiskFull, long_message, &long_message_encoder),
               StatusCode::kInternalError);

  std::string long_wire;
  Encoder long_wire_encoder(long_wire);
  ExpectOk(long_wire_encoder.WriteU16(
      static_cast<std::uint16_t>(StatusCode::kDiskFull)));
  ExpectOk(long_wire_encoder.WriteU32(513u));
  long_wire.append(513u, 'a');
  Decoder long_wire_decoder(long_wire);
  ExpectStatus(minikafka::protocol::DecodeResponsePrefix(
                   &long_wire_decoder, &code, &message),
               StatusCode::kPayloadDecodeError);

  std::string bad_message_wire;
  Encoder bad_message_encoder(bad_message_wire);
  ExpectOk(bad_message_encoder.WriteU16(
      static_cast<std::uint16_t>(StatusCode::kDiskFull)));
  ExpectOk(bad_message_encoder.WriteBlob(InvalidUtf8()));
  Decoder bad_message_decoder(bad_message_wire);
  ExpectStatus(minikafka::protocol::DecodeResponsePrefix(
                   &bad_message_decoder, &code, &message),
               StatusCode::kInvalidUtf8);
}

void TestCreateAndListTopics() {
  CreateTopicRequest request;
  request.topic = "orders";
  request.partition_count = 4u;
  std::string payload;
  ExpectOk(minikafka::protocol::EncodeCreateTopicRequest(request, &payload));
  CreateTopicRequest decoded_request;
  ExpectOk(
      minikafka::protocol::DecodeCreateTopicRequest(payload, &decoded_request));
  MK_EXPECT_EQ(decoded_request.topic, request.topic);
  MK_EXPECT_EQ(decoded_request.partition_count, request.partition_count);

  CreateTopicRequest invalid_request = request;
  invalid_request.partition_count = 0u;
  ExpectStatus(minikafka::protocol::EncodeCreateTopicRequest(invalid_request,
                                                             &payload),
               StatusCode::kInvalidPartitions);

  std::string invalid_count_payload;
  Encoder invalid_count_encoder(invalid_count_payload);
  ExpectOk(invalid_count_encoder.WriteStr("orders"));
  ExpectOk(invalid_count_encoder.WriteU32(17u));
  ExpectStatus(minikafka::protocol::DecodeCreateTopicRequest(
                   invalid_count_payload, &decoded_request),
               StatusCode::kInvalidPartitions);

  std::string bad_topic_payload;
  Encoder bad_topic_encoder(bad_topic_payload);
  ExpectOk(bad_topic_encoder.WriteBlob(InvalidUtf8()));
  ExpectOk(bad_topic_encoder.WriteU32(4u));
  ExpectStatus(minikafka::protocol::DecodeCreateTopicRequest(
                   bad_topic_payload, &decoded_request),
               StatusCode::kInvalidUtf8);

  std::string trailing_payload;
  ExpectOk(minikafka::protocol::EncodeCreateTopicRequest(request,
                                                         &trailing_payload));
  trailing_payload.push_back('\0');
  ExpectStatus(minikafka::protocol::DecodeCreateTopicRequest(
                   trailing_payload, &decoded_request),
               StatusCode::kPayloadDecodeError);

  CreateTopicResponse response;
  response.error_code = StatusCode::kTopicExists;
  response.message = "exists";
  response.topic = "orders";
  response.partition_count = 4u;
  std::string response_payload;
  ExpectOk(
      minikafka::protocol::EncodeCreateTopicResponse(response, &response_payload));
  CreateTopicResponse decoded_response;
  ExpectOk(minikafka::protocol::DecodeCreateTopicResponse(
      response_payload, &decoded_response));
  MK_EXPECT_EQ(decoded_response.error_code, response.error_code);
  MK_EXPECT_EQ(decoded_response.message, response.message);
  MK_EXPECT_EQ(decoded_response.topic, response.topic);
  MK_EXPECT_EQ(decoded_response.partition_count, response.partition_count);

  CreateTopicResponse ok_response;
  ok_response.topic = "orders";
  ok_response.partition_count = 4u;
  std::string ok_response_payload;
  ExpectOk(minikafka::protocol::EncodeCreateTopicResponse(ok_response,
                                                           &ok_response_payload));
  CreateTopicResponse decoded_ok_response;
  ExpectOk(minikafka::protocol::DecodeCreateTopicResponse(
      ok_response_payload, &decoded_ok_response));
  MK_EXPECT_EQ(decoded_ok_response.error_code, StatusCode::kOk);
  MK_EXPECT_TRUE(decoded_ok_response.message.empty());
  MK_EXPECT_EQ(decoded_ok_response.topic, ok_response.topic);

  CreateTopicResponse ok_with_message;
  ok_with_message.message = "unexpected";
  ExpectStatus(minikafka::protocol::EncodeCreateTopicResponse(
                   ok_with_message, &response_payload),
               StatusCode::kInternalError);

  std::string unknown_response;
  Encoder unknown_response_encoder(unknown_response);
  ExpectOk(unknown_response_encoder.WriteU16(0x1234u));
  ExpectOk(unknown_response_encoder.WriteStr("orders"));
  ExpectOk(unknown_response_encoder.WriteU32(4u));
  ExpectStatus(minikafka::protocol::DecodeCreateTopicResponse(
                   unknown_response, &decoded_response),
               StatusCode::kPayloadDecodeError);

  std::string list_request;
  ExpectOk(minikafka::protocol::EncodeListTopicsRequest(&list_request));
  MK_EXPECT_TRUE(list_request.empty());
  ExpectOk(minikafka::protocol::DecodeListTopicsRequest(list_request));
  std::string nonempty_list_request = "x";
  ExpectStatus(minikafka::protocol::DecodeListTopicsRequest(
                   nonempty_list_request),
               StatusCode::kPayloadDecodeError);

  ListTopicsResponse list_response;
  TopicInfo first;
  first.topic = "orders";
  first.partition_count = 4u;
  TopicInfo second;
  second.topic = "events";
  second.partition_count = 16u;
  list_response.entries.push_back(first);
  list_response.entries.push_back(second);
  std::string list_payload;
  ExpectOk(
      minikafka::protocol::EncodeListTopicsResponse(list_response, &list_payload));
  ListTopicsResponse decoded_list;
  ExpectOk(minikafka::protocol::DecodeListTopicsResponse(
      list_payload, &decoded_list));
  MK_EXPECT_EQ(decoded_list.entries.size(),
               static_cast<std::size_t>(2));
  MK_EXPECT_EQ(decoded_list.entries[0].topic, std::string("orders"));
  MK_EXPECT_EQ(decoded_list.entries[1].partition_count, 16u);

  list_payload.push_back('\0');
  ExpectStatus(minikafka::protocol::DecodeListTopicsResponse(
                   list_payload, &decoded_list),
               StatusCode::kPayloadDecodeError);
}

void TestFetch() {
  FetchRequest request;
  request.topic = "orders";
  request.partition = 1u;
  request.offset = 5u;
  request.max_bytes = 0u;
  std::string payload;
  ExpectOk(minikafka::protocol::EncodeFetchRequest(request, &payload));
  FetchRequest decoded_request;
  ExpectOk(minikafka::protocol::DecodeFetchRequest(payload, &decoded_request));
  MK_EXPECT_EQ(decoded_request.topic, request.topic);
  MK_EXPECT_EQ(decoded_request.partition, request.partition);
  MK_EXPECT_EQ(decoded_request.offset, request.offset);
  MK_EXPECT_EQ(decoded_request.max_bytes, request.max_bytes);

  request.max_bytes = minikafka::protocol::kMaxFetchBytes;
  ExpectOk(minikafka::protocol::EncodeFetchRequest(request, &payload));
  ExpectOk(minikafka::protocol::DecodeFetchRequest(payload, &decoded_request));

  request.max_bytes = minikafka::protocol::kMaxFetchBytes + 1u;
  ExpectStatus(minikafka::protocol::EncodeFetchRequest(request, &payload),
               StatusCode::kInternalError);

  std::string too_large_payload;
  Encoder too_large_encoder(too_large_payload);
  ExpectOk(too_large_encoder.WriteStr("orders"));
  ExpectOk(too_large_encoder.WriteU32(1u));
  ExpectOk(too_large_encoder.WriteU64(5u));
  ExpectOk(too_large_encoder.WriteU32(
      minikafka::protocol::kMaxFetchBytes + 1u));
  ExpectStatus(minikafka::protocol::DecodeFetchRequest(
                   too_large_payload, &decoded_request),
               StatusCode::kPayloadDecodeError);

  FetchResponse response;
  response.topic = "orders";
  response.partition = 1u;
  response.log_end_offset = 10u;
  FetchRecord first;
  first.offset = 5u;
  first.flags = 1u;
  first.key = "k";
  first.value = "v";
  FetchRecord second;
  second.offset = 6u;
  second.flags = 0u;
  second.value = "";
  response.records.push_back(first);
  response.records.push_back(second);
  std::string response_payload;
  ExpectOk(
      minikafka::protocol::EncodeFetchResponse(response, &response_payload));
  FetchResponse decoded_response;
  ExpectOk(minikafka::protocol::DecodeFetchResponse(
      response_payload, &decoded_response));
  MK_EXPECT_EQ(decoded_response.topic, response.topic);
  MK_EXPECT_EQ(decoded_response.partition, response.partition);
  MK_EXPECT_EQ(decoded_response.log_end_offset, response.log_end_offset);
  MK_EXPECT_EQ(decoded_response.records.size(),
               static_cast<std::size_t>(2));
  MK_EXPECT_EQ(decoded_response.records[0].key, std::string("k"));
  MK_EXPECT_EQ(decoded_response.records[0].flags, 1);
  MK_EXPECT_EQ(decoded_response.records[1].key, std::string());
  MK_EXPECT_EQ(decoded_response.records[1].value, std::string());

  std::string bad_flags_response;
  AppendOkPrefix(&bad_flags_response);
  Encoder bad_flags_encoder(bad_flags_response);
  ExpectOk(bad_flags_encoder.WriteStr("orders"));
  ExpectOk(bad_flags_encoder.WriteU32(1u));
  ExpectOk(bad_flags_encoder.WriteU64(10u));
  ExpectOk(bad_flags_encoder.WriteU32(1u));
  ExpectOk(bad_flags_encoder.WriteU64(5u));
  ExpectOk(bad_flags_encoder.WriteU16(2u));
  ExpectOk(bad_flags_encoder.WriteU32(0u));
  ExpectOk(bad_flags_encoder.WriteU32(0u));
  ExpectStatus(minikafka::protocol::DecodeFetchResponse(
                   bad_flags_response, &decoded_response),
               StatusCode::kPayloadDecodeError);

  std::string missing_key_flag_response;
  AppendOkPrefix(&missing_key_flag_response);
  Encoder missing_key_encoder(missing_key_flag_response);
  ExpectOk(missing_key_encoder.WriteStr("orders"));
  ExpectOk(missing_key_encoder.WriteU32(1u));
  ExpectOk(missing_key_encoder.WriteU64(10u));
  ExpectOk(missing_key_encoder.WriteU32(1u));
  ExpectOk(missing_key_encoder.WriteU64(5u));
  ExpectOk(missing_key_encoder.WriteU16(0u));
  ExpectOk(missing_key_encoder.WriteBlob("k"));
  ExpectOk(missing_key_encoder.WriteU32(0u));
  ExpectStatus(minikafka::protocol::DecodeFetchResponse(
                   missing_key_flag_response, &decoded_response),
               StatusCode::kPayloadDecodeError);

  std::string invalid_utf8_response;
  AppendOkPrefix(&invalid_utf8_response);
  Encoder invalid_utf8_encoder(invalid_utf8_response);
  ExpectOk(invalid_utf8_encoder.WriteStr("orders"));
  ExpectOk(invalid_utf8_encoder.WriteU32(1u));
  ExpectOk(invalid_utf8_encoder.WriteU64(10u));
  ExpectOk(invalid_utf8_encoder.WriteU32(1u));
  ExpectOk(invalid_utf8_encoder.WriteU64(5u));
  ExpectOk(invalid_utf8_encoder.WriteU16(1u));
  ExpectOk(invalid_utf8_encoder.WriteBlob("k"));
  ExpectOk(invalid_utf8_encoder.WriteBlob(InvalidUtf8()));
  ExpectStatus(minikafka::protocol::DecodeFetchResponse(
                   invalid_utf8_response, &decoded_response),
               StatusCode::kInvalidUtf8);
}

void TestOffsetsAndReplay() {
  OffsetFetchRequest fetch_request;
  fetch_request.group = "g1";
  fetch_request.topic = "orders";
  fetch_request.consumer_index = 1u;
  fetch_request.consumer_count = 2u;
  std::string payload;
  ExpectOk(
      minikafka::protocol::EncodeOffsetFetchRequest(fetch_request, &payload));
  OffsetFetchRequest decoded_fetch_request;
  ExpectOk(minikafka::protocol::DecodeOffsetFetchRequest(
      payload, &decoded_fetch_request));
  MK_EXPECT_EQ(decoded_fetch_request.group, fetch_request.group);
  MK_EXPECT_EQ(decoded_fetch_request.topic, fetch_request.topic);
  MK_EXPECT_EQ(decoded_fetch_request.consumer_index,
               fetch_request.consumer_index);
  MK_EXPECT_EQ(decoded_fetch_request.consumer_count,
               fetch_request.consumer_count);

  OffsetFetchRequest invalid_slot = fetch_request;
  invalid_slot.consumer_index = 2u;
  ExpectStatus(minikafka::protocol::EncodeOffsetFetchRequest(invalid_slot,
                                                             &payload),
               StatusCode::kInvalidConsumerSlot);

  std::string invalid_slot_payload;
  Encoder invalid_slot_encoder(invalid_slot_payload);
  ExpectOk(invalid_slot_encoder.WriteStr("g1"));
  ExpectOk(invalid_slot_encoder.WriteStr("orders"));
  ExpectOk(invalid_slot_encoder.WriteU32(2u));
  ExpectOk(invalid_slot_encoder.WriteU32(2u));
  ExpectStatus(minikafka::protocol::DecodeOffsetFetchRequest(
                   invalid_slot_payload, &decoded_fetch_request),
               StatusCode::kInvalidConsumerSlot);

  OffsetFetchResponse fetch_response;
  fetch_response.assigned_partitions.push_back(0u);
  fetch_response.assigned_partitions.push_back(2u);
  OffsetFetchEntry first_entry;
  first_entry.partition = 0u;
  first_entry.has_committed = true;
  first_entry.committed_offset = 5u;
  OffsetFetchEntry second_entry;
  second_entry.partition = 2u;
  second_entry.has_committed = false;
  second_entry.committed_offset = 0u;
  fetch_response.entries.push_back(first_entry);
  fetch_response.entries.push_back(second_entry);
  std::string fetch_response_payload;
  ExpectOk(minikafka::protocol::EncodeOffsetFetchResponse(fetch_response,
                                                          &fetch_response_payload));
  OffsetFetchResponse decoded_fetch_response;
  ExpectOk(minikafka::protocol::DecodeOffsetFetchResponse(
      fetch_response_payload, &decoded_fetch_response));
  MK_EXPECT_EQ(decoded_fetch_response.assigned_partitions.size(),
               static_cast<std::size_t>(2));
  MK_EXPECT_EQ(decoded_fetch_response.entries.size(),
               static_cast<std::size_t>(2));
  MK_EXPECT_TRUE(decoded_fetch_response.entries[0].has_committed);
  MK_EXPECT_EQ(decoded_fetch_response.entries[0].committed_offset, 5u);
  MK_EXPECT_FALSE(decoded_fetch_response.entries[1].has_committed);

  std::string mismatched_counts;
  AppendOkPrefix(&mismatched_counts);
  Encoder mismatched_encoder(mismatched_counts);
  ExpectOk(mismatched_encoder.WriteU32(1u));
  ExpectOk(mismatched_encoder.WriteU32(0u));
  ExpectOk(mismatched_encoder.WriteU32(0u));
  ExpectStatus(minikafka::protocol::DecodeOffsetFetchResponse(
                   mismatched_counts, &decoded_fetch_response),
               StatusCode::kPayloadDecodeError);

  std::string mismatched_partition;
  AppendOkPrefix(&mismatched_partition);
  Encoder mismatched_partition_encoder(mismatched_partition);
  ExpectOk(mismatched_partition_encoder.WriteU32(1u));
  ExpectOk(mismatched_partition_encoder.WriteU32(0u));
  ExpectOk(mismatched_partition_encoder.WriteU32(1u));
  ExpectOk(mismatched_partition_encoder.WriteU32(1u));
  ExpectOk(mismatched_partition_encoder.WriteU8(0u));
  ExpectOk(mismatched_partition_encoder.WriteU64(0u));
  ExpectStatus(minikafka::protocol::DecodeOffsetFetchResponse(
                   mismatched_partition, &decoded_fetch_response),
               StatusCode::kPayloadDecodeError);

  std::string bad_has_committed;
  AppendOkPrefix(&bad_has_committed);
  Encoder bad_has_committed_encoder(bad_has_committed);
  ExpectOk(bad_has_committed_encoder.WriteU32(1u));
  ExpectOk(bad_has_committed_encoder.WriteU32(0u));
  ExpectOk(bad_has_committed_encoder.WriteU32(1u));
  ExpectOk(bad_has_committed_encoder.WriteU32(0u));
  ExpectOk(bad_has_committed_encoder.WriteU8(2u));
  ExpectOk(bad_has_committed_encoder.WriteU64(0u));
  ExpectStatus(minikafka::protocol::DecodeOffsetFetchResponse(
                   bad_has_committed, &decoded_fetch_response),
               StatusCode::kPayloadDecodeError);

  OffsetCommitRequest commit_request;
  commit_request.group = "g1";
  commit_request.topic = "orders";
  OffsetCommitEntry commit_entry;
  commit_entry.partition = 0u;
  commit_entry.committed_offset = 5u;
  commit_request.entries.push_back(commit_entry);
  commit_entry.partition = 1u;
  commit_entry.committed_offset = 0u;
  commit_request.entries.push_back(commit_entry);
  std::string commit_payload;
  ExpectOk(minikafka::protocol::EncodeOffsetCommitRequest(commit_request,
                                                          &commit_payload));
  OffsetCommitRequest decoded_commit_request;
  ExpectOk(minikafka::protocol::DecodeOffsetCommitRequest(
      commit_payload, &decoded_commit_request));
  MK_EXPECT_EQ(decoded_commit_request.entries.size(),
               static_cast<std::size_t>(2));
  MK_EXPECT_EQ(decoded_commit_request.entries[0].committed_offset, 5u);

  OffsetCommitRequest too_many_entries = commit_request;
  for (std::uint32_t i = 2u; i < 17u; ++i) {
    OffsetCommitEntry extra;
    extra.partition = i;
    extra.committed_offset = 0u;
    too_many_entries.entries.push_back(extra);
  }
  ExpectStatus(minikafka::protocol::EncodeOffsetCommitRequest(
                   too_many_entries, &commit_payload),
               StatusCode::kInternalError);

  std::string too_many_payload;
  Encoder too_many_encoder(too_many_payload);
  ExpectOk(too_many_encoder.WriteStr("g1"));
  ExpectOk(too_many_encoder.WriteStr("orders"));
  ExpectOk(too_many_encoder.WriteU32(17u));
  ExpectStatus(minikafka::protocol::DecodeOffsetCommitRequest(
                   too_many_payload, &decoded_commit_request),
               StatusCode::kPayloadDecodeError);

  OffsetCommitResponse commit_response;
  commit_response.applied_count = 2u;
  std::string commit_response_payload;
  ExpectOk(minikafka::protocol::EncodeOffsetCommitResponse(
      commit_response, &commit_response_payload));
  OffsetCommitResponse decoded_commit_response;
  ExpectOk(minikafka::protocol::DecodeOffsetCommitResponse(
      commit_response_payload, &decoded_commit_response));
  MK_EXPECT_EQ(decoded_commit_response.applied_count, 2u);

  OffsetCommitResponse error_commit_response;
  error_commit_response.error_code = StatusCode::kStorageIoError;
  error_commit_response.message = "io";
  std::string error_commit_payload;
  ExpectOk(minikafka::protocol::EncodeOffsetCommitResponse(
      error_commit_response, &error_commit_payload));
  OffsetCommitResponse decoded_error_commit;
  ExpectOk(minikafka::protocol::DecodeOffsetCommitResponse(
      error_commit_payload, &decoded_error_commit));
  MK_EXPECT_EQ(decoded_error_commit.error_code, StatusCode::kStorageIoError);
  MK_EXPECT_EQ(decoded_error_commit.message, std::string("io"));

  ReplayRequest replay_request;
  replay_request.topic = "orders";
  replay_request.partition = 0u;
  replay_request.offset = 3u;
  replay_request.count = 2u;
  std::string replay_payload;
  ExpectOk(minikafka::protocol::EncodeReplayRequest(replay_request,
                                                    &replay_payload));
  ReplayRequest decoded_replay_request;
  ExpectOk(minikafka::protocol::DecodeReplayRequest(
      replay_payload, &decoded_replay_request));
  MK_EXPECT_EQ(decoded_replay_request.topic, replay_request.topic);
  MK_EXPECT_EQ(decoded_replay_request.offset, replay_request.offset);
  MK_EXPECT_EQ(decoded_replay_request.count, replay_request.count);

  ReplayResponse replay_response;
  replay_response.topic = "orders";
  replay_response.partition = 0u;
  replay_response.log_end_offset = 10u;
  FetchRecord replay_record;
  replay_record.offset = 3u;
  replay_record.flags = 1u;
  replay_record.key = "k";
  replay_record.value = "v";
  replay_response.records.push_back(replay_record);
  std::string replay_response_payload;
  ExpectOk(minikafka::protocol::EncodeReplayResponse(
      replay_response, &replay_response_payload));
  ReplayResponse decoded_replay_response;
  ExpectOk(minikafka::protocol::DecodeReplayResponse(
      replay_response_payload, &decoded_replay_response));
  MK_EXPECT_EQ(decoded_replay_response.records.size(),
               static_cast<std::size_t>(1));
  MK_EXPECT_EQ(decoded_replay_response.records[0].value, std::string("v"));

  MK_EXPECT_TRUE(minikafka::protocol::IsKnownMessageType(0x0001u));
  MK_EXPECT_TRUE(minikafka::protocol::IsKnownMessageType(0x0007u));
  MK_EXPECT_TRUE(minikafka::protocol::IsKnownMessageType(0x8001u));
  MK_EXPECT_TRUE(minikafka::protocol::IsKnownMessageType(0x8007u));
  MK_EXPECT_FALSE(minikafka::protocol::IsKnownMessageType(0x0000u));
  MK_EXPECT_FALSE(minikafka::protocol::IsKnownMessageType(0x0008u));
  MK_EXPECT_FALSE(minikafka::protocol::IsKnownMessageType(0x8000u));
  MK_EXPECT_FALSE(minikafka::protocol::IsKnownMessageType(0x8008u));
  MK_EXPECT_FALSE(minikafka::protocol::IsKnownMessageType(0xFFFFu));
}

}  // namespace

void TestMessages() {
  TestResponsePrefix();
  TestCreateAndListTopics();
  TestFetch();
  TestOffsetsAndReplay();
}
