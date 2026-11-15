#ifndef MINIKAFKA_PROTOCOL_MESSAGES_H_
#define MINIKAFKA_PROTOCOL_MESSAGES_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "minikafka/common/status.h"
#include "minikafka/protocol/codec.h"

namespace minikafka {
namespace protocol {

using common::Status;
using common::StatusCode;

// 05 §3.1 的 7 种请求与 7 种响应。
enum class MessageType : std::uint16_t {
  kCreateTopic = 0x0001,
  kListTopics = 0x0002,
  kProduce = 0x0003,
  kFetch = 0x0004,
  kOffsetFetch = 0x0005,
  kOffsetCommit = 0x0006,
  kReplay = 0x0007,
  kCreateTopicResponse = 0x8001,
  kListTopicsResponse = 0x8002,
  kProduceResponse = 0x8003,
  kFetchResponse = 0x8004,
  kOffsetFetchResponse = 0x8005,
  kOffsetCommitResponse = 0x8006,
  kReplayResponse = 0x8007,
};

bool IsKnownMessageType(std::uint16_t msg_type);

constexpr std::uint32_t kMaxPartitionCount = 16u;
constexpr std::uint32_t kMaxProduceEntries = 1024u;
constexpr std::uint32_t kMaxFetchBytes = 4u * 1024u * 1024u;
constexpr std::uint32_t kMaxOffsetCommitEntries = 16u;
constexpr std::size_t kMaxRecordSize = 1024u * 1024u;

struct CreateTopicRequest {
  std::string topic;
  std::uint32_t partition_count = 0;
};

struct CreateTopicResponse {
  common::StatusCode error_code = common::StatusCode::kOk;
  std::string message;
  std::string topic;
  std::uint32_t partition_count = 0;
};

struct TopicInfo {
  std::string topic;
  std::uint32_t partition_count = 0;
};

struct ListTopicsResponse {
  common::StatusCode error_code = common::StatusCode::kOk;
  std::string message;
  std::vector<TopicInfo> entries;
};

struct ProduceEntry {
  std::uint16_t flags = 0;
  std::string key;
  std::string value;
};

struct ProduceRequest {
  std::string topic;
  std::uint32_t partition = 0;
  std::vector<ProduceEntry> entries;
};

struct ProduceResult {
  std::uint32_t index = 0;
  std::uint8_t status = 0;
  common::StatusCode error_code = common::StatusCode::kOk;
  std::uint64_t offset = 0;
};

struct ProduceResponse {
  common::StatusCode error_code = common::StatusCode::kOk;
  std::string message;
  std::uint8_t status = 0;
  std::vector<ProduceResult> results;
};

struct FetchRequest {
  std::string topic;
  std::uint32_t partition = 0;
  std::uint64_t offset = 0;
  std::uint32_t max_bytes = 0;
};

struct FetchRecord {
  std::uint64_t offset = 0;
  std::uint16_t flags = 0;
  std::string key;
  std::string value;
};

struct FetchResponse {
  common::StatusCode error_code = common::StatusCode::kOk;
  std::string message;
  std::string topic;
  std::uint32_t partition = 0;
  std::uint64_t log_end_offset = 0;
  std::vector<FetchRecord> records;
};

struct OffsetFetchRequest {
  std::string group;
  std::string topic;
  std::uint32_t consumer_index = 0;
  std::uint32_t consumer_count = 0;
};

struct OffsetFetchEntry {
  std::uint32_t partition = 0;
  bool has_committed = false;
  std::uint64_t committed_offset = 0;
};

struct OffsetFetchResponse {
  common::StatusCode error_code = common::StatusCode::kOk;
  std::string message;
  std::vector<std::uint32_t> assigned_partitions;
  std::vector<OffsetFetchEntry> entries;
};

struct OffsetCommitEntry {
  std::uint32_t partition = 0;
  std::uint64_t committed_offset = 0;
};

struct OffsetCommitRequest {
  std::string group;
  std::string topic;
  std::vector<OffsetCommitEntry> entries;
};

struct OffsetCommitResponse {
  common::StatusCode error_code = common::StatusCode::kOk;
  std::string message;
  std::uint32_t applied_count = 0;
};

struct ReplayRequest {
  std::string topic;
  std::uint32_t partition = 0;
  std::uint64_t offset = 0;
  std::uint32_t count = 0;
};

// 05 §3.8：Replay 成功响应与 Fetch 同构。
using ReplayResponse = FetchResponse;

// 响应通用前缀：error_code u16；非 0 时紧随 message str（≤512B）。
Status EncodeResponsePrefix(common::StatusCode error_code,
                            const std::string& message,
                            Encoder* encoder);
Status DecodeResponsePrefix(Decoder* decoder,
                            common::StatusCode* error_code,
                            std::string* message);

Status EncodeCreateTopicRequest(const CreateTopicRequest& request,
                                std::string* payload);
Status DecodeCreateTopicRequest(const std::string& payload,
                                CreateTopicRequest* request);
Status EncodeCreateTopicResponse(const CreateTopicResponse& response,
                                 std::string* payload);
Status DecodeCreateTopicResponse(const std::string& payload,
                                 CreateTopicResponse* response);

Status EncodeListTopicsRequest(std::string* payload);
Status DecodeListTopicsRequest(const std::string& payload);
Status EncodeListTopicsResponse(const ListTopicsResponse& response,
                                std::string* payload);
Status DecodeListTopicsResponse(const std::string& payload,
                                ListTopicsResponse* response);

Status EncodeProduceRequest(const ProduceRequest& request,
                            std::string* payload);
Status DecodeProduceRequest(const std::string& payload,
                            ProduceRequest* request);
Status EncodeProduceResponse(const ProduceResponse& response,
                             std::string* payload);
Status DecodeProduceResponse(const std::string& payload,
                             ProduceResponse* response);

Status EncodeFetchRequest(const FetchRequest& request, std::string* payload);
Status DecodeFetchRequest(const std::string& payload, FetchRequest* request);
Status EncodeFetchResponse(const FetchResponse& response,
                           std::string* payload);
Status DecodeFetchResponse(const std::string& payload,
                           FetchResponse* response);

Status EncodeOffsetFetchRequest(const OffsetFetchRequest& request,
                                std::string* payload);
Status DecodeOffsetFetchRequest(const std::string& payload,
                                OffsetFetchRequest* request);
Status EncodeOffsetFetchResponse(const OffsetFetchResponse& response,
                                 std::string* payload);
Status DecodeOffsetFetchResponse(const std::string& payload,
                                 OffsetFetchResponse* response);

Status EncodeOffsetCommitRequest(const OffsetCommitRequest& request,
                                 std::string* payload);
Status DecodeOffsetCommitRequest(const std::string& payload,
                                 OffsetCommitRequest* request);
Status EncodeOffsetCommitResponse(const OffsetCommitResponse& response,
                                  std::string* payload);
Status DecodeOffsetCommitResponse(const std::string& payload,
                                  OffsetCommitResponse* response);

Status EncodeReplayRequest(const ReplayRequest& request, std::string* payload);
Status DecodeReplayRequest(const std::string& payload, ReplayRequest* request);
Status EncodeReplayResponse(const ReplayResponse& response,
                            std::string* payload);
Status DecodeReplayResponse(const std::string& payload,
                            ReplayResponse* response);

}  // namespace protocol
}  // namespace minikafka

#endif  // MINIKAFKA_PROTOCOL_MESSAGES_H_
