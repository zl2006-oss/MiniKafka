#include "minikafka/broker/service.h"

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "minikafka/storage/record.h"
#include "minikafka/storage/storage_worker.h"

namespace minikafka {
namespace broker {

using common::Status;
using common::StatusCode;
using common::StatusOr;
using protocol::FetchRecord;
using protocol::FetchResponse;
using protocol::ProduceResponse;
using protocol::ProduceResult;
using storage::kRecordHeaderSize;
using storage::PartitionStore;
using storage::Record;

namespace {

// 一条记录在响应里的字节数：协议头 + key + value（不含帧头）。
std::uint32_t RecordWireSize(const Record& record) {
  return static_cast<std::uint32_t>(kRecordHeaderSize + record.key.size() +
                                    record.value.size());
}

std::uint8_t AggregateStatus(std::size_t appended, std::size_t failed) {
  if (failed == 0u) {
    return 0u;  // 全部追加
  }
  return appended == 0u ? 2u : 1u;  // 无条目追加 / 部分追加
}

}  // namespace

ProduceResponse PartitionService::Produce(
    const protocol::ProduceRequest& request) {
  ProduceResponse response;

  StatusOr<PartitionStore*> resolved =
      manager_->Resolve(request.topic, request.partition);
  if (!resolved.ok()) {
    // 请求级错误：status=2、result_count=0（05 §3.4）。
    response.error_code = resolved.status().code();
    response.message = resolved.status().message();
    response.status = 2u;
    return response;
  }
  PartitionStore* store = resolved.value();

  if (request.entries.empty()) {
    response.error_code = StatusCode::kInvalidBatch;
    response.message = "produce must carry at least one entry";
    response.status = 2u;
    return response;
  }

  response.results.reserve(request.entries.size());
  bool blocked = false;  // AD-14：同分区首个失败后，后续条目不再尝试
  std::size_t appended = 0;
  std::size_t failed = 0;

  for (std::size_t i = 0; i < request.entries.size(); ++i) {
    ProduceResult result;
    result.index = static_cast<std::uint32_t>(i);

    if (blocked) {
      result.status = 2u;
      result.error_code = StatusCode::kPartitionBlocked;
      response.results.push_back(result);
      ++failed;
      continue;
    }

    Record record;
    record.flags = request.entries[i].flags;
    record.key = request.entries[i].key;
    record.value = request.entries[i].value;

    std::uint64_t offset = 0;
    // Append 内部先做磁盘空间预检（R16），再编码校验（R17）并顺序追加（R2/R27）。
    const Status append_status = store->Append(record, &offset);
    if (!append_status.ok()) {
      result.status = 1u;
      result.error_code = append_status.code();
      result.offset = 0;
      blocked = true;
      ++failed;
    } else {
      result.status = 0u;
      result.error_code = StatusCode::kOk;
      result.offset = offset;
      ++appended;
    }
    response.results.push_back(result);
  }

  response.status = AggregateStatus(appended, failed);
  return response;
}

common::Status PartitionService::ReadRecords(
    PartitionStore* store, std::uint64_t offset, std::uint32_t max_bytes,
    std::uint32_t max_count, FetchResponse* response) const {
  const std::uint64_t log_end = store->next_offset();
  response->log_end_offset = log_end;

  std::uint64_t current = offset;
  std::uint64_t used = 0;
  while (current < log_end &&
         response->records.size() < static_cast<std::size_t>(max_count)) {
    StatusOr<Record> read = store->ReadAt(current);
    if (!read.ok()) {
      if (response->records.empty()) {
        return read.status();
      }
      break;  // 已经拿到部分记录：按成功返回，避免整批回退
    }
    const std::uint32_t wire_size = RecordWireSize(read.value());
    // 至少返回一条能容纳的记录；之后受 max_bytes 与 4 MiB 硬上限约束。
    if (!response->records.empty() &&
        (used + wire_size > max_bytes ||
         used + wire_size > protocol::kMaxFetchBytes)) {
      break;
    }
    FetchRecord record;
    record.offset = current;
    record.flags = read.value().flags;
    record.key = read.value().key;
    record.value = read.value().value;
    used += wire_size;
    response->records.push_back(std::move(record));
    ++current;
  }
  return Status();
}

FetchResponse PartitionService::Fetch(
    const protocol::FetchRequest& request) {
  FetchResponse response;
  response.topic = request.topic;
  response.partition = request.partition;

  StatusOr<PartitionStore*> resolved =
      manager_->Resolve(request.topic, request.partition);
  if (!resolved.ok()) {
    response.error_code = resolved.status().code();
    response.message = resolved.status().message();
    return response;
  }
  PartitionStore* store = resolved.value();

  std::uint32_t max_bytes = request.max_bytes;
  if (max_bytes == 0u) {
    max_bytes = kDefaultFetchBytes;
  }
  if (max_bytes > protocol::kMaxFetchBytes) {
    max_bytes = protocol::kMaxFetchBytes;
  }

  const std::uint64_t log_end = store->next_offset();
  if (request.offset > log_end) {
    response.log_end_offset = log_end;
    response.error_code = StatusCode::kOffsetOutOfRange;
    response.message = "offset " + std::to_string(request.offset) +
                       " is beyond log end " + std::to_string(log_end);
    return response;
  }

  const Status read_status =
      ReadRecords(store, request.offset, max_bytes, kMaxFetchRecords, &response);
  if (!read_status.ok()) {
    response.error_code = read_status.code();
    response.message = read_status.message();
    response.records.clear();
  }
  return response;
}

FetchResponse PartitionService::Replay(
    const protocol::ReplayRequest& request) {
  FetchResponse response;
  response.topic = request.topic;
  response.partition = request.partition;

  StatusOr<PartitionStore*> resolved =
      manager_->Resolve(request.topic, request.partition);
  if (!resolved.ok()) {
    response.error_code = resolved.status().code();
    response.message = resolved.status().message();
    return response;
  }
  PartitionStore* store = resolved.value();

  const std::uint64_t log_end = store->next_offset();
  if (request.offset > log_end) {
    response.log_end_offset = log_end;
    response.error_code = StatusCode::kOffsetOutOfRange;
    response.message = "offset " + std::to_string(request.offset) +
                       " is beyond log end " + std::to_string(log_end);
    return response;
  }

  std::uint32_t max_count = kMaxFetchRecords;
  if (request.count != 0u && request.count < max_count) {
    max_count = request.count;  // count=0 表示读到末尾
  }
  const Status read_status = ReadRecords(
      store, request.offset, protocol::kMaxFetchBytes, max_count, &response);
  if (!read_status.ok()) {
    response.error_code = read_status.code();
    response.message = read_status.message();
    response.records.clear();
  }
  return response;
}

}  // namespace broker
}  // namespace minikafka
