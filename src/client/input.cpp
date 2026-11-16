#include "minikafka/client/input.h"

#include <cstddef>

namespace minikafka {
namespace client {

using common::JsonObject;
using common::Status;
using common::StatusCode;
using storage::kFlagKeyPresent;
using storage::kRecordHeaderSize;

common::Status ParseMessageLine(const std::string& line, std::size_t index,
                                RawMessage* out) {
  if (out == nullptr) {
    return Status(StatusCode::kInternalError, "null message output");
  }
  JsonObject object;
  Status status = common::ParseJsonObject(line, &object);
  if (!status.ok()) {
    return status;
  }

  RawMessage message;
  message.index = index;
  std::string value;
  status = object.RequireString("value", &value);
  if (!status.ok()) {
    return status;  // 缺 value → invalid_value_type（CLI 侧归为 invalid_json 类）
  }
  message.value = value;

  std::string key;
  if (object.GetString("key", &key)) {
    message.flags = kFlagKeyPresent;
    message.key = key;
  } else if (object.Has("key") && !object.IsNull("key")) {
    // key 存在但不是字符串/null：按非法值处理。
    return Status(StatusCode::kInvalidValueType,
                  "key must be a string or null");
  }

  *out = message;
  return Status();
}

bool RecordTooLarge(const RawMessage& message,
                    std::size_t max_record_bytes) {
  const std::size_t record_size =
      kRecordHeaderSize + message.key.size() + message.value.size();
  return record_size > max_record_bytes;
}

}  // namespace client
}  // namespace minikafka
