#ifndef MINIKAFKA_CLIENT_INPUT_H_
#define MINIKAFKA_CLIENT_INPUT_H_

#include <cstddef>
#include <cstdint>
#include <istream>
#include <string>
#include <vector>

#include "minikafka/common/json.h"
#include "minikafka/common/status.h"
#include "minikafka/storage/record.h"

namespace minikafka {
namespace client {

// 一条待发送消息：flags 只保留 KEY_PRESENT 位（05 §3.4）。
struct RawMessage {
  std::size_t index = 0;
  std::uint16_t flags = 0;
  std::string key;
  std::string value;
};

// 解析一行 JSON（R19）：value 必填字符串；key 可选，缺失/null 视为无 key。
common::Status ParseMessageLine(const std::string& line, std::size_t index,
                                RawMessage* out);

// 记录大小口径与 04 §3 一致：24B 头 + key + value，上限 1 MiB（R17）。
constexpr std::size_t kMaxRecordBytes = 1024u * 1024u;

bool RecordTooLarge(const RawMessage& message);

}  // namespace client
}  // namespace minikafka

#endif  // MINIKAFKA_CLIENT_INPUT_H_
