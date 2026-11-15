#ifndef MINIKAFKA_COMMON_JSON_H_
#define MINIKAFKA_COMMON_JSON_H_

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include "minikafka/common/status.h"

namespace minikafka {
namespace common {

// 受限 JSON 子集（AD-07/Q-P14）：顶层必须是扁平对象；值只允许字符串或 null。
struct JsonValue {
  enum class Type { kString, kNull };

  Type type = Type::kNull;
  std::string string_value;
};

class JsonObject {
 public:
  JsonObject() = default;

  bool Has(const std::string& key) const;
  bool IsNull(const std::string& key) const;

  // 仅在字段存在且为字符串时返回 true；null/缺失返回 false。
  bool GetString(const std::string& key, std::string* value) const;

  // 必填字符串校验：缺失、null 或非字符串均返回 invalid_value_type。
  Status RequireString(const std::string& key, std::string* value) const;

  // 写入前校验 key/value 的 UTF-8；重复 key 为更新语义。
  Status SetString(const std::string& key, const std::string& value);
  Status SetNull(const std::string& key);

  const std::vector<std::pair<std::string, JsonValue>>& items() const {
    return items_;
  }

  std::string Serialize() const;
  void Clear() { items_.clear(); }

 private:
  const JsonValue* Find(const std::string& key) const;
  JsonValue* Find(const std::string& key);

  std::vector<std::pair<std::string, JsonValue>> items_;
};

// 解析一行/一段 JSON 对象；成功时覆盖 *out。
// 语法/结构错误 -> invalid_json；字符串编码非法 -> invalid_utf8；
// 值不是字符串或 null -> invalid_value_type。
Status ParseJsonObject(const std::string& text, JsonObject* out);

bool IsValidUtf8(const std::string& text);
Status ValidateUtf8(const std::string& text);

}  // namespace common
}  // namespace minikafka

#endif  // MINIKAFKA_COMMON_JSON_H_
