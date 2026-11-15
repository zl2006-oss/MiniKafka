#include "minikafka/common/json.h"

#include <cstdint>
#include <string>
#include <utility>

namespace minikafka {
namespace common {
namespace {

bool IsWhitespace(char c) {
  return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

int HexValue(char c) {
  if (c >= '0' && c <= '9') {
    return c - '0';
  }
  if (c >= 'a' && c <= 'f') {
    return c - 'a' + 10;
  }
  if (c >= 'A' && c <= 'F') {
    return c - 'A' + 10;
  }
  return -1;
}

void AppendCodePoint(std::uint32_t code_point, std::string* out) {
  if (code_point <= 0x7Fu) {
    out->push_back(static_cast<char>(code_point));
  } else if (code_point <= 0x7FFu) {
    out->push_back(static_cast<char>(0xC0u | (code_point >> 6)));
    out->push_back(static_cast<char>(0x80u | (code_point & 0x3Fu)));
  } else if (code_point <= 0xFFFFu) {
    out->push_back(static_cast<char>(0xE0u | (code_point >> 12)));
    out->push_back(static_cast<char>(0x80u | ((code_point >> 6) & 0x3Fu)));
    out->push_back(static_cast<char>(0x80u | (code_point & 0x3Fu)));
  } else if (code_point <= 0x10FFFFu) {
    out->push_back(static_cast<char>(0xF0u | (code_point >> 18)));
    out->push_back(static_cast<char>(0x80u | ((code_point >> 12) & 0x3Fu)));
    out->push_back(static_cast<char>(0x80u | ((code_point >> 6) & 0x3Fu)));
    out->push_back(static_cast<char>(0x80u | (code_point & 0x3Fu)));
  }
}

void AppendJsonString(const std::string& value, std::string* out) {
  static const char kHex[] = "0123456789abcdef";
  out->push_back('"');
  for (char ch : value) {
    const unsigned int c = static_cast<unsigned char>(ch);
    switch (c) {
      case '"':
        out->append("\\\"");
        break;
      case '\\':
        out->append("\\\\");
        break;
      case '\b':
        out->append("\\b");
        break;
      case '\f':
        out->append("\\f");
        break;
      case '\n':
        out->append("\\n");
        break;
      case '\r':
        out->append("\\r");
        break;
      case '\t':
        out->append("\\t");
        break;
      default:
        if (c < 0x20u) {
          out->append("\\u00");
          out->push_back(kHex[(c >> 4) & 0x0Fu]);
          out->push_back(kHex[c & 0x0Fu]);
        } else {
          out->push_back(static_cast<char>(c));
        }
        break;
    }
  }
  out->push_back('"');
}

bool IsValidUtf8Impl(const std::string& text) {
  const unsigned char* data =
      reinterpret_cast<const unsigned char*>(text.data());
  const std::size_t size = text.size();
  std::size_t i = 0;

  while (i < size) {
    const unsigned int c = static_cast<unsigned int>(data[i]);
    if (c <= 0x7Fu) {
      ++i;
      continue;
    }
    if (c >= 0xC2u && c <= 0xDFu) {
      if (i + 1 >= size || (data[i + 1] & 0xC0u) != 0x80u) {
        return false;
      }
      i += 2;
      continue;
    }
    if (c >= 0xE0u && c <= 0xEFu) {
      if (i + 2 >= size) {
        return false;
      }
      const unsigned int c1 = static_cast<unsigned int>(data[i + 1]);
      const unsigned int c2 = static_cast<unsigned int>(data[i + 2]);
      if ((c2 & 0xC0u) != 0x80u) {
        return false;
      }
      if (c == 0xE0u) {
        if (c1 < 0xA0u || c1 > 0xBFu) {
          return false;
        }
      } else if (c == 0xEDu) {
        if (c1 < 0x80u || c1 > 0x9Fu) {
          return false;
        }
      } else if (c1 < 0x80u || c1 > 0xBFu) {
        return false;
      }
      i += 3;
      continue;
    }
    if (c >= 0xF0u && c <= 0xF4u) {
      if (i + 3 >= size) {
        return false;
      }
      const unsigned int c1 = static_cast<unsigned int>(data[i + 1]);
      const unsigned int c2 = static_cast<unsigned int>(data[i + 2]);
      const unsigned int c3 = static_cast<unsigned int>(data[i + 3]);
      if ((c2 & 0xC0u) != 0x80u || (c3 & 0xC0u) != 0x80u) {
        return false;
      }
      if (c == 0xF0u) {
        if (c1 < 0x90u || c1 > 0xBFu) {
          return false;
        }
      } else if (c == 0xF4u) {
        if (c1 < 0x80u || c1 > 0x8Fu) {
          return false;
        }
      } else if (c1 < 0x80u || c1 > 0xBFu) {
        return false;
      }
      i += 4;
      continue;
    }
    return false;
  }
  return true;
}

class Parser {
 public:
  explicit Parser(const std::string& text) : text_(text), pos_(0) {}

  Status ParseObject(JsonObject* out) {
    SkipWhitespace();
    if (!Consume('{')) {
      return Error(StatusCode::kInvalidJson, "expected '{'");
    }

    JsonObject result;
    SkipWhitespace();
    if (Consume('}')) {
      SkipWhitespace();
      if (pos_ != text_.size()) {
        return Error(StatusCode::kInvalidJson, "trailing characters");
      }
      *out = std::move(result);
      return Status();
    }

    while (true) {
      SkipWhitespace();
      if (!Peek('"')) {
        return Error(StatusCode::kInvalidJson, "expected string key");
      }

      std::string key;
      Status status = ParseString(&key);
      if (!status.ok()) {
        return status;
      }
      if (result.Has(key)) {
        return Error(StatusCode::kInvalidJson, "duplicate key");
      }

      SkipWhitespace();
      if (!Consume(':')) {
        return Error(StatusCode::kInvalidJson, "expected ':'");
      }
      SkipWhitespace();

      JsonValue value;
      status = ParseValue(&value);
      if (!status.ok()) {
        return status;
      }
      if (value.type == JsonValue::Type::kString) {
        status = result.SetString(key, value.string_value);
      } else {
        status = result.SetNull(key);
      }
      if (!status.ok()) {
        return status;
      }

      SkipWhitespace();
      if (Consume(',')) {
        SkipWhitespace();
        if (Peek('}')) {
          return Error(StatusCode::kInvalidJson, "trailing comma");
        }
        continue;
      }
      if (Consume('}')) {
        break;
      }
      return Error(StatusCode::kInvalidJson, "expected ',' or '}'");
    }

    SkipWhitespace();
    if (pos_ != text_.size()) {
      return Error(StatusCode::kInvalidJson, "trailing characters");
    }
    *out = std::move(result);
    return Status();
  }

 private:
  Status ParseValue(JsonValue* out) {
    if (Peek('"')) {
      std::string value;
      Status status = ParseString(&value);
      if (!status.ok()) {
        return status;
      }
      out->type = JsonValue::Type::kString;
      out->string_value = std::move(value);
      return Status();
    }

    if (Peek('n')) {
      if (text_.size() - pos_ >= 4u &&
          text_.compare(pos_, 4u, "null") == 0) {
        pos_ += 4u;
        out->type = JsonValue::Type::kNull;
        out->string_value.clear();
        return Status();
      }
      return Error(StatusCode::kInvalidJson, "invalid literal");
    }

    const char c = PeekChar();
    if (c == '{' || c == '[' || c == '-' || (c >= '0' && c <= '9') ||
        c == 't' || c == 'f') {
      return Error(StatusCode::kInvalidValueType,
                   "only string or null values are supported");
    }
    return Error(StatusCode::kInvalidJson, "unexpected value");
  }

  Status ParseString(std::string* out) {
    if (!Consume('"')) {
      return Error(StatusCode::kInvalidJson, "expected '\"'");
    }

    std::string result;
    while (pos_ < text_.size()) {
      const unsigned int c = static_cast<unsigned char>(text_[pos_]);
      if (c == '"') {
        ++pos_;
        if (!IsValidUtf8Impl(result)) {
          return Error(StatusCode::kInvalidUtf8, "invalid UTF-8 in string");
        }
        *out = std::move(result);
        return Status();
      }
      if (c == '\\') {
        ++pos_;
        if (pos_ >= text_.size()) {
          return Error(StatusCode::kInvalidJson, "unterminated escape");
        }
        const char escaped = text_[pos_++];
        switch (escaped) {
          case '"':
            result.push_back('"');
            break;
          case '\\':
            result.push_back('\\');
            break;
          case '/':
            result.push_back('/');
            break;
          case 'b':
            result.push_back('\b');
            break;
          case 'f':
            result.push_back('\f');
            break;
          case 'n':
            result.push_back('\n');
            break;
          case 'r':
            result.push_back('\r');
            break;
          case 't':
            result.push_back('\t');
            break;
          case 'u': {
            std::uint32_t unit = 0;
            if (!ReadHex4(&unit)) {
              return Error(StatusCode::kInvalidJson, "invalid \\u escape");
            }
            if (unit >= 0xD800u && unit <= 0xDBFFu) {
              if (pos_ + 1u >= text_.size() || text_[pos_] != '\\' ||
                  text_[pos_ + 1u] != 'u') {
                return Error(StatusCode::kInvalidJson,
                             "missing low surrogate");
              }
              pos_ += 2u;
              std::uint32_t low = 0;
              if (!ReadHex4(&low) || low < 0xDC00u || low > 0xDFFFu) {
                return Error(StatusCode::kInvalidJson,
                             "invalid low surrogate");
              }
              const std::uint32_t code_point =
                  0x10000u + ((unit - 0xD800u) << 10) + (low - 0xDC00u);
              AppendCodePoint(code_point, &result);
            } else if (unit >= 0xDC00u && unit <= 0xDFFFu) {
              return Error(StatusCode::kInvalidJson, "isolated low surrogate");
            } else {
              AppendCodePoint(unit, &result);
            }
            break;
          }
          default:
            return Error(StatusCode::kInvalidJson, "invalid escape");
        }
        continue;
      }
      if (c < 0x20u) {
        return Error(StatusCode::kInvalidJson,
                     "unescaped control character");
      }
      result.push_back(text_[pos_]);
      ++pos_;
    }
    return Error(StatusCode::kInvalidJson, "unterminated string");
  }

  bool ReadHex4(std::uint32_t* value) {
    if (pos_ + 4u > text_.size()) {
      return false;
    }
    std::uint32_t parsed = 0;
    for (std::size_t i = 0; i < 4u; ++i) {
      const int digit = HexValue(text_[pos_ + i]);
      if (digit < 0) {
        return false;
      }
      parsed = (parsed << 4) | static_cast<std::uint32_t>(digit);
    }
    pos_ += 4u;
    *value = parsed;
    return true;
  }

  void SkipWhitespace() {
    while (pos_ < text_.size() && IsWhitespace(text_[pos_])) {
      ++pos_;
    }
  }

  bool Peek(char c) const {
    return pos_ < text_.size() && text_[pos_] == c;
  }

  char PeekChar() const { return pos_ < text_.size() ? text_[pos_] : '\0'; }

  bool Consume(char c) {
    if (!Peek(c)) {
      return false;
    }
    ++pos_;
    return true;
  }

  Status Error(StatusCode code, const std::string& message) const {
    return Status(code, message + " at offset " + std::to_string(pos_));
  }

  const std::string& text_;
  std::size_t pos_;
};

}  // namespace

bool JsonObject::Has(const std::string& key) const {
  return Find(key) != nullptr;
}

bool JsonObject::IsNull(const std::string& key) const {
  const JsonValue* value = Find(key);
  return value != nullptr && value->type == JsonValue::Type::kNull;
}

bool JsonObject::GetString(const std::string& key, std::string* value) const {
  if (value == nullptr) {
    return false;
  }
  const JsonValue* item = Find(key);
  if (item == nullptr || item->type != JsonValue::Type::kString) {
    return false;
  }
  *value = item->string_value;
  return true;
}

Status JsonObject::RequireString(const std::string& key,
                                std::string* value) const {
  if (value == nullptr) {
    return Status(StatusCode::kInternalError, "output string is null");
  }
  const JsonValue* item = Find(key);
  if (item == nullptr) {
    return Status(StatusCode::kInvalidValueType,
                  "missing field '" + key + "'");
  }
  if (item->type != JsonValue::Type::kString) {
    return Status(StatusCode::kInvalidValueType,
                  "field '" + key + "' must be a string");
  }
  *value = item->string_value;
  return Status();
}

Status JsonObject::SetString(const std::string& key,
                             const std::string& value) {
  if (!IsValidUtf8Impl(key)) {
    return Status(StatusCode::kInvalidUtf8, "key is not valid UTF-8");
  }
  if (!IsValidUtf8Impl(value)) {
    return Status(StatusCode::kInvalidUtf8, "value is not valid UTF-8");
  }

  JsonValue* existing = Find(key);
  if (existing != nullptr) {
    existing->type = JsonValue::Type::kString;
    existing->string_value = value;
    return Status();
  }

  JsonValue item;
  item.type = JsonValue::Type::kString;
  item.string_value = value;
  items_.emplace_back(key, std::move(item));
  return Status();
}

Status JsonObject::SetNull(const std::string& key) {
  if (!IsValidUtf8Impl(key)) {
    return Status(StatusCode::kInvalidUtf8, "key is not valid UTF-8");
  }

  JsonValue* existing = Find(key);
  if (existing != nullptr) {
    existing->type = JsonValue::Type::kNull;
    existing->string_value.clear();
    return Status();
  }

  JsonValue item;
  item.type = JsonValue::Type::kNull;
  items_.emplace_back(key, std::move(item));
  return Status();
}

std::string JsonObject::Serialize() const {
  std::string result;
  result.push_back('{');
  for (std::size_t i = 0; i < items_.size(); ++i) {
    if (i != 0) {
      result.push_back(',');
    }
    AppendJsonString(items_[i].first, &result);
    result.push_back(':');
    if (items_[i].second.type == JsonValue::Type::kString) {
      AppendJsonString(items_[i].second.string_value, &result);
    } else {
      result.append("null");
    }
  }
  result.push_back('}');
  return result;
}

const JsonValue* JsonObject::Find(const std::string& key) const {
  for (const std::pair<std::string, JsonValue>& item : items_) {
    if (item.first == key) {
      return &item.second;
    }
  }
  return nullptr;
}

JsonValue* JsonObject::Find(const std::string& key) {
  for (std::pair<std::string, JsonValue>& item : items_) {
    if (item.first == key) {
      return &item.second;
    }
  }
  return nullptr;
}

Status ParseJsonObject(const std::string& text, JsonObject* out) {
  if (out == nullptr) {
    return Status(StatusCode::kInternalError, "output object is null");
  }
  Parser parser(text);
  JsonObject result;
  Status status = parser.ParseObject(&result);
  if (!status.ok()) {
    return status;
  }
  *out = std::move(result);
  return Status();
}

bool IsValidUtf8(const std::string& text) { return IsValidUtf8Impl(text); }

Status ValidateUtf8(const std::string& text) {
  if (IsValidUtf8Impl(text)) {
    return Status();
  }
  return Status(StatusCode::kInvalidUtf8, "invalid UTF-8");
}

}  // namespace common
}  // namespace minikafka
