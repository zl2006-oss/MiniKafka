#include "minikafka/client/cli.h"

#include <cstddef>
#include <cstdlib>
#include <iostream>

#include "minikafka/common/status.h"

namespace minikafka {
namespace client {

using common::Status;
using common::StatusCode;

namespace {

bool ParseUnsigned(const std::string& text, std::uint64_t* value) {
  if (text.empty()) {
    return false;
  }
  std::uint64_t result = 0;
  for (std::size_t i = 0; i < text.size(); ++i) {
    const char ch = text[i];
    if (ch < '0' || ch > '9') {
      return false;
    }
    const std::uint64_t digit = static_cast<std::uint64_t>(ch - '0');
    if (result > (static_cast<std::uint64_t>(-1) - digit) / 10u) {
      return false;
    }
    result = result * 10u + digit;
  }
  *value = result;
  return true;
}

}  // namespace

common::Status ParseAddress(const std::string& text, Address* out) {
  if (out == nullptr) {
    return Status(StatusCode::kInternalError, "null address output");
  }
  if (text.empty()) {
    return Status(StatusCode::kUsage, "empty broker address");
  }
  Address address;
  const std::size_t colon = text.rfind(':');
  if (colon == std::string::npos) {
    address.host = text;
  } else {
    address.host = text.substr(0, colon);
    const std::string port_text = text.substr(colon + 1);
    std::uint64_t port = 0;
    if (!ParseUnsigned(port_text, &port) || port == 0u || port > 65535u) {
      return Status(StatusCode::kUsage, "invalid port in broker address");
    }
    address.port = static_cast<std::uint16_t>(port);
  }
  if (address.host.empty()) {
    return Status(StatusCode::kUsage, "empty host in broker address");
  }
  *out = address;
  return Status();
}

common::Status ParseFrom(const std::string& text, FromSpec* out) {
  if (out == nullptr) {
    return Status(StatusCode::kInternalError, "null from output");
  }
  FromSpec spec;
  if (text == "earliest") {
    spec.mode = FromMode::kEarliest;
  } else if (text == "latest") {
    spec.mode = FromMode::kLatest;
  } else {
    std::uint64_t offset = 0;
    if (!ParseUnsigned(text, &offset)) {
      return Status(StatusCode::kInvalidFrom,
                    "--from must be earliest, latest or a non-negative "
                    "integer");
    }
    spec.mode = FromMode::kOffset;
    spec.offset = offset;
  }
  *out = spec;
  return Status();
}

common::Status ParseIntervalSeconds(const std::string& text, int* seconds) {
  if (seconds == nullptr) {
    return Status(StatusCode::kInternalError, "null interval output");
  }
  std::string digits = text;
  if (!digits.empty() && digits.back() == 's') {
    digits.pop_back();
  }
  std::uint64_t value = 0;
  if (!ParseUnsigned(digits, &value) || value == 0u || value > 86400u) {
    return Status(StatusCode::kInvalidInterval,
                  "--auto-commit-interval must be a positive number of "
                  "seconds, e.g. 5s");
  }
  *seconds = static_cast<int>(value);
  return Status();
}

std::string Quote(const std::string& text) {
  std::string out;
  out.reserve(text.size() + 2u);
  out.push_back('"');
  for (std::size_t i = 0; i < text.size(); ++i) {
    const unsigned char ch = static_cast<unsigned char>(text[i]);
    switch (ch) {
      case '"':
        out += "\\\"";
        break;
      case '\\':
        out += "\\\\";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      case '\b':
        out += "\\b";
        break;
      case '\f':
        out += "\\f";
        break;
      default:
        if (ch < 0x20u) {
          const char* hex = "0123456789abcdef";
          out += "\\u00";
          out.push_back(hex[(ch >> 4) & 0x0Fu]);
          out.push_back(hex[ch & 0x0Fu]);
        } else {
          out.push_back(static_cast<char>(ch));
        }
        break;
    }
  }
  out.push_back('"');
  return out;
}

std::string Number(std::uint64_t value) { return std::to_string(value); }

bool ParseU64(const std::string& text, std::uint64_t* value) {
  if (value == nullptr) {
    return false;
  }
  return ParseUnsigned(text, value);
}

std::string RecordLine(const std::string& topic, std::uint32_t partition,
                       std::uint64_t offset, bool has_key,
                       const std::string& key, const std::string& value) {
  std::string line = "{\"topic\":";
  line += Quote(topic);
  line += ",\"partition\":";
  line += Number(partition);
  line += ",\"offset\":";
  line += Number(offset);
  line += ",\"key\":";
  line += has_key ? Quote(key) : std::string("null");
  line += ",\"value\":";
  line += Quote(value);
  line += "}";
  return line;
}

int ExitCodeFor(const common::Status& status) {
  switch (status.code()) {
    case StatusCode::kOk:
      return 0;
    case StatusCode::kUsage:
    case StatusCode::kInvalidInterval:
    // 05 §3.4：invalid_from 由 CLI 本地拒绝，映射为用法类退出码 2。
    case StatusCode::kInvalidFrom:
      return 2;
    default:
      return 3;
  }
}

bool IsRetryable(StatusCode code) {
  switch (code) {
    case StatusCode::kDiskFull:
    case StatusCode::kBrokerUnavailable:
    case StatusCode::kStorageIoError:
    case StatusCode::kStorageUnavailable:
      return true;
    default:
      return false;
  }
}

void PrintTopicUsage() {
  std::cout << "usage: minikafka topic <create|list> [options]\n"
            << "  topic create --topic <name> --partitions <1..16>\n"
            << "  topic list\n";
}

void PrintProduceUsage() {
  std::cout
      << "usage: minikafka produce --topic <name> [options]\n"
      << "  --topic <name>               目标 Topic（必填）\n"
      << "  --partition <n>              显式分区；缺省按 key 哈希/轮询选择\n"
      << "  --retries <n>                重试次数，默认 3\n"
      << "  --retry-backoff-ms <n>       重试间隔，默认 100\n"
      << "  --retry-timeout-ms <n>       总超时，默认 5000\n"
      << "  --max-record-bytes <n>       单条记录上限，默认/上限 1048576\n"
      << "  --message '<json>' | --file <path> | stdin\n";
}

void PrintConsumeUsage() {
  std::cout
      << "usage: minikafka consume --topic <name> --group <name> "
         "--consumer-index <i> --consumer-count <n> [options]\n"
      << "  --from earliest|latest|<n>   起始位点，默认 earliest\n"
      << "  --auto-commit-interval <Ns>  自动提交间隔，默认 5s\n"
      << "  --exit-at-end                读到当前末尾即退出\n";
}

void PrintReplayUsage() {
  std::cout << "usage: minikafka replay --topic <name> [options]\n"
            << "  --partition <n>              默认 0\n"
            << "  --offset <n>                 起始 offset，默认 0\n"
            << "  --count <n>                  条数；缺省读到末尾\n";
}

}  // namespace client
}  // namespace minikafka
