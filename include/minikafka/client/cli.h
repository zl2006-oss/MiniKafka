#ifndef MINIKAFKA_CLIENT_CLI_H_
#define MINIKAFKA_CLIENT_CLI_H_

#include <cstdint>
#include <string>
#include <vector>

#include "minikafka/common/status.h"

namespace minikafka {
namespace client {

// 03 §3.4 B4 的客户端地址默认值（bench 可覆盖，其余子命令固定用它）。
struct Address {
  std::string host = "127.0.0.1";
  std::uint16_t port = 9092;
};

// "host:port" / "host"（缺省端口 9092）。
common::Status ParseAddress(const std::string& text, Address* out);

// `--from` 的三种取值（R11）：earliest / latest / <n>。
enum class FromMode { kEarliest, kLatest, kOffset };

struct FromSpec {
  FromMode mode = FromMode::kEarliest;
  std::uint64_t offset = 0;
};

common::Status ParseFrom(const std::string& text, FromSpec* out);

// "5s" / "5" → 秒；非正数返回 kInvalidInterval（CLI 映射为退出码 2）。
common::Status ParseIntervalSeconds(const std::string& text, int* seconds);

// 输出 JSON 字符串字面量（含首尾引号）与无符号整数。
std::string Quote(const std::string& text);
std::string Number(std::uint64_t value);

// 无符号十进制解析：空串、含非数字、溢出均返回 false。
bool ParseU64(const std::string& text, std::uint64_t* value);

// consume / replay 的单条输出行（R19/R146：至少含 partition/offset/key/value）。
// 无 key 时输出 "key":null，与 produce 输入的 null 写法对称。
std::string RecordLine(const std::string& topic, std::uint32_t partition,
                       std::uint64_t offset, bool has_key,
                       const std::string& key, const std::string& value);

// 错误码 → 进程退出码：用法类 2，业务/校验类 3（03 §3.4）。
int ExitCodeFor(const common::Status& status);

// 命令入口：返回进程退出码（0/2/3/4）。
int RunTopicCreate(const std::vector<std::string>& tokens);
int RunTopicList(const std::vector<std::string>& tokens);
int RunProduce(const std::vector<std::string>& tokens);
int RunConsume(const std::vector<std::string>& tokens);
int RunReplay(const std::vector<std::string>& tokens);

// 各子命令用法（`--help` 与用法错误时输出）。
void PrintTopicUsage();
void PrintProduceUsage();
void PrintConsumeUsage();
void PrintReplayUsage();

}  // namespace client
}  // namespace minikafka

#endif  // MINIKAFKA_CLIENT_CLI_H_
