#ifndef MINIKAFKA_COMMON_EVENT_SINK_H_
#define MINIKAFKA_COMMON_EVENT_SINK_H_

#include <iostream>
#include <ostream>
#include <string>

#include "minikafka/common/status.h"

namespace minikafka {
namespace common {

// stdout 结果与 stderr 事件/错误分流（R19/Q-P9）；每次写入一行 JSON。
class EventSink {
 public:
  EventSink();
  EventSink(std::ostream& out, std::ostream& err);

  void WriteResult(const std::string& line);
  void WriteEvent(const std::string& line);

  // 输出 {"error":"<字符串码>","message":"<说明>"} 到 stderr；ok 状态不输出。
  void WriteError(const Status& status);

  void Flush();

 private:
  static void WriteLine(std::ostream& stream, const std::string& line);

  std::ostream& out_;
  std::ostream& err_;
};

}  // namespace common
}  // namespace minikafka

#endif  // MINIKAFKA_COMMON_EVENT_SINK_H_
