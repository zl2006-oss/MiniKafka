#include "minikafka/common/event_sink.h"

#include "minikafka/common/json.h"

namespace minikafka {
namespace common {

EventSink::EventSink() : out_(std::cout), err_(std::cerr) {}

EventSink::EventSink(std::ostream& out, std::ostream& err)
    : out_(out), err_(err) {}

void EventSink::WriteResult(const std::string& line) { WriteLine(out_, line); }

void EventSink::WriteEvent(const std::string& line) { WriteLine(err_, line); }

void EventSink::WriteError(const Status& status) {
  if (status.ok()) {
    return;
  }

  JsonObject object;
  const Status error_status =
      object.SetString("error", StatusCodeToString(status.code()));
  const Status message_status =
      error_status.ok() ? object.SetString("message", status.message())
                        : error_status;
  if (!error_status.ok() || !message_status.ok()) {
    WriteLine(err_,
              "{\"error\":\"internal_error\",\"message\":\"failed to "
              "serialize error\"}");
    return;
  }
  WriteLine(err_, object.Serialize());
}

void EventSink::Flush() {
  out_.flush();
  err_.flush();
}

void EventSink::WriteLine(std::ostream& stream, const std::string& line) {
  if (!line.empty() && line.back() == '\n') {
    stream << line;
  } else {
    stream << line << '\n';
  }
}

}  // namespace common
}  // namespace minikafka
