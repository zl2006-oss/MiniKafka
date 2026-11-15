#include "test_common.h"

#include <sstream>
#include <string>

#include "minikafka/common/event_sink.h"
#include "minikafka/common/json.h"

void TestEventSink() {
  using minikafka::common::EventSink;
  using minikafka::common::JsonObject;
  using minikafka::common::ParseJsonObject;
  using minikafka::common::Status;
  using minikafka::common::StatusCode;

  std::ostringstream out;
  std::ostringstream err;
  EventSink sink(out, err);

  sink.WriteResult("{\"a\":1}");
  MK_EXPECT_EQ(out.str(), std::string("{\"a\":1}\n"));
  MK_EXPECT_EQ(err.str(), std::string());

  sink.WriteEvent("{\"event\":\"ready\"}");
  MK_EXPECT_EQ(err.str(), std::string("{\"event\":\"ready\"}\n"));

  sink.WriteResult("x\n");
  MK_EXPECT_EQ(out.str(), std::string("{\"a\":1}\nx\n"));

  sink.WriteResult("second");
  MK_EXPECT_EQ(out.str(), std::string("{\"a\":1}\nx\nsecond\n"));

  sink.Flush();

  std::ostringstream error_out;
  std::ostringstream error_err;
  EventSink error_sink(error_out, error_err);
  error_sink.WriteError(Status(StatusCode::kDiskFull, "no space"));
  MK_EXPECT_EQ(error_out.str(), std::string());

  JsonObject error_object;
  Status parse_status = ParseJsonObject(error_err.str(), &error_object);
  MK_EXPECT_TRUE(parse_status.ok());
  std::string code;
  std::string message;
  MK_EXPECT_TRUE(error_object.RequireString("error", &code).ok());
  MK_EXPECT_TRUE(error_object.RequireString("message", &message).ok());
  MK_EXPECT_EQ(code, std::string("disk_full"));
  MK_EXPECT_EQ(message, std::string("no space"));

  std::ostringstream ok_out;
  std::ostringstream ok_err;
  EventSink ok_sink(ok_out, ok_err);
  ok_sink.WriteError(Status());
  MK_EXPECT_EQ(ok_out.str(), std::string());
  MK_EXPECT_EQ(ok_err.str(), std::string());

  std::ostringstream quote_out;
  std::ostringstream quote_err;
  EventSink quote_sink(quote_out, quote_err);
  quote_sink.WriteError(Status(StatusCode::kInvalidJson, "a\"b"));
  JsonObject quote_object;
  parse_status = ParseJsonObject(quote_err.str(), &quote_object);
  MK_EXPECT_TRUE(parse_status.ok());
  MK_EXPECT_TRUE(quote_object.RequireString("message", &message).ok());
  MK_EXPECT_EQ(message, std::string("a\"b"));

  std::string invalid_message;
  invalid_message.push_back(static_cast<char>(0xC3));
  invalid_message.push_back('(');
  std::ostringstream fallback_out;
  std::ostringstream fallback_err;
  EventSink fallback_sink(fallback_out, fallback_err);
  fallback_sink.WriteError(
      Status(StatusCode::kInternalError, invalid_message));
  JsonObject fallback_object;
  parse_status = ParseJsonObject(fallback_err.str(), &fallback_object);
  MK_EXPECT_TRUE(parse_status.ok());
  MK_EXPECT_TRUE(fallback_object.RequireString("error", &code).ok());
  MK_EXPECT_EQ(code, std::string("internal_error"));
}
