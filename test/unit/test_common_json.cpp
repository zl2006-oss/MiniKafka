#include "test_common.h"

#include <cstddef>
#include <string>

#include "minikafka/common/json.h"

namespace {

using minikafka::common::JsonObject;
using minikafka::common::ParseJsonObject;
using minikafka::common::Status;
using minikafka::common::StatusCode;

void ExpectError(const Status& status, StatusCode code) {
  MK_EXPECT_FALSE(status.ok());
  MK_EXPECT_EQ(status.code(), code);
}

std::string InvalidUtf8() {
  std::string value;
  value.push_back(static_cast<char>(0xC3));
  value.push_back('(');
  return value;
}

}  // namespace

void TestJson() {
  JsonObject object;
  Status status = ParseJsonObject("{\"key\":\"k0\",\"value\":\"hello\"}",
                                  &object);
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_TRUE(object.Has("key"));
  MK_EXPECT_FALSE(object.IsNull("key"));
  MK_EXPECT_FALSE(object.Has("missing"));

  std::string value;
  MK_EXPECT_TRUE(object.GetString("value", &value));
  MK_EXPECT_EQ(value, std::string("hello"));
  MK_EXPECT_FALSE(object.GetString("missing", &value));
  MK_EXPECT_EQ(value, std::string("hello"));

  MK_EXPECT_TRUE(object.RequireString("value", &value).ok());
  MK_EXPECT_EQ(value, std::string("hello"));
  ExpectError(object.RequireString("missing", &value),
              StatusCode::kInvalidValueType);

  MK_EXPECT_EQ(object.Serialize(),
               std::string("{\"key\":\"k0\",\"value\":\"hello\"}"));

  JsonObject null_object;
  status = ParseJsonObject("{\"key\":null,\"value\":\"v\"}", &null_object);
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_TRUE(null_object.Has("key"));
  MK_EXPECT_TRUE(null_object.IsNull("key"));
  MK_EXPECT_FALSE(null_object.GetString("key", &value));
  ExpectError(null_object.RequireString("key", &value),
              StatusCode::kInvalidValueType);
  MK_EXPECT_EQ(null_object.Serialize(), std::string("{\"key\":null,\"value\":\"v\"}"));

  JsonObject empty_value;
  status = ParseJsonObject("{\"value\":\"\"}", &empty_value);
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_TRUE(empty_value.RequireString("value", &value).ok());
  MK_EXPECT_EQ(value, std::string());

  JsonObject missing_value;
  status = ParseJsonObject("{}", &missing_value);
  MK_EXPECT_TRUE(status.ok());
  ExpectError(missing_value.RequireString("value", &value),
              StatusCode::kInvalidValueType);

  const std::string escaped_input =
      "{\"value\":\"a\\\"b\\\\c\\/d\\b\\f\\n\\r\\t\"}";
  JsonObject escaped;
  status = ParseJsonObject(escaped_input, &escaped);
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_TRUE(escaped.RequireString("value", &value).ok());
  MK_EXPECT_EQ(value, std::string("a\"b\\c/d\b\f\n\r\t"));

  JsonObject escaped_again;
  status = ParseJsonObject(escaped.Serialize(), &escaped_again);
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_TRUE(escaped_again.RequireString("value", &value).ok());
  MK_EXPECT_EQ(value, std::string("a\"b\\c/d\b\f\n\r\t"));

  JsonObject unicode;
  status = ParseJsonObject("{\"value\":\"\\u4f60\\u597d\"}", &unicode);
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_TRUE(unicode.RequireString("value", &value).ok());
  MK_EXPECT_EQ(value, std::string("\xE4\xBD\xA0\xE5\xA5\xBD"));

  JsonObject surrogate;
  status = ParseJsonObject("{\"value\":\"\\uD83D\\uDE00\"}", &surrogate);
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_TRUE(surrogate.RequireString("value", &value).ok());
  MK_EXPECT_EQ(value, std::string("\xF0\x9F\x98\x80"));

  JsonObject invalid_escape;
  ExpectError(ParseJsonObject("{\"value\":\"\\q\"}", &invalid_escape),
              StatusCode::kInvalidJson);

  JsonObject invalid_hex;
  ExpectError(ParseJsonObject("{\"value\":\"\\u12G4\"}", &invalid_hex),
              StatusCode::kInvalidJson);

  JsonObject isolated_high;
  ExpectError(ParseJsonObject("{\"value\":\"\\uD83D\"}", &isolated_high),
              StatusCode::kInvalidJson);

  JsonObject isolated_low;
  ExpectError(ParseJsonObject("{\"value\":\"\\uDE00\"}", &isolated_low),
              StatusCode::kInvalidJson);

  std::string control_input = "{\"value\":\"a";
  control_input.push_back('\n');
  control_input += "b\"}";
  JsonObject control;
  ExpectError(ParseJsonObject(control_input, &control),
              StatusCode::kInvalidJson);

  std::string invalid_utf8_input = "{\"value\":\"";
  invalid_utf8_input += InvalidUtf8();
  invalid_utf8_input += "\"}";
  JsonObject invalid_utf8;
  ExpectError(ParseJsonObject(invalid_utf8_input, &invalid_utf8),
              StatusCode::kInvalidUtf8);

  std::string overlong_input = "{\"value\":\"";
  overlong_input += std::string("\xC0\x80", 2);
  overlong_input += "\"}";
  JsonObject overlong;
  ExpectError(ParseJsonObject(overlong_input, &overlong),
              StatusCode::kInvalidUtf8);

  std::string surrogate_input = "{\"value\":\"";
  surrogate_input += std::string("\xED\xA0\x80", 3);
  surrogate_input += "\"}";
  JsonObject surrogate_bytes;
  ExpectError(ParseJsonObject(surrogate_input, &surrogate_bytes),
              StatusCode::kInvalidUtf8);

  std::string truncated_input = "{\"value\":\"";
  truncated_input += std::string("\xE4\xBD", 2);
  truncated_input += "\"}";
  JsonObject truncated;
  ExpectError(ParseJsonObject(truncated_input, &truncated),
              StatusCode::kInvalidUtf8);

  JsonObject number_value;
  ExpectError(ParseJsonObject("{\"value\":123}", &number_value),
              StatusCode::kInvalidValueType);

  JsonObject boolean_value;
  ExpectError(ParseJsonObject("{\"value\":true}", &boolean_value),
              StatusCode::kInvalidValueType);

  JsonObject nested_value;
  ExpectError(ParseJsonObject("{\"value\":{\"a\":\"b\"}}", &nested_value),
              StatusCode::kInvalidValueType);

  JsonObject array_value;
  ExpectError(ParseJsonObject("{\"value\":[]}", &array_value),
              StatusCode::kInvalidValueType);

  JsonObject duplicate_key;
  ExpectError(ParseJsonObject("{\"a\":\"1\",\"a\":\"2\"}", &duplicate_key),
              StatusCode::kInvalidJson);

  JsonObject trailing;
  ExpectError(ParseJsonObject("{\"a\":\"1\"}x", &trailing),
              StatusCode::kInvalidJson);

  JsonObject missing_brace;
  ExpectError(ParseJsonObject("{\"a\":\"1\"", &missing_brace),
              StatusCode::kInvalidJson);

  JsonObject empty_input;
  ExpectError(ParseJsonObject("", &empty_input), StatusCode::kInvalidJson);

  JsonObject top_level_array;
  ExpectError(ParseJsonObject("[\"a\"]", &top_level_array),
              StatusCode::kInvalidJson);

  JsonObject top_level_null;
  ExpectError(ParseJsonObject("null", &top_level_null),
              StatusCode::kInvalidJson);

  JsonObject non_string_key;
  ExpectError(ParseJsonObject("{1:\"a\"}", &non_string_key),
              StatusCode::kInvalidJson);

  JsonObject missing_colon;
  ExpectError(ParseJsonObject("{\"a\" \"b\"}", &missing_colon),
              StatusCode::kInvalidJson);

  JsonObject trailing_comma;
  ExpectError(ParseJsonObject("{\"a\":\"1\",}", &trailing_comma),
              StatusCode::kInvalidJson);

  ExpectError(ParseJsonObject("{}", nullptr), StatusCode::kInternalError);

  JsonObject whitespace;
  status = ParseJsonObject("  {\"a\": \"1\"}\n", &whitespace);
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_TRUE(whitespace.GetString("a", &value));
  MK_EXPECT_EQ(value, std::string("1"));

  JsonObject built;
  MK_EXPECT_TRUE(built.SetString("b", "2").ok());
  MK_EXPECT_TRUE(built.SetString("a", "1").ok());
  MK_EXPECT_TRUE(built.SetNull("c").ok());
  MK_EXPECT_EQ(built.Serialize(), std::string("{\"b\":\"2\",\"a\":\"1\",\"c\":null}"));
  MK_EXPECT_EQ(built.items().size(), static_cast<std::size_t>(3));
  MK_EXPECT_TRUE(built.SetString("a", "3").ok());
  MK_EXPECT_EQ(built.Serialize(), std::string("{\"b\":\"2\",\"a\":\"3\",\"c\":null}"));

  JsonObject invalid_key;
  ExpectError(invalid_key.SetString(InvalidUtf8(), "v"),
              StatusCode::kInvalidUtf8);
  ExpectError(invalid_key.SetString("k", InvalidUtf8()),
              StatusCode::kInvalidUtf8);
  ExpectError(invalid_key.SetNull(InvalidUtf8()), StatusCode::kInvalidUtf8);

  JsonObject cleared;
  MK_EXPECT_TRUE(cleared.SetString("a", "1").ok());
  cleared.Clear();
  MK_EXPECT_FALSE(cleared.Has("a"));
  MK_EXPECT_EQ(cleared.Serialize(), std::string("{}"));

  const std::string long_value(1024u * 1024u, 'a');
  JsonObject long_object;
  MK_EXPECT_TRUE(long_object.SetString("value", long_value).ok());
  const std::string long_line = long_object.Serialize();
  JsonObject long_parsed;
  status = ParseJsonObject(long_line, &long_parsed);
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_TRUE(long_parsed.RequireString("value", &value).ok());
  MK_EXPECT_EQ(value.size(), long_value.size());

  MK_EXPECT_TRUE(minikafka::common::IsValidUtf8(""));
  MK_EXPECT_TRUE(minikafka::common::IsValidUtf8("abc"));
  MK_EXPECT_TRUE(minikafka::common::IsValidUtf8(std::string("\xC3\xA9", 2)));
  MK_EXPECT_TRUE(
      minikafka::common::IsValidUtf8(std::string("\xE4\xBD\xA0", 3)));
  MK_EXPECT_TRUE(
      minikafka::common::IsValidUtf8(std::string("\xF0\x9F\x98\x80", 4)));
  MK_EXPECT_FALSE(minikafka::common::IsValidUtf8(std::string("\xC0\x80", 2)));
  MK_EXPECT_FALSE(
      minikafka::common::IsValidUtf8(std::string("\xED\xA0\x80", 3)));
  MK_EXPECT_FALSE(
      minikafka::common::IsValidUtf8(std::string("\xF4\x90\x80\x80", 4)));
  MK_EXPECT_FALSE(minikafka::common::IsValidUtf8(std::string("\xE4\xBD", 2)));

  MK_EXPECT_TRUE(minikafka::common::ValidateUtf8("abc").ok());
  ExpectError(minikafka::common::ValidateUtf8(InvalidUtf8()),
              StatusCode::kInvalidUtf8);
}
