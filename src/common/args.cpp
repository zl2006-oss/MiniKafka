#include "minikafka/common/args.h"

#include <climits>
#include <cstddef>
#include <cstdlib>
#include <utility>

namespace minikafka {
namespace common {

void Args::AddString(const std::string& name,
                     const std::string& default_value,
                     bool required) {
  Option option;
  option.name = name;
  option.type = Type::kString;
  option.required = required;
  option.default_string = default_value;
  option.string_value = default_value;
  Add(std::move(option));
}

void Args::AddInt(const std::string& name, int default_value, bool required) {
  Option option;
  option.name = name;
  option.type = Type::kInt;
  option.required = required;
  option.default_int = default_value;
  option.int_value = default_value;
  Add(std::move(option));
}

void Args::AddBool(const std::string& name, bool default_value) {
  Option option;
  option.name = name;
  option.type = Type::kBool;
  option.default_bool = default_value;
  option.bool_value = default_value;
  Add(std::move(option));
}

void Args::Add(Option option) {
  if (!IsValidName(option.name)) {
    std::abort();
  }
  for (const Option& existing : options_) {
    if (existing.name == option.name) {
      std::abort();
    }
  }
  options_.push_back(std::move(option));
}

Status Args::Parse(const std::vector<std::string>& tokens) {
  for (Option& option : options_) {
    option.provided = false;
    option.string_value = option.default_string;
    option.int_value = option.default_int;
    option.bool_value = option.default_bool;
  }

  std::size_t index = 0;
  while (index < tokens.size()) {
    const std::string& token = tokens[index];
    if (token.size() < 3 || token[0] != '-' || token[1] != '-') {
      return Status(StatusCode::kUsage,
                    "unexpected argument '" + token + "'");
    }

    const std::string name = token.substr(2);
    Option* option = Find(name);
    if (option == nullptr) {
      return Status(StatusCode::kUsage,
                    "unknown option '--" + name + "'");
    }
    if (option->provided) {
      return Status(StatusCode::kUsage,
                    "option '--" + name + "' specified more than once");
    }

    option->provided = true;
    if (option->type == Type::kBool) {
      option->bool_value = true;
      ++index;
      continue;
    }

    if (index + 1 >= tokens.size()) {
      return Status(StatusCode::kUsage,
                    "option '--" + name + "' requires a value");
    }
    const std::string& value = tokens[index + 1];
    if (option->type == Type::kString) {
      option->string_value = value;
    } else {
      int parsed = 0;
      if (!ParseInt(value, &parsed)) {
        return Status(StatusCode::kUsage,
                      "option '--" + name +
                          "' requires an integer, got '" + value + "'");
      }
      option->int_value = parsed;
    }
    index += 2;
  }

  for (const Option& option : options_) {
    if (option.required && !option.provided) {
      return Status(StatusCode::kUsage,
                    "missing required option '--" + option.name + "'");
    }
  }
  return Status();
}

Status Args::Parse(int argc, const char* const argv[], int start_index) {
  if (start_index < 0 || start_index > argc) {
    return Status(StatusCode::kUsage, "invalid argument start index");
  }

  std::vector<std::string> tokens;
  for (int i = start_index; i < argc; ++i) {
    tokens.emplace_back(argv[i] == nullptr ? "" : argv[i]);
  }
  return Parse(tokens);
}

bool Args::Has(const std::string& name) const {
  const Option* option = Find(name);
  return option != nullptr && option->provided;
}

const std::string& Args::GetString(const std::string& name) const {
  const Option* option = Find(name);
  if (option == nullptr || option->type != Type::kString) {
    std::abort();
  }
  return option->string_value;
}

int Args::GetInt(const std::string& name) const {
  const Option* option = Find(name);
  if (option == nullptr || option->type != Type::kInt) {
    std::abort();
  }
  return option->int_value;
}

bool Args::GetBool(const std::string& name) const {
  const Option* option = Find(name);
  if (option == nullptr || option->type != Type::kBool) {
    std::abort();
  }
  return option->bool_value;
}

Args::Option* Args::Find(const std::string& name) {
  for (Option& option : options_) {
    if (option.name == name) {
      return &option;
    }
  }
  return nullptr;
}

const Args::Option* Args::Find(const std::string& name) const {
  for (const Option& option : options_) {
    if (option.name == name) {
      return &option;
    }
  }
  return nullptr;
}

bool Args::IsValidName(const std::string& name) {
  if (name.empty() || name[0] < 'a' || name[0] > 'z') {
    return false;
  }
  for (std::size_t i = 1; i < name.size(); ++i) {
    const char c = name[i];
    const bool valid = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
                       c == '-';
    if (!valid) {
      return false;
    }
  }
  return true;
}

bool Args::ParseInt(const std::string& text, int* value) {
  if (value == nullptr || text.empty()) {
    return false;
  }

  const bool negative = text[0] == '-';
  const std::size_t first_digit = negative ? 1u : 0u;
  if (first_digit == text.size()) {
    return false;
  }
  for (std::size_t i = first_digit; i < text.size(); ++i) {
    if (text[i] < '0' || text[i] > '9') {
      return false;
    }
  }

  const long long limit = negative ? 2147483648LL : INT_MAX;
  long long parsed = 0;
  for (std::size_t i = first_digit; i < text.size(); ++i) {
    const int digit = text[i] - '0';
    if (parsed > (limit - digit) / 10) {
      return false;
    }
    parsed = parsed * 10 + digit;
  }

  if (negative) {
    if (parsed == 2147483648LL) {
      *value = INT_MIN;
    } else {
      *value = -static_cast<int>(parsed);
    }
  } else {
    *value = static_cast<int>(parsed);
  }
  return true;
}

}  // namespace common
}  // namespace minikafka
