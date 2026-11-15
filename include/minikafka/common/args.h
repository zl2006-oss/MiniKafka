#ifndef MINIKAFKA_COMMON_ARGS_H_
#define MINIKAFKA_COMMON_ARGS_H_

#include <string>
#include <vector>

#include "minikafka/common/status.h"

namespace minikafka {
namespace common {

// --kebab-case 参数解析（R20/B4）。
// 名称不带前导 "--"；字符串/整数选项消费下一个 token，布尔开关不消费值。
// 未提供时取注册时的默认值；未知参数、缺值、重复、必填缺失均返回 usage。
class Args {
 public:
  Args() = default;

  void AddString(const std::string& name,
                 const std::string& default_value,
                 bool required = false);
  void AddInt(const std::string& name, int default_value, bool required = false);
  void AddBool(const std::string& name, bool default_value = false);

  // tokens 不包含程序名与子命令。
  Status Parse(const std::vector<std::string>& tokens);
  Status Parse(int argc, const char* const argv[], int start_index = 1);

  bool Has(const std::string& name) const;

  const std::string& GetString(const std::string& name) const;
  int GetInt(const std::string& name) const;
  bool GetBool(const std::string& name) const;

 private:
  enum class Type { kString, kInt, kBool };

  struct Option {
    std::string name;
    Type type = Type::kString;
    bool required = false;
    bool provided = false;
    std::string default_string;
    int default_int = 0;
    bool default_bool = false;
    std::string string_value;
    int int_value = 0;
    bool bool_value = false;
  };

  void Add(Option option);
  Option* Find(const std::string& name);
  const Option* Find(const std::string& name) const;
  static bool IsValidName(const std::string& name);
  static bool ParseInt(const std::string& text, int* value);

  std::vector<Option> options_;
};

}  // namespace common
}  // namespace minikafka

#endif  // MINIKAFKA_COMMON_ARGS_H_
