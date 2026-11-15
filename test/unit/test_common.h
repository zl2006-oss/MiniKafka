#ifndef MINIKAFKA_TEST_COMMON_H_
#define MINIKAFKA_TEST_COMMON_H_

#include <iostream>
#include <string>

namespace mk_test {

extern int g_failures;

inline void Fail(const char* file, int line, const std::string& message) {
  ++g_failures;
  std::cerr << file << ":" << line << ": " << message << "\n";
}

}  // namespace mk_test

#define MK_EXPECT_TRUE(expr)                                              \
  do {                                                                    \
    if (!(expr)) {                                                        \
      mk_test::Fail(__FILE__, __LINE__, "expected true: " #expr);          \
    }                                                                     \
  } while (0)

#define MK_EXPECT_FALSE(expr)                                             \
  do {                                                                    \
    if ((expr)) {                                                         \
      mk_test::Fail(__FILE__, __LINE__, "expected false: " #expr);         \
    }                                                                     \
  } while (0)

#define MK_EXPECT_EQ(actual, expected)                                    \
  do {                                                                    \
    if (!((actual) == (expected))) {                                      \
      mk_test::Fail(__FILE__, __LINE__,                                   \
                    "expected equality: " #actual " == " #expected);      \
    }                                                                     \
  } while (0)

#define MK_EXPECT_NE(actual, expected)                                    \
  do {                                                                    \
    if (!((actual) != (expected))) {                                      \
      mk_test::Fail(__FILE__, __LINE__,                                   \
                    "expected inequality: " #actual " != " #expected);    \
    }                                                                     \
  } while (0)

#endif  // MINIKAFKA_TEST_COMMON_H_
