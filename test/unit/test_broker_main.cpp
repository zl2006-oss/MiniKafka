#include "test_common.h"

#include <iostream>

void TestBrokerTopic();
void TestBrokerService();
void TestBrokerEndToEnd();

int mk_test::g_failures = 0;

int main() {
  TestBrokerTopic();
  TestBrokerService();
  TestBrokerEndToEnd();

  if (mk_test::g_failures != 0) {
    std::cerr << mk_test::g_failures << " broker test failure(s)\n";
    return 1;
  }

  std::cout << "all broker tests passed\n";
  return 0;
}
