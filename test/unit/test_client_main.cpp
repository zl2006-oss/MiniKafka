#include "test_common.h"

#include <iostream>

void TestClientCli();

int mk_test::g_failures = 0;

int main() {
  TestClientCli();

  if (mk_test::g_failures != 0) {
    std::cerr << mk_test::g_failures << " client test failure(s)\n";
    return 1;
  }

  std::cout << "all client tests passed\n";
  return 0;
}
