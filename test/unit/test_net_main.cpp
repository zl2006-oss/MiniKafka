#include "test_common.h"

#include <iostream>

void TestEventLoop();
void TestAcceptor();
void TestReactorPool();

int mk_test::g_failures = 0;

int main() {
  TestEventLoop();
  TestAcceptor();
  TestReactorPool();

  if (mk_test::g_failures != 0) {
    std::cerr << mk_test::g_failures << " net test failure(s)\n";
    return 1;
  }

  std::cout << "all net tests passed\n";
  return 0;
}
