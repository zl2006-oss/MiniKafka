#include "test_common.h"

#include <iostream>

void TestBenchStats();

int mk_test::g_failures = 0;

int main() {
  TestBenchStats();

  if (mk_test::g_failures != 0) {
    std::cerr << mk_test::g_failures << " bench test failure(s)\n";
    return 1;
  }

  std::cout << "all bench tests passed\n";
  return 0;
}
