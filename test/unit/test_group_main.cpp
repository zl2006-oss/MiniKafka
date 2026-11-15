#include "test_common.h"

#include <iostream>

void TestGroupAssignment();
void TestGroupCoordinator();

int mk_test::g_failures = 0;

int main() {
  TestGroupAssignment();
  TestGroupCoordinator();

  if (mk_test::g_failures != 0) {
    std::cerr << mk_test::g_failures << " group test failure(s)\n";
    return 1;
  }

  std::cout << "all group tests passed\n";
  return 0;
}
