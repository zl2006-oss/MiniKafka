#include "test_common.h"

#include <iostream>

void TestStatus();
void TestJson();
void TestArgs();
void TestClock();
void TestBackoff();
void TestEventSink();
void TestHash();

int mk_test::g_failures = 0;

int main() {
  TestStatus();
  TestJson();
  TestArgs();
  TestClock();
  TestBackoff();
  TestEventSink();
  TestHash();

  if (mk_test::g_failures != 0) {
    std::cerr << mk_test::g_failures << " common test failure(s)\n";
    return 1;
  }

  std::cout << "all common tests passed\n";
  return 0;
}
