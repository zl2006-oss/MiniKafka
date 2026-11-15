#include "test_common.h"

#include <iostream>

void TestCrc32();
void TestFrame();
void TestFrameDecoder();
void TestMessages();
void TestProduce();

int mk_test::g_failures = 0;

int main() {
  TestCrc32();
  TestFrame();
  TestFrameDecoder();
  TestMessages();
  TestProduce();

  if (mk_test::g_failures != 0) {
    std::cerr << mk_test::g_failures << " protocol test failure(s)\n";
    return 1;
  }

  std::cout << "all protocol tests passed\n";
  return 0;
}
