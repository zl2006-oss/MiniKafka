#include "test_common.h"

#include <iostream>

void TestStorageRecord();
void TestStoragePath();
void TestStorageSegment();
void TestStorageIndex();
void TestStorageOffset();
void TestStorageMeta();
void TestStorageRecovery();
void TestStorageWorker();

int mk_test::g_failures = 0;

int main() {
  TestStorageRecord();
  TestStoragePath();
  TestStorageSegment();
  TestStorageIndex();
  TestStorageOffset();
  TestStorageMeta();
  TestStorageRecovery();
  TestStorageWorker();

  if (mk_test::g_failures != 0) {
    std::cerr << mk_test::g_failures << " storage test failure(s)\n";
    return 1;
  }

  std::cout << "all storage tests passed\n";
  return 0;
}
