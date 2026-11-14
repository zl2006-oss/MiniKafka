#include <cstring>
#include <iostream>

namespace {

void PrintUsage() {
  std::cerr << "usage: minikafka <broker|topic|produce|consume|replay|bench> [options]\n";
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    PrintUsage();
    return 2;
  }

  const char* const kCommands[] = {"broker",  "topic",  "produce",
                                   "consume", "replay", "bench"};
  for (const char* command : kCommands) {
    if (std::strcmp(argv[1], command) == 0) {
      std::cerr << "not implemented: " << command << "\n";
      return 2;
    }
  }

  PrintUsage();
  return 2;
}
