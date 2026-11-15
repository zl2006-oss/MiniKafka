#ifndef MINIKAFKA_TEST_STORAGE_UTIL_H_
#define MINIKAFKA_TEST_STORAGE_UTIL_H_

#include <dirent.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include <string>

namespace mk_test {
namespace storage_test {

inline std::string CreateTempDirectory() {
  char pattern[] = "/tmp/minikafka_storage_XXXXXX";
  char* created = ::mkdtemp(pattern);
  return created == nullptr ? std::string() : std::string(created);
}

inline void RemoveTempDirectory(const std::string& directory) {
  if (directory.empty()) {
    return;
  }
  DIR* dir = ::opendir(directory.c_str());
  if (dir != nullptr) {
    while (true) {
      const struct dirent* entry = ::readdir(dir);
      if (entry == nullptr) {
        break;
      }
      const std::string name(entry->d_name);
      if (name == "." || name == "..") {
        continue;
      }
      const std::string path = directory + "/" + name;
      struct stat st;
      if (::lstat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
        RemoveTempDirectory(path);
      } else {
        ::unlink(path.c_str());
      }
    }
    ::closedir(dir);
  }
  ::rmdir(directory.c_str());
}

class TempDir {
 public:
  TempDir() : path_(CreateTempDirectory()) {}
  ~TempDir() { RemoveTempDirectory(path_); }

  const std::string& path() const { return path_; }
  bool ok() const { return !path_.empty(); }

  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;

 private:
  std::string path_;
};

}  // namespace storage_test
}  // namespace mk_test

#endif  // MINIKAFKA_TEST_STORAGE_UTIL_H_
