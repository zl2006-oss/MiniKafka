#include "test_common.h"
#include "test_storage_util.h"

#include <sys/stat.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iterator>
#include <string>
#include <utility>
#include <vector>

#include "minikafka/common/status.h"
#include "minikafka/storage/paths.h"
#include "minikafka/storage/topic_meta.h"

namespace {

using minikafka::common::Status;
using minikafka::common::StatusCode;
using minikafka::common::StatusOr;
using minikafka::storage::kTopicMetaFooterSize;
using minikafka::storage::kTopicMetaHeaderSize;
using minikafka::storage::TopicMetadata;
using minikafka::storage::TopicMetadataStore;
using minikafka::storage::TopicScanResult;

TopicMetadata MakeMetadata(const std::string& name,
                           std::uint32_t partition_count) {
  TopicMetadata metadata;
  metadata.name = name;
  metadata.partition_count = partition_count;
  return metadata;
}

std::string ReadFile(const std::string& path) {
  std::ifstream file(path.c_str(), std::ios::binary);
  return std::string(std::istreambuf_iterator<char>(file),
                     std::istreambuf_iterator<char>());
}

void WriteFile(const std::string& path, const std::string& data) {
  std::ofstream file(path.c_str(), std::ios::binary | std::ios::trunc);
  file.write(data.data(), static_cast<std::streamsize>(data.size()));
}

void ExpectStatus(const Status& status, StatusCode code) {
  MK_EXPECT_FALSE(status.ok());
  MK_EXPECT_EQ(status.code(), code);
}

const TopicMetadata* FindTopic(const TopicScanResult& result,
                               const std::string& name) {
  for (std::size_t i = 0; i < result.topics.size(); ++i) {
    if (result.topics[i].name == name) {
      return &result.topics[i];
    }
  }
  return nullptr;
}

bool HasWarningContaining(const TopicScanResult& result,
                          const std::string& needle) {
  for (std::size_t i = 0; i < result.warnings.size(); ++i) {
    if (result.warnings[i].find(needle) != std::string::npos) {
      return true;
    }
  }
  return false;
}

void TestMetaCodecRoundTrip() {
  const TopicMetadata metadata = MakeMetadata("orders", 4u);
  std::string bytes;
  Status status =
      minikafka::storage::EncodeTopicMetadata(metadata, &bytes);
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_EQ(bytes.size(),
               kTopicMetaHeaderSize + metadata.name.size() +
                   kTopicMetaFooterSize);

  TopicMetadata decoded;
  status = minikafka::storage::DecodeTopicMetadata(bytes.data(), bytes.size(),
                                                   &decoded);
  MK_EXPECT_TRUE(status.ok());
  MK_EXPECT_EQ(decoded.name, std::string("orders"));
  MK_EXPECT_EQ(decoded.partition_count, 4u);

  std::string corrupt = bytes;
  corrupt[corrupt.size() - 1u] ^= static_cast<char>(0xFF);
  MK_EXPECT_FALSE(minikafka::storage::DecodeTopicMetadata(
                      corrupt.data(), corrupt.size(), &decoded)
                      .ok());

  std::string ignored;
  ExpectStatus(minikafka::storage::EncodeTopicMetadata(
                   MakeMetadata("Bad", 1u), &ignored),
               StatusCode::kInvalidTopicName);
  ExpectStatus(minikafka::storage::EncodeTopicMetadata(
                   MakeMetadata("", 1u), &ignored),
               StatusCode::kInvalidTopicName);
  ExpectStatus(minikafka::storage::EncodeTopicMetadata(
                   MakeMetadata("orders", 0u), &ignored),
               StatusCode::kInvalidPartitions);
  ExpectStatus(minikafka::storage::EncodeTopicMetadata(
                   MakeMetadata("orders", 17u), &ignored),
               StatusCode::kInvalidPartitions);
}

void TestCreateLoadAndAtomicWrite() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  StatusOr<TopicMetadataStore> opened = TopicMetadataStore::Open(dir.path());
  MK_EXPECT_TRUE(opened.ok());
  if (!opened.ok()) {
    return;
  }
  TopicMetadataStore store = std::move(opened).value();

  const TopicMetadata metadata = MakeMetadata("orders", 4u);
  MK_EXPECT_TRUE(store.Create(metadata).ok());

  std::string path;
  MK_EXPECT_TRUE(minikafka::storage::MetaPath(dir.path(), "orders", &path).ok());
  MK_EXPECT_TRUE(::access(path.c_str(), F_OK) == 0);
  MK_EXPECT_TRUE(::access((path + ".tmp").c_str(), F_OK) != 0);

  const std::string first_bytes = ReadFile(path);
  MK_EXPECT_FALSE(first_bytes.empty());

  StatusOr<TopicMetadata> loaded = store.Load("orders");
  MK_EXPECT_TRUE(loaded.ok());
  if (loaded.ok()) {
    MK_EXPECT_EQ(loaded.value().name, std::string("orders"));
    MK_EXPECT_EQ(loaded.value().partition_count, 4u);
  }

  TopicScanResult scan;
  MK_EXPECT_TRUE(store.Scan(&scan).ok());
  MK_EXPECT_EQ(scan.topics.size(), static_cast<std::size_t>(1));
  MK_EXPECT_TRUE(scan.warnings.empty());
  const TopicMetadata* scanned = FindTopic(scan, "orders");
  MK_EXPECT_TRUE(scanned != nullptr);
  if (scanned != nullptr) {
    MK_EXPECT_EQ(scanned->partition_count, 4u);
  }

  ExpectStatus(store.Create(MakeMetadata("orders", 2u)),
               StatusCode::kTopicExists);
  MK_EXPECT_EQ(ReadFile(path), first_bytes);
}

void TestCorruptMetaSkippedAndSameNameRejected() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  StatusOr<TopicMetadataStore> opened = TopicMetadataStore::Open(dir.path());
  MK_EXPECT_TRUE(opened.ok());
  if (!opened.ok()) {
    return;
  }
  TopicMetadataStore store = std::move(opened).value();

  MK_EXPECT_TRUE(store.Create(MakeMetadata("alpha", 1u)).ok());
  MK_EXPECT_TRUE(store.Create(MakeMetadata("beta", 2u)).ok());

  std::string alpha_path;
  std::string beta_path;
  MK_EXPECT_TRUE(
      minikafka::storage::MetaPath(dir.path(), "alpha", &alpha_path).ok());
  MK_EXPECT_TRUE(
      minikafka::storage::MetaPath(dir.path(), "beta", &beta_path).ok());
  WriteFile(alpha_path, "corrupt-meta");
  WriteFile(alpha_path + ".tmp", "leftover-tmp");

  TopicScanResult scan;
  MK_EXPECT_TRUE(store.Scan(&scan).ok());
  MK_EXPECT_EQ(scan.topics.size(), static_cast<std::size_t>(1));
  MK_EXPECT_TRUE(FindTopic(scan, "alpha") == nullptr);
  MK_EXPECT_TRUE(FindTopic(scan, "beta") != nullptr);
  MK_EXPECT_FALSE(scan.warnings.empty());
  MK_EXPECT_TRUE(HasWarningContaining(scan, "alpha"));
  MK_EXPECT_TRUE(::access(alpha_path.c_str(), F_OK) == 0);

  MK_EXPECT_FALSE(store.Load("alpha").ok());
  StatusOr<TopicMetadata> beta = store.Load("beta");
  MK_EXPECT_TRUE(beta.ok());
  if (beta.ok()) {
    MK_EXPECT_EQ(beta.value().partition_count, 2u);
  }

  ExpectStatus(store.Create(MakeMetadata("alpha", 3u)),
               StatusCode::kTopicExists);
  MK_EXPECT_EQ(ReadFile(alpha_path), std::string("corrupt-meta"));
  MK_EXPECT_TRUE(::access(beta_path.c_str(), F_OK) == 0);
}

void TestPartitionCountOutOfRangeRejected() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  StatusOr<TopicMetadataStore> opened = TopicMetadataStore::Open(dir.path());
  MK_EXPECT_TRUE(opened.ok());
  if (!opened.ok()) {
    return;
  }
  TopicMetadataStore store = std::move(opened).value();

  ExpectStatus(store.Create(MakeMetadata("bad0", 0u)),
               StatusCode::kInvalidPartitions);
  ExpectStatus(store.Create(MakeMetadata("bad17", 17u)),
               StatusCode::kInvalidPartitions);

  std::string path;
  MK_EXPECT_TRUE(minikafka::storage::MetaPath(dir.path(), "bad0", &path).ok());
  MK_EXPECT_TRUE(::access(path.c_str(), F_OK) != 0);
  MK_EXPECT_TRUE(minikafka::storage::MetaPath(dir.path(), "bad17", &path).ok());
  MK_EXPECT_TRUE(::access(path.c_str(), F_OK) != 0);
}

void TestFilenameInternalNameMismatchSkipped() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  StatusOr<TopicMetadataStore> opened = TopicMetadataStore::Open(dir.path());
  MK_EXPECT_TRUE(opened.ok());
  if (!opened.ok()) {
    return;
  }
  TopicMetadataStore store = std::move(opened).value();

  MK_EXPECT_TRUE(store.Create(MakeMetadata("beta", 3u)).ok());
  std::string beta_path;
  std::string alpha_path;
  MK_EXPECT_TRUE(
      minikafka::storage::MetaPath(dir.path(), "beta", &beta_path).ok());
  MK_EXPECT_TRUE(
      minikafka::storage::MetaPath(dir.path(), "alpha", &alpha_path).ok());
  WriteFile(alpha_path, ReadFile(beta_path));

  TopicScanResult scan;
  MK_EXPECT_TRUE(store.Scan(&scan).ok());
  MK_EXPECT_TRUE(FindTopic(scan, "alpha") == nullptr);
  MK_EXPECT_TRUE(FindTopic(scan, "beta") != nullptr);
  MK_EXPECT_TRUE(HasWarningContaining(scan, "alpha"));

  ExpectStatus(store.Create(MakeMetadata("alpha", 1u)),
               StatusCode::kTopicExists);
}

void TestTmpFileIgnoredAndCreateAllowed() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  StatusOr<TopicMetadataStore> opened = TopicMetadataStore::Open(dir.path());
  MK_EXPECT_TRUE(opened.ok());
  if (!opened.ok()) {
    return;
  }
  TopicMetadataStore store = std::move(opened).value();

  const std::string meta_dir = dir.path() + "/meta";
  MK_EXPECT_EQ(::mkdir(meta_dir.c_str(), 0755), 0);

  std::string path;
  MK_EXPECT_TRUE(minikafka::storage::MetaPath(dir.path(), "orders", &path).ok());
  WriteFile(path + ".tmp", "interrupted-meta");
  MK_EXPECT_TRUE(::access(path.c_str(), F_OK) != 0);

  TopicScanResult scan;
  MK_EXPECT_TRUE(store.Scan(&scan).ok());
  MK_EXPECT_TRUE(scan.topics.empty());
  MK_EXPECT_TRUE(scan.warnings.empty());

  MK_EXPECT_TRUE(store.Create(MakeMetadata("orders", 2u)).ok());
  MK_EXPECT_TRUE(::access(path.c_str(), F_OK) == 0);
  MK_EXPECT_TRUE(::access((path + ".tmp").c_str(), F_OK) != 0);
  StatusOr<TopicMetadata> loaded = store.Load("orders");
  MK_EXPECT_TRUE(loaded.ok());
  if (loaded.ok()) {
    MK_EXPECT_EQ(loaded.value().partition_count, 2u);
  }
}

}  // namespace

void TestStorageMeta() {
  TestMetaCodecRoundTrip();
  TestCreateLoadAndAtomicWrite();
  TestCorruptMetaSkippedAndSameNameRejected();
  TestPartitionCountOutOfRangeRejected();
  TestFilenameInternalNameMismatchSkipped();
  TestTmpFileIgnoredAndCreateAllowed();
}
