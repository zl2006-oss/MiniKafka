#include "test_common.h"
#include "test_storage_util.h"

#include <cstddef>
#include <cstdio>
#include <string>
#include <utility>
#include <vector>

#include "minikafka/broker/topic_registry.h"
#include "minikafka/common/status.h"

namespace {

using minikafka::broker::TopicEntry;
using minikafka::broker::TopicRegistry;
using minikafka::common::Status;
using minikafka::common::StatusCode;
using minikafka::common::StatusOr;

std::string MetaPath(const std::string& data_dir, const std::string& topic) {
  return data_dir + "/meta/" + topic + ".meta";
}

void ExpectStatus(const Status& status, StatusCode code) {
  MK_EXPECT_FALSE(status.ok());
  MK_EXPECT_EQ(status.code(), code);
}

// F-T01：创建 → 列表 → 分区数可查；同名拒绝。
void TestCreateAndList() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }

  StatusOr<TopicRegistry> loaded = TopicRegistry::Load(dir.path());
  MK_EXPECT_TRUE(loaded.ok());
  if (!loaded.ok()) {
    return;
  }
  TopicRegistry registry = std::move(loaded).value();
  MK_EXPECT_EQ(registry.size(), 0u);
  MK_EXPECT_TRUE(registry.List().empty());
  MK_EXPECT_FALSE(registry.Contains("orders"));

  MK_EXPECT_TRUE(registry.Create("orders", 3u).ok());
  MK_EXPECT_TRUE(registry.Create("events", 1u).ok());
  MK_EXPECT_EQ(registry.size(), 2u);
  MK_EXPECT_TRUE(registry.Contains("orders"));

  StatusOr<std::uint32_t> count = registry.PartitionCount("orders");
  MK_EXPECT_TRUE(count.ok());
  if (count.ok()) {
    MK_EXPECT_EQ(count.value(), 3u);
  }

  // F-T02：按名称升序返回。
  const std::vector<TopicEntry> entries = registry.List();
  MK_EXPECT_EQ(entries.size(), 2u);
  if (entries.size() == 2u) {
    MK_EXPECT_EQ(entries[0].name, std::string("events"));
    MK_EXPECT_EQ(entries[1].name, std::string("orders"));
    MK_EXPECT_EQ(entries[1].partition_count, 3u);
  }

  // 同名重复创建必须拒绝。
  ExpectStatus(registry.Create("orders", 1u), StatusCode::kTopicExists);

  StatusOr<std::uint32_t> missing = registry.PartitionCount("nope");
  MK_EXPECT_FALSE(missing.ok());
  MK_EXPECT_EQ(missing.status().code(), StatusCode::kTopicNotFound);
}

// R14/Q-P8：名称规则与分区数上下限。
void TestValidation() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  StatusOr<TopicRegistry> loaded = TopicRegistry::Load(dir.path());
  MK_EXPECT_TRUE(loaded.ok());
  if (!loaded.ok()) {
    return;
  }
  TopicRegistry registry = std::move(loaded).value();

  ExpectStatus(registry.Create("Orders", 1u), StatusCode::kInvalidTopicName);
  ExpectStatus(registry.Create("", 1u), StatusCode::kInvalidTopicName);
  ExpectStatus(registry.Create("a b", 1u), StatusCode::kInvalidTopicName);
  ExpectStatus(registry.Create("a/b", 1u), StatusCode::kInvalidTopicName);

  ExpectStatus(registry.Create("ok", 0u), StatusCode::kInvalidPartitions);
  ExpectStatus(registry.Create("ok", 17u), StatusCode::kInvalidPartitions);
  MK_EXPECT_EQ(registry.size(), 0u);

  MK_EXPECT_TRUE(registry.Create("ok", 16u).ok());
  MK_EXPECT_EQ(registry.size(), 1u);
}

// R30：元数据跨重启保留；损坏文件跳过并告警，不阻止加载。
void TestReloadAndCorruptMeta() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }

  {
    StatusOr<TopicRegistry> loaded = TopicRegistry::Load(dir.path());
    MK_EXPECT_TRUE(loaded.ok());
    if (!loaded.ok()) {
      return;
    }
    TopicRegistry registry = std::move(loaded).value();
    MK_EXPECT_TRUE(registry.Create("alpha", 2u).ok());
    MK_EXPECT_TRUE(registry.Create("beta", 4u).ok());
  }

  StatusOr<TopicRegistry> reloaded = TopicRegistry::Load(dir.path());
  MK_EXPECT_TRUE(reloaded.ok());
  if (!reloaded.ok()) {
    return;
  }
  MK_EXPECT_EQ(reloaded.value().size(), 2u);
  MK_EXPECT_TRUE(reloaded.value().warnings().empty());
  StatusOr<std::uint32_t> beta = reloaded.value().PartitionCount("beta");
  MK_EXPECT_TRUE(beta.ok());
  if (beta.ok()) {
    MK_EXPECT_EQ(beta.value(), 4u);
  }

  // 破坏 alpha 的元数据文件后重新加载：alpha 被跳过并记 warning，beta 仍在。
  std::FILE* file = std::fopen(MetaPath(dir.path(), "alpha").c_str(), "wb");
  MK_EXPECT_TRUE(file != nullptr);
  if (file == nullptr) {
    return;
  }
  const char garbage[8] = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07};
  MK_EXPECT_EQ(std::fwrite(garbage, 1u, sizeof(garbage), file),
               sizeof(garbage));
  std::fclose(file);

  StatusOr<TopicRegistry> degraded = TopicRegistry::Load(dir.path());
  MK_EXPECT_TRUE(degraded.ok());
  if (!degraded.ok()) {
    return;
  }
  MK_EXPECT_EQ(degraded.value().size(), 1u);
  MK_EXPECT_FALSE(degraded.value().Contains("alpha"));
  MK_EXPECT_TRUE(degraded.value().Contains("beta"));
  MK_EXPECT_FALSE(degraded.value().warnings().empty());
}

}  // namespace

void TestBrokerTopic() {
  TestCreateAndList();
  TestValidation();
  TestReloadAndCorruptMeta();
}
