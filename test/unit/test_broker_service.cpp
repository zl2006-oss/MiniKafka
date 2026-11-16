#include "test_common.h"
#include "test_storage_util.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "minikafka/broker/partition_manager.h"
#include "minikafka/broker/service.h"
#include "minikafka/common/status.h"
#include "minikafka/protocol/messages.h"
#include "minikafka/storage/record.h"

namespace {

using minikafka::broker::kMaxFetchRecords;
using minikafka::broker::PartitionKeyFor;
using minikafka::broker::PartitionManager;
using minikafka::broker::PartitionService;
using minikafka::common::StatusCode;
using minikafka::protocol::FetchResponse;
using minikafka::protocol::ProduceEntry;
using minikafka::protocol::ProduceRequest;
using minikafka::protocol::ProduceResponse;
using minikafka::protocol::ReplayRequest;
using minikafka::protocol::FetchRequest;
using minikafka::storage::kFlagKeyPresent;

const std::uint64_t kSegmentBytes = 64u * 1024u;
const std::uint64_t kDiskThreshold = 1u;

ProduceEntry MakeEntry(const std::string& key, const std::string& value) {
  ProduceEntry entry;
  entry.flags = kFlagKeyPresent;
  entry.key = key;
  entry.value = value;
  return entry;
}

ProduceRequest MakeProduce(const std::string& topic, std::uint32_t partition,
                           const std::vector<ProduceEntry>& entries) {
  ProduceRequest request;
  request.topic = topic;
  request.partition = partition;
  request.entries = entries;
  return request;
}

// 打开一个含 topic=t（2 分区）的管理器；失败返回 false。
bool OpenManager(const std::string& data_dir, PartitionManager** out,
                 std::uint64_t max_record_bytes =
                     minikafka::storage::kMaxRecordSize) {
  PartitionManager* manager = new PartitionManager(
      data_dir, kSegmentBytes, kDiskThreshold, 2u, max_record_bytes);
  const minikafka::common::Status status = manager->OpenTopic("t", 2u);
  if (!status.ok()) {
    delete manager;
    return false;
  }
  *out = manager;
  return true;
}

// A.1 主链路：produce 分配 offset，fetch 原样读回。
void TestProduceThenFetch() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  PartitionManager* manager = nullptr;
  MK_EXPECT_TRUE(OpenManager(dir.path(), &manager));
  if (manager == nullptr) {
    return;
  }
  PartitionService service(manager);

  std::vector<ProduceEntry> entries;
  entries.push_back(MakeEntry("k0", "v0"));
  entries.push_back(MakeEntry("k1", "v1"));
  const ProduceResponse produced =
      service.Produce(MakeProduce("t", 0u, entries));
  MK_EXPECT_EQ(produced.error_code, StatusCode::kOk);
  MK_EXPECT_EQ(produced.status, 0u);
  MK_EXPECT_EQ(produced.results.size(), 2u);
  if (produced.results.size() == 2u) {
    MK_EXPECT_EQ(produced.results[0].index, 0u);
    MK_EXPECT_EQ(produced.results[0].status, 0u);
    MK_EXPECT_EQ(produced.results[0].offset, 0u);
    MK_EXPECT_EQ(produced.results[1].offset, 1u);
  }

  FetchRequest request;
  request.topic = "t";
  request.partition = 0u;
  request.offset = 0u;
  request.max_bytes = 0u;
  const FetchResponse fetched = service.Fetch(request);
  MK_EXPECT_EQ(fetched.error_code, StatusCode::kOk);
  MK_EXPECT_EQ(fetched.log_end_offset, 2u);
  MK_EXPECT_EQ(fetched.records.size(), 2u);
  if (fetched.records.size() == 2u) {
    MK_EXPECT_EQ(fetched.records[0].offset, 0u);
    MK_EXPECT_EQ(fetched.records[0].key, std::string("k0"));
    MK_EXPECT_EQ(fetched.records[0].value, std::string("v0"));
    MK_EXPECT_EQ(fetched.records[1].offset, 1u);
    MK_EXPECT_EQ(fetched.records[1].value, std::string("v1"));
    MK_EXPECT_EQ(fetched.records[1].flags, kFlagKeyPresent);
  }

  // 重启后 offset 从磁盘恢复继续递增（V1）。
  delete manager;
  manager = nullptr;
  MK_EXPECT_TRUE(OpenManager(dir.path(), &manager));
  if (manager == nullptr) {
    return;
  }
  PartitionService restarted(manager);
  const ProduceResponse again =
      restarted.Produce(MakeProduce("t", 0u, {MakeEntry("k2", "v2")}));
  MK_EXPECT_EQ(again.status, 0u);
  if (again.results.size() == 1u) {
    MK_EXPECT_EQ(again.results[0].offset, 2u);
  }
  delete manager;
}

// 请求级错误：topic 不存在 / 分区越界 → status=2 且 result_count=0。
void TestProduceRequestErrors() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  PartitionManager* manager = nullptr;
  MK_EXPECT_TRUE(OpenManager(dir.path(), &manager));
  if (manager == nullptr) {
    return;
  }
  PartitionService service(manager);

  const ProduceResponse missing = service.Produce(
      MakeProduce("nope", 0u, {MakeEntry("k", "v")}));
  MK_EXPECT_EQ(missing.error_code, StatusCode::kTopicNotFound);
  MK_EXPECT_EQ(missing.status, 2u);
  MK_EXPECT_TRUE(missing.results.empty());
  MK_EXPECT_FALSE(missing.message.empty());

  const ProduceResponse bad_partition = service.Produce(
      MakeProduce("t", 2u, {MakeEntry("k", "v")}));
  MK_EXPECT_EQ(bad_partition.error_code, StatusCode::kInvalidPartition);
  MK_EXPECT_EQ(bad_partition.status, 2u);
  MK_EXPECT_TRUE(bad_partition.results.empty());

  const ProduceResponse empty =
      service.Produce(MakeProduce("t", 0u, std::vector<ProduceEntry>()));
  MK_EXPECT_EQ(empty.error_code, StatusCode::kInvalidBatch);
  MK_EXPECT_EQ(empty.status, 2u);
  delete manager;
}

// AD-14：同分区首个失败后，后续条目 not_attempted（partition_blocked）。
void TestProduceBlocksAfterFailure() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  PartitionManager* manager = nullptr;
  MK_EXPECT_TRUE(OpenManager(dir.path(), &manager));
  if (manager == nullptr) {
    return;
  }
  PartitionService service(manager);

  const std::string too_large(1024u * 1024u + 1u, 'x');
  std::vector<ProduceEntry> entries;
  entries.push_back(MakeEntry("k", too_large));
  entries.push_back(MakeEntry("k", "small"));
  const ProduceResponse response =
      service.Produce(MakeProduce("t", 0u, entries));

  MK_EXPECT_EQ(response.error_code, StatusCode::kOk);
  MK_EXPECT_EQ(response.status, 2u);  // 无条目追加
  MK_EXPECT_EQ(response.results.size(), 2u);
  if (response.results.size() == 2u) {
    MK_EXPECT_EQ(response.results[0].status, 1u);
    MK_EXPECT_EQ(response.results[0].error_code, StatusCode::kMessageTooLarge);
    MK_EXPECT_EQ(response.results[0].offset, 0u);
    MK_EXPECT_EQ(response.results[1].status, 2u);
    MK_EXPECT_EQ(response.results[1].error_code,
                 StatusCode::kPartitionBlocked);
  }

  // 失败条目没有写入任何字节：分区仍然从 offset 0 开始。
  FetchRequest request;
  request.topic = "t";
  request.partition = 0u;
  request.offset = 0u;
  const FetchResponse fetched = service.Fetch(request);
  MK_EXPECT_EQ(fetched.error_code, StatusCode::kOk);
  MK_EXPECT_EQ(fetched.log_end_offset, 0u);
  MK_EXPECT_TRUE(fetched.records.empty());
  delete manager;
}

// R17：Broker 端 --max-record-bytes 调低后，超限条目被拒且不占 offset。
void TestProduceConfiguredRecordLimit() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  PartitionManager* manager = nullptr;
  MK_EXPECT_TRUE(OpenManager(dir.path(), &manager, 64u));
  if (manager == nullptr) {
    return;
  }
  PartitionService service(manager);

  std::vector<ProduceEntry> entries;
  entries.push_back(MakeEntry("k", std::string(50u, 'v')));
  entries.push_back(MakeEntry("k", std::string(39u, 'v')));
  const ProduceResponse response =
      service.Produce(MakeProduce("t", 0u, entries));

  MK_EXPECT_EQ(response.status, 2u);
  MK_EXPECT_EQ(response.results.size(), 2u);
  if (response.results.size() == 2u) {
    MK_EXPECT_EQ(response.results[0].status, 1u);
    MK_EXPECT_EQ(response.results[0].error_code, StatusCode::kMessageTooLarge);
    MK_EXPECT_EQ(response.results[1].status, 2u);
    MK_EXPECT_EQ(response.results[1].error_code,
                 StatusCode::kPartitionBlocked);
  }

  // 失败条目未占 offset：后续恰好 64B 的记录仍从 offset 0 开始。
  const ProduceResponse ok =
      service.Produce(MakeProduce("t", 0u, {MakeEntry("k", std::string(39u, 'v'))}));
  MK_EXPECT_EQ(ok.status, 0u);
  if (ok.results.size() == 1u) {
    MK_EXPECT_EQ(ok.results[0].offset, 0u);
  }
  delete manager;
}

// 05 §3.5：offset == log_end 返回空批；offset > log_end 报 out_of_range。
void TestFetchBoundaries() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  PartitionManager* manager = nullptr;
  MK_EXPECT_TRUE(OpenManager(dir.path(), &manager));
  if (manager == nullptr) {
    return;
  }
  PartitionService service(manager);
  MK_EXPECT_EQ(
      service.Produce(MakeProduce("t", 1u, {MakeEntry("k", "v")})).status, 0u);

  FetchRequest at_end;
  at_end.topic = "t";
  at_end.partition = 1u;
  at_end.offset = 1u;
  const FetchResponse empty = service.Fetch(at_end);
  MK_EXPECT_EQ(empty.error_code, StatusCode::kOk);
  MK_EXPECT_EQ(empty.log_end_offset, 1u);
  MK_EXPECT_TRUE(empty.records.empty());

  FetchRequest beyond = at_end;
  beyond.offset = 2u;
  const FetchResponse out = service.Fetch(beyond);
  MK_EXPECT_EQ(out.error_code, StatusCode::kOffsetOutOfRange);
  MK_EXPECT_EQ(out.log_end_offset, 1u);
  MK_EXPECT_FALSE(out.message.empty());

  // 错误响应也必须回填 topic/partition，编码器总会写这两个字段。
  MK_EXPECT_EQ(out.topic, std::string("t"));
  MK_EXPECT_EQ(out.partition, 1u);

  FetchRequest missing_topic = at_end;
  missing_topic.topic = "nope";
  const FetchResponse missing = service.Fetch(missing_topic);
  MK_EXPECT_EQ(missing.error_code, StatusCode::kTopicNotFound);
  delete manager;
}

// max_bytes 限制条数，但至少返回一条。
void TestFetchMaxBytes() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  PartitionManager* manager = nullptr;
  MK_EXPECT_TRUE(OpenManager(dir.path(), &manager));
  if (manager == nullptr) {
    return;
  }
  PartitionService service(manager);

  std::vector<ProduceEntry> entries;
  for (int i = 0; i < 3; ++i) {
    entries.push_back(
        MakeEntry("k", std::string(100, static_cast<char>('a' + i))));
  }
  MK_EXPECT_EQ(service.Produce(MakeProduce("t", 0u, entries)).status, 0u);

  FetchRequest small;
  small.topic = "t";
  small.partition = 0u;
  small.offset = 0u;
  small.max_bytes = 150u;  // 单条 24+1+100=125，第二条放不下
  const FetchResponse limited = service.Fetch(small);
  MK_EXPECT_EQ(limited.error_code, StatusCode::kOk);
  MK_EXPECT_EQ(limited.log_end_offset, 3u);
  MK_EXPECT_EQ(limited.records.size(), 1u);
  if (!limited.records.empty()) {
    MK_EXPECT_EQ(limited.records[0].value, std::string(100, 'a'));
  }

  FetchRequest all = small;
  all.max_bytes = 0u;  // 0 → 默认 1 MiB
  const FetchResponse full = service.Fetch(all);
  MK_EXPECT_EQ(full.records.size(), 3u);
  delete manager;
}

// R12：replay 只读，按 count 截断；不改变任何状态。
void TestReplay() {
  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  PartitionManager* manager = nullptr;
  MK_EXPECT_TRUE(OpenManager(dir.path(), &manager));
  if (manager == nullptr) {
    return;
  }
  PartitionService service(manager);

  std::vector<ProduceEntry> entries;
  for (int i = 0; i < 4; ++i) {
    entries.push_back(MakeEntry("k", "v" + std::to_string(i)));
  }
  MK_EXPECT_EQ(service.Produce(MakeProduce("t", 0u, entries)).status, 0u);

  ReplayRequest request;
  request.topic = "t";
  request.partition = 0u;
  request.offset = 1u;
  request.count = 2u;
  const FetchResponse replay = service.Replay(request);
  MK_EXPECT_EQ(replay.error_code, StatusCode::kOk);
  MK_EXPECT_EQ(replay.log_end_offset, 4u);
  MK_EXPECT_EQ(replay.records.size(), 2u);
  if (replay.records.size() == 2u) {
    MK_EXPECT_EQ(replay.records[0].offset, 1u);
    MK_EXPECT_EQ(replay.records[0].value, std::string("v1"));
    MK_EXPECT_EQ(replay.records[1].offset, 2u);
  }

  // count=0 → 读到末尾。
  request.count = 0u;
  const FetchResponse to_end = service.Replay(request);
  MK_EXPECT_EQ(to_end.records.size(), 3u);

  // 重放不写任何字节：末尾 offset 仍是 4。
  FetchRequest fetch;
  fetch.topic = "t";
  fetch.partition = 0u;
  fetch.offset = 0u;
  MK_EXPECT_EQ(service.Fetch(fetch).log_end_offset, 4u);

  // count 超过可读条数 → 返回现有条数并成功。
  request.offset = 2u;
  request.count = kMaxFetchRecords;
  const FetchResponse tail = service.Replay(request);
  MK_EXPECT_EQ(tail.error_code, StatusCode::kOk);
  MK_EXPECT_EQ(tail.records.size(), 2u);

  request.offset = 9u;
  MK_EXPECT_EQ(service.Replay(request).error_code,
               StatusCode::kOffsetOutOfRange);
  delete manager;
}

// AD-05：分区亲和键由 topic 与 partition 共同决定，且跨调用稳定。
void TestPartitionAffinity() {
  const std::uint32_t a0 = PartitionKeyFor("alpha", 0u);
  const std::uint32_t b0 = PartitionKeyFor("beta", 0u);
  const std::uint32_t a1 = PartitionKeyFor("alpha", 1u);
  MK_EXPECT_EQ(PartitionKeyFor("alpha", 0u), a0);
  MK_EXPECT_NE(a0, b0);
  MK_EXPECT_NE(a0, a1);

  mk_test::storage_test::TempDir dir;
  MK_EXPECT_TRUE(dir.ok());
  if (!dir.ok()) {
    return;
  }
  PartitionManager manager(dir.path(), kSegmentBytes, kDiskThreshold, 4u);
  MK_EXPECT_TRUE(manager.OpenTopic("alpha", 4u).ok());
  MK_EXPECT_TRUE(manager.OpenTopic("beta", 4u).ok());
  MK_EXPECT_EQ(manager.topic_count(), 2u);
  MK_EXPECT_EQ(manager.ready_partitions(), 8u);
  MK_EXPECT_EQ(manager.unavailable_partitions(), 0u);
  MK_EXPECT_TRUE(manager.warnings().empty());

  // 同一 (topic, partition) 多次解析到同一 worker 与同一 store。
  void* first = manager.Find("alpha", 2u);
  MK_EXPECT_TRUE(first != nullptr);
  MK_EXPECT_TRUE(first == manager.Find("alpha", 2u));
  MK_EXPECT_TRUE(manager.Find("alpha", 4u) == nullptr);
  MK_EXPECT_TRUE(manager.Find("missing", 0u) == nullptr);
  MK_EXPECT_FALSE(manager.Knows("missing"));
  MK_EXPECT_TRUE(manager.Knows("alpha"));
}

}  // namespace

void TestBrokerService() {
  TestProduceThenFetch();
  TestProduceRequestErrors();
  TestProduceBlocksAfterFailure();
  TestProduceConfiguredRecordLimit();
  TestFetchBoundaries();
  TestFetchMaxBytes();
  TestReplay();
  TestPartitionAffinity();
}
