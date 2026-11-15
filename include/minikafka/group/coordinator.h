#ifndef MINIKAFKA_GROUP_COORDINATOR_H_
#define MINIKAFKA_GROUP_COORDINATOR_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "minikafka/common/status.h"
#include "minikafka/group/assignment.h"
#include "minikafka/storage/offset_file.h"

namespace minikafka {
namespace group {

struct CommittedOffset {
  std::uint32_t partition = 0;
  bool has_committed = false;
  std::uint64_t committed_offset = 0;
};

struct FetchOutcome {
  // 该槽位负责的分区（升序）。
  std::vector<std::uint32_t> assigned_partitions;
  // 与 assigned_partitions 同序、同长度。
  std::vector<CommittedOffset> entries;
};

// M6 消费组协调：静态分配（Q-P2=A）+ 已提交位点（R11/R28）。
// - 不维护成员表/心跳/会话，成员加入退出不触发重分配；
// - 位点状态与位点文件只由"该 group 所属 Group-shard Storage Worker"访问，
//   因此本类不加锁（03 §4.3 / AD-17）；
// - 不含协议结构：调用方（M5）负责报文编解码，保持 M6 只依赖 M4 + M1。
class GroupCoordinator {
 public:
  // 位点文件损坏降级时的告警出口（R28 要求 Broker 侧 stderr 告警）。
  using WarningSink = std::function<void(const std::string&)>;

  explicit GroupCoordinator(std::string data_dir,
                            WarningSink warning_sink = WarningSink(),
                            const AssignmentStrategy* strategy = nullptr);

  GroupCoordinator(const GroupCoordinator&) = delete;
  GroupCoordinator& operator=(const GroupCoordinator&) = delete;

  // 05 §3.6：返回该槽位的分配与已提交位点；无位点 → has_committed=false。
  // 错误码：kInvalidGroupName / kInvalidTopicName / kInvalidConsumerSlot /
  //         kInvalidPartitions / kStorageIoError。
  common::StatusOr<FetchOutcome> Fetch(const std::string& group,
                                       const std::string& topic,
                                       std::uint32_t consumer_index,
                                       std::uint32_t consumer_count,
                                       std::uint32_t partition_count);

  // 05 §3.7 + S-13：先整体校验，再合并落盘（同分区取 max、不回退）；
  // 任一分区越界则整体不应用。成功返回 applied_count。
  common::StatusOr<std::uint32_t> Commit(
      const std::string& group, const std::string& topic,
      const std::vector<storage::OffsetEntry>& updates,
      std::uint32_t partition_count);

  // 诊断用：已加载的 (group, topic) 数量。
  std::size_t tracked_topics() const { return states_.size(); }
  bool knows(const std::string& group, const std::string& topic) const;

 private:
  struct TopicState {
    std::unique_ptr<storage::OffsetStore> store;
    std::map<std::uint32_t, std::uint64_t> committed;
  };

  // 首次访问某 (group, topic) 时打开位点文件并加载快照；失败返回 nullptr。
  TopicState* Ensure(const std::string& group, const std::string& topic,
                     common::Status* error);

  std::string data_dir_;
  WarningSink warning_sink_;
  // default_strategy_ 必须先于 strategy_ 声明：strategy_ 可能指向它。
  StaticAssignmentStrategy default_strategy_;
  const AssignmentStrategy* strategy_;
  std::map<std::pair<std::string, std::string>, std::unique_ptr<TopicState>>
      states_;
};

}  // namespace group
}  // namespace minikafka

#endif  // MINIKAFKA_GROUP_COORDINATOR_H_
