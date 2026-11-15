#include "minikafka/group/coordinator.h"

#include <utility>

#include "minikafka/storage/paths.h"

namespace minikafka {
namespace group {

using common::Status;
using common::StatusCode;
using common::StatusOr;
using storage::OffsetEntry;
using storage::OffsetLoadResult;
using storage::OffsetSnapshot;
using storage::OffsetStore;

GroupCoordinator::GroupCoordinator(std::string data_dir,
                                   WarningSink warning_sink,
                                   const AssignmentStrategy* strategy)
    : data_dir_(std::move(data_dir)),
      warning_sink_(std::move(warning_sink)),
      strategy_(strategy == nullptr ? &default_strategy_ : strategy) {}

bool GroupCoordinator::knows(const std::string& group,
                             const std::string& topic) const {
  return states_.find(std::make_pair(group, topic)) != states_.end();
}

GroupCoordinator::TopicState* GroupCoordinator::Ensure(
    const std::string& group, const std::string& topic, common::Status* error) {
  const std::pair<std::string, std::string> key(group, topic);
  std::map<std::pair<std::string, std::string>,
           std::unique_ptr<TopicState>>::iterator it = states_.find(key);
  if (it != states_.end()) {
    return it->second.get();
  }

  StatusOr<OffsetStore> store = OffsetStore::Open(data_dir_, group, topic);
  if (!store.ok()) {
    *error = store.status();
    return nullptr;
  }

  std::unique_ptr<TopicState> state(new TopicState());
  state->store.reset(new OffsetStore(std::move(store).value()));

  // R28：双槽都坏也不报错退出，降级为"无已提交位点"并告警。
  StatusOr<OffsetLoadResult> loaded = state->store->Load();
  if (!loaded.ok()) {
    *error = loaded.status();
    return nullptr;
  }
  const OffsetLoadResult& result = loaded.value();
  for (std::size_t i = 0; i < result.snapshot.entries.size(); ++i) {
    state->committed[result.snapshot.entries[i].partition] =
        result.snapshot.entries[i].committed_offset;
  }
  if (result.degraded && warning_sink_) {
    warning_sink_("offset file degraded: group=" + group + " topic=" + topic +
                  (result.warning.empty() ? std::string()
                                          : " " + result.warning));
  }

  TopicState* raw = state.get();
  states_[key] = std::move(state);
  return raw;
}

common::StatusOr<FetchOutcome> GroupCoordinator::Fetch(
    const std::string& group, const std::string& topic,
    std::uint32_t consumer_index, std::uint32_t consumer_count,
    std::uint32_t partition_count) {
  const Status group_status = storage::ValidateGroupName(group);
  if (!group_status.ok()) {
    return StatusOr<FetchOutcome>(group_status);
  }
  const Status topic_status = storage::ValidateTopicName(topic);
  if (!topic_status.ok()) {
    return StatusOr<FetchOutcome>(topic_status);
  }

  std::vector<std::uint32_t> assigned;
  const Status assign_status = strategy_->Assign(
      consumer_index, consumer_count, partition_count, &assigned);
  if (!assign_status.ok()) {
    return StatusOr<FetchOutcome>(assign_status);
  }

  Status ensure_status;
  TopicState* state = Ensure(group, topic, &ensure_status);
  if (state == nullptr) {
    return StatusOr<FetchOutcome>(ensure_status);
  }

  FetchOutcome outcome;
  outcome.assigned_partitions = assigned;
  outcome.entries.reserve(assigned.size());
  for (std::size_t i = 0; i < assigned.size(); ++i) {
    CommittedOffset entry;
    entry.partition = assigned[i];
    const std::map<std::uint32_t, std::uint64_t>::const_iterator found =
        state->committed.find(assigned[i]);
    if (found != state->committed.end()) {
      entry.has_committed = true;
      entry.committed_offset = found->second;
    }
    outcome.entries.push_back(entry);
  }
  return StatusOr<FetchOutcome>(std::move(outcome));
}

common::StatusOr<std::uint32_t> GroupCoordinator::Commit(
    const std::string& group, const std::string& topic,
    const std::vector<OffsetEntry>& updates, std::uint32_t partition_count) {
  const Status group_status = storage::ValidateGroupName(group);
  if (!group_status.ok()) {
    return StatusOr<std::uint32_t>(group_status);
  }
  const Status topic_status = storage::ValidateTopicName(topic);
  if (!topic_status.ok()) {
    return StatusOr<std::uint32_t>(topic_status);
  }
  if (partition_count == 0u) {
    return StatusOr<std::uint32_t>(
        Status(StatusCode::kInvalidPartitions, "topic has no partition"));
  }

  // S-13：先整体校验，任一条越界则整体不应用。
  for (std::size_t i = 0; i < updates.size(); ++i) {
    if (updates[i].partition >= partition_count) {
      return StatusOr<std::uint32_t>(
          Status(StatusCode::kInvalidPartition,
                 "partition out of range: " +
                     std::to_string(updates[i].partition) + " >= " +
                     std::to_string(partition_count)));
    }
  }
  if (updates.empty()) {
    return StatusOr<std::uint32_t>(0u);
  }

  Status ensure_status;
  TopicState* state = Ensure(group, topic, &ensure_status);
  if (state == nullptr) {
    return StatusOr<std::uint32_t>(ensure_status);
  }

  OffsetSnapshot resulting;
  // 落盘路径自己会做"同分区取 max、不回退"（04 §6.3），此处不重复判断。
  const Status commit_status = state->store->Commit(updates, &resulting);
  if (!commit_status.ok()) {
    return StatusOr<std::uint32_t>(commit_status);
  }

  state->committed.clear();
  for (std::size_t i = 0; i < resulting.entries.size(); ++i) {
    state->committed[resulting.entries[i].partition] =
        resulting.entries[i].committed_offset;
  }
  return StatusOr<std::uint32_t>(
      static_cast<std::uint32_t>(updates.size()));
}

}  // namespace group
}  // namespace minikafka
