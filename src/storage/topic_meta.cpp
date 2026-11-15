#include "minikafka/storage/topic_meta.h"

#include <dirent.h>
#include <sys/stat.h>

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "byte_codec.h"
#include "file_util.h"
#include "minikafka/common/crc32.h"
#include "minikafka/common/defaults.h"
#include "minikafka/storage/paths.h"

namespace minikafka {
namespace storage {

using common::Status;
using common::StatusCode;
using common::StatusOr;

namespace {

const std::size_t kMaxTopicNameLength = 64u;

bool HasMetaSuffix(const std::string& file_name) {
  const std::string suffix = ".meta";
  return file_name.size() > suffix.size() &&
         file_name.compare(file_name.size() - suffix.size(), suffix.size(),
                           suffix) == 0;
}

std::string TopicFromMetaFile(const std::string& file_name) {
  return file_name.substr(0u, file_name.size() - 5u);
}

void AppendWarning(const std::string& topic, const std::string& path,
                   const std::string& reason,
                   std::vector<std::string>* warnings) {
  warnings->push_back("skip topic '" + topic + "' metadata '" + path +
                      "': " + reason);
}

}  // namespace

Status EncodeTopicMetadata(const TopicMetadata& metadata,
                           std::string* bytes) {
  if (bytes == nullptr) {
    return Status(StatusCode::kInternalError, "null topic metadata output");
  }
  if (!IsValidTopicName(metadata.name)) {
    return Status(StatusCode::kInvalidTopicName, "invalid topic name");
  }
  if (metadata.partition_count <
          static_cast<std::uint32_t>(common::kMinPartitions) ||
      metadata.partition_count >
          static_cast<std::uint32_t>(common::kMaxPartitions)) {
    return Status(StatusCode::kInvalidPartitions,
                  "partition_count must be in 1..16");
  }

  std::string out;
  out.reserve(kTopicMetaHeaderSize + metadata.name.size() +
              kTopicMetaFooterSize);
  internal::AppendU32(&out, kTopicMetaMagic);
  internal::AppendU16(&out, kTopicMetaVersion);
  internal::AppendU16(&out, 0u);
  internal::AppendU32(&out, metadata.partition_count);
  internal::AppendU32(&out,
                      static_cast<std::uint32_t>(metadata.name.size()));
  out.append(metadata.name);
  internal::AppendU32(&out, common::Crc32(out));

  *bytes = std::move(out);
  return Status();
}

Status DecodeTopicMetadata(const void* data, std::size_t size,
                           TopicMetadata* metadata) {
  if (metadata == nullptr) {
    return Status(StatusCode::kInternalError, "null topic metadata output");
  }
  if (size < kTopicMetaHeaderSize + kTopicMetaFooterSize) {
    return Status(StatusCode::kPayloadDecodeError,
                  "topic metadata file is too small");
  }
  if (data == nullptr) {
    return Status(StatusCode::kInternalError, "null topic metadata input");
  }

  const unsigned char* bytes = static_cast<const unsigned char*>(data);
  const std::uint32_t magic = internal::ReadU32(bytes);
  if (magic != kTopicMetaMagic) {
    return Status(StatusCode::kInvalidFrame, "invalid topic metadata magic");
  }
  const std::uint16_t version = internal::ReadU16(bytes + 4u);
  if (version != kTopicMetaVersion) {
    return Status(StatusCode::kUnsupportedVersion,
                  "unsupported topic metadata version");
  }
  const std::uint16_t flags = internal::ReadU16(bytes + 6u);
  if (flags != 0u) {
    return Status(StatusCode::kPayloadDecodeError,
                  "topic metadata flags must be zero");
  }
  const std::uint32_t partition_count = internal::ReadU32(bytes + 8u);
  if (partition_count <
          static_cast<std::uint32_t>(common::kMinPartitions) ||
      partition_count >
          static_cast<std::uint32_t>(common::kMaxPartitions)) {
    return Status(StatusCode::kInvalidPartitions,
                  "partition_count must be in 1..16");
  }
  const std::uint32_t name_len = internal::ReadU32(bytes + 12u);
  if (name_len == 0u || name_len > kMaxTopicNameLength) {
    return Status(StatusCode::kInvalidTopicName, "invalid topic name length");
  }

  const std::uint64_t expected_size =
      static_cast<std::uint64_t>(kTopicMetaHeaderSize) +
      static_cast<std::uint64_t>(name_len) +
      static_cast<std::uint64_t>(kTopicMetaFooterSize);
  if (expected_size != static_cast<std::uint64_t>(size)) {
    return Status(StatusCode::kPayloadDecodeError,
                  "topic metadata length does not match name_len");
  }

  const std::uint32_t stored_crc =
      internal::ReadU32(bytes + size - kTopicMetaFooterSize);
  if (common::Crc32(data, size - kTopicMetaFooterSize) != stored_crc) {
    return Status(StatusCode::kFrameCrcError,
                  "topic metadata CRC mismatch");
  }

  TopicMetadata decoded;
  decoded.name.assign(reinterpret_cast<const char*>(bytes + kTopicMetaHeaderSize),
                      name_len);
  if (!IsValidTopicName(decoded.name)) {
    return Status(StatusCode::kInvalidTopicName, "invalid topic name");
  }
  decoded.partition_count = partition_count;

  *metadata = std::move(decoded);
  return Status();
}

TopicMetadataStore::TopicMetadataStore(std::string data_dir,
                                       std::string meta_directory)
    : data_dir_(std::move(data_dir)),
      meta_directory_(std::move(meta_directory)) {}

common::StatusOr<TopicMetadataStore> TopicMetadataStore::Open(
    const std::string& data_dir) {
  if (data_dir.empty()) {
    return StatusOr<TopicMetadataStore>(
        Status(StatusCode::kInternalError, "empty data directory"));
  }
  return StatusOr<TopicMetadataStore>(
      TopicMetadataStore(data_dir, internal::JoinPath(data_dir, "meta")));
}

common::Status TopicMetadataStore::Create(const TopicMetadata& metadata) {
  std::string bytes;
  Status status = EncodeTopicMetadata(metadata, &bytes);
  if (!status.ok()) {
    return status;
  }

  std::string path;
  status = MetaPath(data_dir_, metadata.name, &path);
  if (!status.ok()) {
    return status;
  }

  struct stat st;
  if (::stat(path.c_str(), &st) == 0) {
    return Status(StatusCode::kTopicExists,
                  "topic metadata already exists: " + metadata.name);
  }
  if (errno != ENOENT) {
    return Status(StatusCode::kStorageIoError,
                  "cannot inspect topic metadata path: " + path);
  }

  status = internal::EnsureDirectory(meta_directory_);
  if (!status.ok()) {
    return status;
  }
  return internal::WriteFileAtomically(path, path + ".tmp", bytes);
}

common::StatusOr<TopicMetadata> TopicMetadataStore::Load(
    const std::string& topic) const {
  std::string path;
  Status status = MetaPath(data_dir_, topic, &path);
  if (!status.ok()) {
    return StatusOr<TopicMetadata>(status);
  }

  std::string bytes;
  bool exists = false;
  status = internal::ReadWholeFile(path, &bytes, &exists);
  if (!status.ok()) {
    return StatusOr<TopicMetadata>(status);
  }
  if (!exists) {
    return StatusOr<TopicMetadata>(
        Status(StatusCode::kTopicNotFound, "topic metadata not found"));
  }

  TopicMetadata metadata;
  status = DecodeTopicMetadata(bytes.data(), bytes.size(), &metadata);
  if (!status.ok()) {
    return StatusOr<TopicMetadata>(status);
  }
  if (metadata.name != topic) {
    return StatusOr<TopicMetadata>(
        Status(StatusCode::kStorageUnavailable,
               "topic metadata name does not match file name"));
  }
  return StatusOr<TopicMetadata>(std::move(metadata));
}

common::Status TopicMetadataStore::Scan(TopicScanResult* result) const {
  if (result == nullptr) {
    return Status(StatusCode::kInternalError, "null topic scan output");
  }
  result->topics.clear();
  result->warnings.clear();

  DIR* dir = ::opendir(meta_directory_.c_str());
  if (dir == nullptr) {
    if (errno == ENOENT) {
      return Status();
    }
    return Status(StatusCode::kStorageIoError,
                  "cannot open meta directory: " + meta_directory_);
  }

  while (true) {
    const struct dirent* entry = ::readdir(dir);
    if (entry == nullptr) {
      break;
    }
    const std::string file_name(entry->d_name);
    if (file_name == "." || file_name == ".." || !HasMetaSuffix(file_name)) {
      continue;
    }
    const std::string topic = TopicFromMetaFile(file_name);
    const std::string path = internal::JoinPath(meta_directory_, file_name);
    if (!IsValidTopicName(topic)) {
      AppendWarning(topic, path, "invalid topic file name", &result->warnings);
      continue;
    }

    std::string bytes;
    bool exists = false;
    const Status read_status =
        internal::ReadWholeFile(path, &bytes, &exists);
    if (!read_status.ok()) {
      AppendWarning(topic, path, read_status.ToString(), &result->warnings);
      continue;
    }
    if (!exists) {
      AppendWarning(topic, path, "metadata disappeared during scan",
                    &result->warnings);
      continue;
    }

    TopicMetadata metadata;
    const Status decode_status =
        DecodeTopicMetadata(bytes.data(), bytes.size(), &metadata);
    if (!decode_status.ok()) {
      AppendWarning(topic, path, decode_status.ToString(), &result->warnings);
      continue;
    }
    if (metadata.name != topic) {
      AppendWarning(topic, path,
                    "metadata name does not match file name",
                    &result->warnings);
      continue;
    }
    result->topics.push_back(metadata);
  }
  ::closedir(dir);

  std::sort(result->topics.begin(), result->topics.end(),
            [](const TopicMetadata& a, const TopicMetadata& b) {
              return a.name < b.name;
            });
  std::sort(result->warnings.begin(), result->warnings.end());
  return Status();
}

}  // namespace storage
}  // namespace minikafka
