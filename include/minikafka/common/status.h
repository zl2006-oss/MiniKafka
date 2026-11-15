#ifndef MINIKAFKA_COMMON_STATUS_H_
#define MINIKAFKA_COMMON_STATUS_H_

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <string>
#include <type_traits>
#include <utility>

namespace minikafka {
namespace common {

// 05 §5 的完整错误码表；数值码与字符串码一一对应。
// kUsage 在 05 §5 中数值栏为“—”，仅 CLI 本地使用，不进入线上帧。
enum class StatusCode : std::uint16_t {
  kOk = 0x0000,
  kInvalidFrame = 0x0100,
  kFrameCrcError = 0x0101,
  kUnsupportedVersion = 0x0102,
  kUnsupportedMessageType = 0x0103,
  kPayloadDecodeError = 0x0104,
  kRequestTooLarge = 0x0105,
  kInvalidTopicName = 0x0201,
  kInvalidGroupName = 0x0202,
  kTopicNotFound = 0x0203,
  kTopicExists = 0x0204,
  kInvalidPartitions = 0x0205,
  kInvalidPartition = 0x0206,
  kInvalidConsumerSlot = 0x0207,
  kMessageTooLarge = 0x0301,
  kInvalidValueType = 0x0302,
  kInvalidJson = 0x0303,
  kInvalidUtf8 = 0x0304,
  kInvalidBatch = 0x0305,
  kPartitionBlocked = 0x0306,
  kOffsetOutOfRange = 0x0401,
  kInvalidOffset = 0x0402,
  kInvalidCount = 0x0403,
  kInvalidFrom = 0x0404,
  kInvalidInterval = 0x0405,
  kBrokerUnavailable = 0x0601,
  kInternalError = 0x0602,
  kDiskFull = 0x0701,
  kStorageUnavailable = 0x0702,
  kStorageIoError = 0x0703,
  kDeliveryFailed = 0x0901,
  kUsage = 0xFFFF,
};

const char* StatusCodeToString(StatusCode code);
bool StatusCodeFromString(const std::string& name, StatusCode* code);

// 业务错误一律通过 Status/StatusOr 返回，不抛异常（AD-11）。
class Status {
 public:
  Status() = default;
  Status(StatusCode code, std::string message = std::string());

  bool ok() const { return code_ == StatusCode::kOk; }
  explicit operator bool() const { return ok(); }

  StatusCode code() const { return code_; }
  const std::string& message() const { return message_; }
  std::string ToString() const;

 private:
  StatusCode code_ = StatusCode::kOk;
  std::string message_;
};

// StatusOr<T> 不要求 T 可默认构造；错误路径不构造 T。
// value() 在非 ok 状态下属于编程错误，直接 abort；需要安全检查时用 value_if()。
template <typename T>
class StatusOr {
  static_assert(!std::is_reference<T>::value,
                "StatusOr<T> does not support reference types");
  static_assert(!std::is_void<T>::value,
                "StatusOr<T> does not support void");

 public:
  StatusOr(T value) : has_value_(true), status_() {
    new (&storage_) T(std::move(value));
  }

  StatusOr(Status status) : has_value_(false), status_(std::move(status)) {
    if (status_.ok()) {
      status_ = Status(StatusCode::kInternalError,
                       "StatusOr requires a non-ok Status");
    }
  }

  StatusOr(const StatusOr& other)
      : has_value_(other.has_value_), status_(other.status_) {
    if (has_value_) {
      new (&storage_) T(*other.value_ptr());
    }
  }

  StatusOr(StatusOr&& other)
      : has_value_(other.has_value_), status_(std::move(other.status_)) {
    if (has_value_) {
      new (&storage_) T(std::move(*other.value_ptr()));
      other.value_ptr()->~T();
      other.has_value_ = false;
    }
  }

  ~StatusOr() { Reset(); }

  StatusOr& operator=(const StatusOr& other) {
    if (this != &other) {
      Reset();
      status_ = other.status_;
      if (other.has_value_) {
        new (&storage_) T(*other.value_ptr());
        has_value_ = true;
      }
    }
    return *this;
  }

  StatusOr& operator=(StatusOr&& other) {
    if (this != &other) {
      Reset();
      status_ = std::move(other.status_);
      if (other.has_value_) {
        new (&storage_) T(std::move(*other.value_ptr()));
        has_value_ = true;
        other.value_ptr()->~T();
        other.has_value_ = false;
      }
    }
    return *this;
  }

  bool ok() const { return has_value_; }
  explicit operator bool() const { return ok(); }
  const Status& status() const { return status_; }

  T& value() & {
    CheckValue();
    return *value_ptr();
  }
  const T& value() const& {
    CheckValue();
    return *value_ptr();
  }
  T&& value() && {
    CheckValue();
    return std::move(*value_ptr());
  }

  T* operator->() {
    CheckValue();
    return value_ptr();
  }
  const T* operator->() const {
    CheckValue();
    return value_ptr();
  }
  T& operator*() {
    CheckValue();
    return *value_ptr();
  }
  const T& operator*() const {
    CheckValue();
    return *value_ptr();
  }

  T* value_if() { return has_value_ ? value_ptr() : nullptr; }
  const T* value_if() const { return has_value_ ? value_ptr() : nullptr; }

 private:
  void Reset() {
    if (has_value_) {
      value_ptr()->~T();
      has_value_ = false;
    }
  }

  void CheckValue() const {
    if (!has_value_) {
      std::abort();
    }
  }

  T* value_ptr() { return reinterpret_cast<T*>(&storage_); }
  const T* value_ptr() const {
    return reinterpret_cast<const T*>(&storage_);
  }

  typename std::aligned_storage<sizeof(T), alignof(T)>::type storage_;
  bool has_value_;
  Status status_;
};

}  // namespace common
}  // namespace minikafka

#endif  // MINIKAFKA_COMMON_STATUS_H_
