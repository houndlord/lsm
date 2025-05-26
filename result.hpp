#ifndef RESULT_HPP
#define RESULT_HPP

#include <string>
#include <utility>
#include <optional>

#include "slice.hpp"


enum struct ResultCode : int {
  kOk = 0,
  kArenaAllocationFail = 1,
  kNotFound = 2,
  kCorruption = 3,
  kNotSupported = 4,
  kInvalidArgument = 5,
  kIOError = 6,
  kError = 7,
};

class Result {
 public:
  // Constructors
  Result() : code_(ResultCode::kOk), value_slice_(std::nullopt) {}

  explicit Result(const Slice& success_slice)
      : code_(ResultCode::kOk), value_slice_(success_slice) {}

  Result(ResultCode error_code, std::string error_message)
      : code_(error_code),
        message_(std::move(error_message)),
        value_slice_(std::nullopt) {
    if (error_code == ResultCode::kOk && !message_.empty()) {
      // Potentially assert or handle: OK status should not typically have an error message.
    } else if (error_code != ResultCode::kOk && value_slice_.has_value()) {
        // Should not happen with this constructor set
        value_slice_ = std::nullopt;
    }
  }

  // Copy constructor
  Result(const Result& other)
      : code_(other.code_),
        message_(other.message_),
        value_slice_(other.value_slice_) {}

  // Move constructor
  Result(Result&& other) noexcept
      : code_(other.code_),
        message_(std::move(other.message_)),
        value_slice_(std::move(other.value_slice_)) {
    other.code_ = ResultCode::kOk;
  }

  // Copy assignment operator
  Result& operator=(const Result& other) {
    if (this == &other) {
      return *this;
    }
    code_ = other.code_;
    message_ = other.message_;
    value_slice_ = other.value_slice_;
    return *this;
  }

  // Move assignment operator
  Result& operator=(Result&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    code_ = other.code_;
    message_ = std::move(other.message_);
    value_slice_ = std::move(other.value_slice_);
    other.code_ = ResultCode::kOk;
    return *this;
  }

  ~Result() = default;

  // Static factory methods
  static Result OK() { return Result(); }
  static Result OK(const Slice& success_slice) {
    return Result(success_slice);
  }

  static Result ArenaAllocationFail(std::string message = "") {
    return Result(ResultCode::kArenaAllocationFail, std::move(message));
  }
  static Result NotFound(std::string message = "") {
    return Result(ResultCode::kNotFound, std::move(message));
  }
  static Result Corruption(std::string message) {
    return Result(ResultCode::kCorruption, std::move(message));
  }
  static Result NotSupported(std::string message) {
    return Result(ResultCode::kNotSupported, std::move(message));
  }
  static Result InvalidArgument(std::string message) {
    return Result(ResultCode::kInvalidArgument, std::move(message));
  }
  static Result IOError(std::string message) {
    return Result(ResultCode::kIOError, std::move(message));
  }

  // Accessors
  bool ok() const { return code_ == ResultCode::kOk; }
  ResultCode code() const { return code_; }
  const std::string& message() const { return message_; }
  const std::optional<Slice>& value_slice() const { return value_slice_; }

  std::string ToString() const;

  bool operator==(const Result& other) const {
    return code_ == other.code_ && message_ == other.message_ &&
           value_slice_ == other.value_slice_;
  }
  bool operator!=(const Result& other) const { return !(*this == other); }

 private:
  ResultCode code_;
  std::string message_;
  std::optional<Slice> value_slice_;
};

#endif  // RESULT_HPP