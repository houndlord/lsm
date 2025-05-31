#ifndef RESULT_HPP
#define RESULT_HPP

#include <string>
#include <utility>
#include <optional>
#include "slice.hpp"
#include "value.hpp"
#include <iostream>


enum struct ResultCode : int {
  kOk = 0,                   // Operation successful, value_slice may contain data
  kArenaAllocationFail = 1,
  kNotFound = 2,             // Generic not found, or key not found globally in DB
  kCorruption = 3,
  kNotSupported = 4,
  kInvalidArgument = 5,
  kIOError = 6,
  kError = 7,                // Generic error

  // New codes for SSTableReader::Get specifics
  kFoundTombstone = 8,       // Key was found, but it's a tombstone
  kSSTableMiss = 9           // Key was not found in the current SSTable (search can continue)
};

class Result {
 public:
  // Constructors
   Result() : code_(ResultCode::kOk), value_tag_(ValueTag::kData), value_slice_(std::nullopt), message_("") {}

  // OK with a data value (implies ValueTag::kData)
  explicit Result(const Slice& success_slice)
      : code_(ResultCode::kOk), value_slice_(success_slice), value_tag_(ValueTag::kData), message_("") {
        std::cout << "[Result(Slice) Ctor] Called. Slice size: " << success_slice.size() 
              << ". Set value_tag_ to: " << static_cast<int>(value_tag_.value()) << std::endl;
      }

  // Error constructor
  Result(ResultCode error_code, std::string error_message)
      : code_(error_code),
        message_(std::move(error_message)),
        value_slice_(std::nullopt), // Errors don't have a value slice
        value_tag_(ValueTag::kData) { }

  static Result OkTombstone() {
    Result res; // code_ is kOk by default
    res.value_tag_ = ValueTag::kTombstone;
    // value_slice_ remains std::nullopt
    return res;
  }

  // Copy constructor
  Result(const Result& other) = default;
  // Move constructor
  Result(Result&& other) noexcept = default;
  // Copy assignment operator
  Result& operator=(const Result& other) = default;
  // Move assignment operator
  Result& operator=(Result&& other) noexcept = default;

  ~Result() = default;

  // Static factory methods
  static Result OK() { return Result(); }
  static Result OK(const Slice& success_slice) {
    return Result(success_slice);
  }
  // Keep existing error factories
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
  static Result Error(std::string message) { // Generic error factory
    return Result(ResultCode::kError, std::move(message));
  }

  // New static factory methods for SSTable specific outcomes
  static Result FoundTombstone(std::string message = "") {
      return Result(ResultCode::kFoundTombstone, std::move(message));
  }
  static Result SSTableMiss(std::string message = "") { // Key not in this specific SSTable
      return Result(ResultCode::kSSTableMiss, std::move(message));
  }


  // Accessors
  bool ok() const { return code_ == ResultCode::kOk; } // Only kOk is considered "ok" for value presence
  ResultCode code() const { return code_; }
  const std::string& message() const { return message_; }
  const std::optional<Slice>& value_slice() const { return value_slice_; }
  std::optional<ValueTag> value_tag() const { return value_tag_; }

  std::string ToString() const; // Implementation assumed to exist elsewhere

  bool operator==(const Result& other) const {
    return code_ == other.code_ && message_ == other.message_ &&
           value_slice_ == other.value_slice_;
  }
  bool operator!=(const Result& other) const { return !(*this == other); }

 private:
  ResultCode code_;
  std::string message_;
  std::optional<Slice> value_slice_; // Only valid if code_ == kOk
  std::optional<ValueTag> value_tag_ = std::nullopt;
};

#endif  // RESULT_HPP