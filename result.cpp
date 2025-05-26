#include "result.hpp" // Renamed include
#include <string>     // For std::to_string


std::string Result::ToString() const {
  if (ok()) {
    if (value_slice_.has_value()) {
      // If Slice has a ToString() method for a human-readable representation:
      return "OK (value: " + value_slice_->ToString() + ")";
      // If Slice does not have ToString(), or you want something simpler:
      // return "OK (with value)";
      // Or even more detailed (if Slice has size()):
      // return "OK (value slice, size: " + std::to_string(value_slice_->size()) + ")";
    } else {
      return "OK";
    }
  }

  // Handle error codes
  std::string type_str;
  switch (code_) {
    case ResultCode::kArenaAllocationFail:
      type_str = "ArenaAllocationFail";
      break;
    case ResultCode::kNotFound:
      type_str = "NotFound";
      break;
    case ResultCode::kCorruption:
      type_str = "Corruption";
      break;
    case ResultCode::kNotSupported:
      type_str = "NotSupported";
      break;
    case ResultCode::kInvalidArgument:
      type_str = "InvalidArgument";
      break;
    case ResultCode::kIOError:
      type_str = "IOError";
      break;
    default:
      // This case should ideally not be reached if all codes are handled,
      // but good for robustness.
      type_str = "UnknownErrorCode(" + std::to_string(static_cast<int>(code_)) + ")";
      break;
  }

  if (message_.empty()) {
    return type_str;
  } else {
    return type_str + ": " + message_;
  }
}
