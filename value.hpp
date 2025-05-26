#ifndef VALUE_H
#define VALUE_H

#include "slice.hpp"


enum class ValueTag {
  kData,
  kTombstone,
};

struct ValueEntry {
  Slice value_slice;
  ValueTag type;

  // Constructor for a normal value
  ValueEntry(const Slice& val_slice,  ValueTag entry_type = ValueTag::kData)
      : value_slice(val_slice), type(entry_type) {}

  // Constructor for a tombstone (value_slice will be default/empty)
  explicit ValueEntry(ValueTag entry_type) : type(entry_type) {
      // Ensure value_slice is empty or default for non-value types
      if (type != ValueTag::kTombstone) {
          value_slice = Slice(); // Default empty slice
      }
  }
  
  // Default constructor creates a value type with an empty slice by default
  // Or, make it explicit what kind of empty entry this is.
  // For safety, let's make it explicit. User must choose type.
  // ValueEntry() : type(ValueTag::kTypeValue) {} // Default to value with empty slice

  bool IsTombstone() const { return type == ValueTag::kTombstone; }
  bool IsValue() const { return type == ValueTag::kData; }

  // For std::map comparison if ValueEntry was a key (not the case here)
  // or if std::optional<ValueEntry> needs comparison (it doesn't directly if we compare underlying Slice)
  // bool operator==(const ValueEntry& other) const {
  //     if (type != other.type) return false;
  //     if (type == EntryType::kTypeValue) {
  //         return value_slice == other.value_slice;
  //     }
  //     return true; // Tombstones of same type are equal
  // }
};
#endif // VALUE_H