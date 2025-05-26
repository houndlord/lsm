#ifndef SLICE_H
#define SLICE_H

#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <vector>
#include <algorithm>
#include <cstring>

//lightweight non-owning view into a sequence of bytes.
struct Slice{
 public:
  Slice();
  Slice(const std::byte* data, size_t size);
  Slice(const char*);
  Slice(void*);
  Slice(std::string data);
  Slice(const std::vector<char> data);
  Slice(const std::vector<std::byte>& data);

  Slice(const Slice& other);
  Slice(Slice&& other);
  Slice& operator=(const Slice& other) = default;
  bool operator==(const Slice& other) const {
    if (size_ != other.size_) {
      return false;
    }
    if (empty()) { // Both are empty and have same size (0)
      return true;
    }
    // If both are non-empty with same size, compare content
    return std::memcmp(ptr_, other.ptr_, size_) == 0;
  }

  bool operator!=(const Slice& other) const {
    return !(*this == other);
  }

  std::optional<std::reference_wrapper<const std::byte>> at(size_t index);

  const std::byte* data() const;
  bool empty() const;
  size_t size() const;
  int compare(const Slice& other) const {
      const size_t len1 = size();
      const size_t len2 = other.size();
      const size_t min_len = std::min(len1, len2);
      int r = 0;
      if (min_len > 0 && data() != nullptr && other.data() != nullptr) { // Check for nullptrs if Slice can be empty with non-null data_
         r = std::memcmp(data(), other.data(), min_len);
      } else if (min_len > 0) {
        // Handle case where one data ptr might be null but size isn't 0 (shouldn't happen with proper Slice construction)
        if (data() == nullptr && other.data() != nullptr) return -1; // Empty considered less
        if (data() != nullptr && other.data() == nullptr) return 1;  // Non-empty considered greater
        // If both are nullptr but min_len > 0, this is an inconsistent state for Slice.
        // For safety, if sizes are non-zero but data is null, they should be treated based on size.
      }


      if (r == 0) {
          if (len1 < len2) r = -1;
          else if (len1 > len2) r = +1;
      }
      return r;
  }
  std::string ToString() const {
    if (empty()) {
      return "";
    }
    //reinterpret_cast is necessary because std::byte is not char
    return std::string(reinterpret_cast<const char*>(ptr_), size_);
  }

  void clear();

  
 private:
  const std::byte* ptr_ = nullptr;
  std::size_t size_ = 0;
};
#endif //SLICE_H