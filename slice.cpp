#include "slice.hpp"
#include <cstddef>
#include <cstring>


Slice::Slice() {};

Slice::Slice(const std::byte* data, size_t size) : ptr_(data), size_(size) {};

Slice::Slice(const char* data) : ptr_(reinterpret_cast<const std::byte*>(data)), size_(strlen(data)) {};

Slice::Slice(void* data) : ptr_(reinterpret_cast<const std::byte*>(data)), size_(strlen(reinterpret_cast<char*>(data))) {};

Slice::Slice(std::string data) : ptr_(reinterpret_cast<const std::byte*>(data.data())), size_(data.size()) {};

Slice::Slice(const std::vector<char> data) : ptr_(reinterpret_cast<const std::byte*>(data.data())), size_(data.size()) {};

Slice::Slice(const std::vector<std::byte>& data) : ptr_(data.data()), size_(data.size()) {};

Slice::Slice(const Slice& other) : ptr_(other.ptr_), size_(other.size_) {};

Slice::Slice(Slice&& other): Slice(other) {
  other.ptr_ = nullptr;
  other.size_ = 0;
};

std::optional<std::reference_wrapper<const std::byte>> Slice::at(size_t index) {
  if (index < size_) {
    return std::cref(ptr_[index]); // cref for const std::byte&
  } else {
    return std::nullopt;
}
};

const std::byte* Slice::data() const {
  return ptr_;
}

bool Slice::empty() const{
  return (size_ == 0);
}
 
size_t Slice::size() const{
  return size_;
}

void Slice::clear() {
  ptr_ = nullptr;
  size_ = 0;
}