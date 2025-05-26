#include "arena.hpp"
#include <cstddef>
#include <new>

void* AdvanceBlockPtr(std::byte* current_alloc_ptr, std::byte* current_block_end_ptr, size_t alignment, size_t num_bytes) {
  void* ptr_to_align = static_cast<void*>(current_alloc_ptr);
  size_t available_space = static_cast<size_t>(current_block_end_ptr - current_alloc_ptr);
  void* aligned_ptr = std::align(alignment, num_bytes, ptr_to_align, available_space);
  return aligned_ptr;
}

void* Arena::Allocate(size_t num_bytes, size_t alignment) {
  if (num_bytes == 0) {return nullptr;}
  void* aligned_ptr = AdvanceBlockPtr(current_alloc_ptr_, current_block_end_ptr_, alignment, num_bytes);
  if (aligned_ptr == nullptr) { 
    AllocateNewBlock(num_bytes + (alignment - 1));
  }
  if (!current_alloc_ptr_) {
        return nullptr; // OOM
    }
  aligned_ptr = AdvanceBlockPtr(current_alloc_ptr_, current_block_end_ptr_, alignment, num_bytes);
  current_alloc_ptr_ = static_cast<std::byte*>(aligned_ptr) + num_bytes;
  total_bytes_used_ += num_bytes;
  return aligned_ptr;
};

void Arena::AllocateNewBlock(size_t size_bytes) {
  current_alloc_ptr_ = (static_cast<std::byte*>(:: operator new(size_bytes, std::nothrow)));
  block_list_.emplace_back(current_alloc_ptr_);
  current_block_idx_ = block_list_.size() - 1;
  current_block_end_ptr_ = current_alloc_ptr_ + size_bytes;
};
