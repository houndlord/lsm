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
  void* aligned_ptr = nullptr;
  if (current_alloc_ptr_ && current_block_end_ptr_) { // Ensure current block is valid
      aligned_ptr = AdvanceBlockPtr(current_alloc_ptr_, current_block_end_ptr_, alignment, num_bytes);
  }

  if (aligned_ptr == nullptr) { // If no current block or not enough space
      // Calculate minimum new block size. Add alignment padding in case requested size_bytes is small.
      size_t min_new_block_size = num_bytes + alignment -1; // Ensure enough for this allocation + alignment worst case
      // You might want a default minimum block size too, e.g., std::max(min_new_block_size, default_block_size_from_constructor)
      AllocateNewBlock(min_new_block_size); // Allocate a new block

      if (!current_alloc_ptr_) { // If AllocateNewBlock failed (OOM)
          return nullptr;
      }
      // Retry allocation from the newly created block
      aligned_ptr = AdvanceBlockPtr(current_alloc_ptr_, current_block_end_ptr_, alignment, num_bytes);
  }

  if (aligned_ptr == nullptr) {
      // This should ideally not happen if AllocateNewBlock succeeded and was sized correctly.
      // Could happen if num_bytes is excessively large and even a new block couldn't satisfy std::align.
      return nullptr;
  }

  current_alloc_ptr_ = static_cast<std::byte*>(aligned_ptr) + num_bytes;
  total_bytes_used_ += num_bytes;
  return aligned_ptr;
}

void Arena::AllocateNewBlock(size_t size_bytes) {
  current_alloc_ptr_ = (static_cast<std::byte*>(:: operator new(size_bytes, std::nothrow)));
  block_list_.emplace_back(current_alloc_ptr_);
  current_block_idx_ = block_list_.size() - 1;
  current_block_end_ptr_ = current_alloc_ptr_ + size_bytes;
}

#ifdef LSM_PROJECT_ENABLE_TESTING_HOOKS
// This definition is only compiled when tests are being built
bool Arena::IsAddressInCurrentBlock(const void* ptr) const {
  if (!ptr) {
    return false;
  }
  if (block_list_.empty() || !current_alloc_ptr_ || !current_block_end_ptr_) {
    return false;
  }
  const std::byte* p_byte = static_cast<const std::byte*>(ptr);
  std::byte* current_block_start = block_list_.back();
  return (p_byte >= current_block_start && p_byte < current_block_end_ptr_);
}
#endif // LSM_PROJECT_ENABLE_TESTING_HOOKS