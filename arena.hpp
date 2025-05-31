#ifndef ARENA_HPP
#define ARENA_HPP

#include <cstddef>
#include <vector>
#include <memory>

constexpr size_t DEFAULT_ARENA_ALIGNMENT = alignof(std::max_align_t);

void* AdvanceBlockPtr(std::byte* current_alloc_ptr, std::byte* current_block_end_ptr, size_t alignment, size_t num_bytes);

struct Arena {
 public:
  explicit Arena(size_t size = 8192)
    : current_alloc_ptr_(nullptr),
      current_block_end_ptr_(nullptr),
      total_bytes_used_(0)
  {
    AllocateNewBlock(size);
  };
  Arena(const Arena&) = delete;
  Arena(Arena&&) = delete;
  Arena& operator=(const Arena&) = delete;
  Arena& operator=(Arena&&) = delete;
  size_t GetTotalBytesUsed() const { return total_bytes_used_; }
  size_t GetNumBlocksAllocated() const { return block_list_.size(); }

  void* Allocate(size_t num_bytes, size_t alignment = DEFAULT_ARENA_ALIGNMENT);

  template<typename T, typename... Args>
  T* Create(Args&&... constructor_args) {
    void* mem = Allocate(sizeof(T), alignof(T));
    T* obj = new(mem) T(std::forward<Args>(constructor_args)...);
    return obj;
  };

  ~Arena() {
    for (std::byte* block_start_ptr : block_list_) {
        // We used ::operator new (raw allocation), so we use ::operator delete
        // If we used `new std::byte[]`, we'd use `delete[]`.
        // ::operator new returns void*, so we need to cast block_start_ptr.
        ::operator delete(static_cast<void*>(block_start_ptr));
    }
    block_list_.clear();
  }

  #ifdef LSM_PROJECT_ENABLE_TESTING_HOOKS
  // This declaration is only visible when tests are being built
  bool IsAddressInCurrentBlock(const void* ptr) const;
  #endif


 private:
  void AllocateNewBlock(size_t size_bytes);

  std::vector<std::byte*> block_list_;
  size_t current_block_idx_;
  std::byte* storage_;
  std::byte* current_alloc_ptr_;
  std::byte* current_block_end_ptr_;
  size_t total_bytes_used_ = 0;
};
#endif // ARENA_HPP