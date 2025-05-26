#ifndef SSTABLE_READER_HPP
#define SSTABLE_READER_HPP

#include "result.hpp"
#include "zstd.h"
#include "arena.hpp"

#include <cstdint>
#include <fstream>


struct SSTableReader {
 public:
  explicit SSTableReader(std::string filename);
  ~SSTableReader();

  Result Init();
  bool isOpen() {return is_open_;};
  Result Get(const Slice& key, Arena* arena);
  size_t FileSize() {return file_size_;};

  Result ReadBlock(uint64_t block_offset, std::vector<char>& block_content_out);
  Result LoadBlockIntoBuffer(uint64_t block_offset, uint64_t* block_size_on_disk_out);

  const std::vector<char>& internal_block_buffer() {return internal_block_buffer_;};

#ifdef ENABLE_SSTABLE_READER_TEST_HOOKS
  const std::vector<char>& TEST_ONLY_get_internal_buffer_DEBUG() const {
    return internal_block_buffer_;
  }
#else
  // Provide a stub if not testing to avoid compilation errors in tests,
  // though ideally, test-only methods shouldn't be in production headers without ifdefs for code too.
  // This simple stub allows tests to compile if the macro isn't set.
  const std::vector<char>& TEST_ONLY_get_internal_buffer_DEBUG() const {
    // This function should ideally not be called if ENABLE_SSTABLE_READER_TEST_HOOKS is not defined.
    // If it is, it indicates a potential issue in build configuration or test logic.
    // Throwing an error or asserting can help catch such issues.
    // For a simple stub that allows compilation:
    static const std::vector<char> empty_stub_for_non_test_builds;
    // Consider: assert(false && "TEST_ONLY_get_internal_buffer_DEBUG called in non-test build!");
    // Or: throw std::logic_error("TEST_ONLY_get_internal_buffer_DEBUG called in non-test build!");
    return empty_stub_for_non_test_builds;
  }
#endif 

 private:
  std::string filename_;
  std::ifstream file_stream_;
  ZSTD_DCtx* zstd_dctx_ = nullptr;
  bool is_open_ = false;
  uint64_t file_size_ = 0;
  std::vector<char> internal_block_buffer_;
};

#endif // SSTABLE_READER_HPP