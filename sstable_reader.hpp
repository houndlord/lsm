#ifndef SSTABLE_READER_HPP
#define SSTABLE_READER_HPP

#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

#include "arena.hpp" 
#include "result.hpp"
#include "slice.hpp"
#include "value.hpp"
#include "zstd.h"

struct SSTableReader {
 public:
  explicit SSTableReader(std::string filename);
  ~SSTableReader();

  SSTableReader(const SSTableReader&) = delete;
  SSTableReader& operator=(const SSTableReader&) = delete;
  SSTableReader(SSTableReader&&) = delete;
  SSTableReader& operator=(SSTableReader&&) = delete;

  Result Init();
  bool IsOpen() const { return is_open_; }
  uint64_t FileSize() const { return file_size_; }

  // Get method that copies value to an Arena if found
  Result Get(const Slice& search_key, Arena* arena_for_value_copy);

  // Get method that copies value to a std::string if found
  Result Get(const Slice& search_key, std::string* value_out);

  // Helper to load a data block from disk and decompress it into internal_block_buffer_
  Result LoadBlockIntoBuffer(uint64_t block_offset,
                             uint64_t* block_size_on_disk_out);

  const std::vector<char>& GetBlockBuffer() {return internal_block_buffer_;};

#ifdef ENABLE_SSTABLE_READER_TEST_HOOKS
  const std::vector<char>& TEST_ONLY_get_internal_buffer_DEBUG() const {
    return internal_block_buffer_;
  }
#else
  const std::vector<char>& TEST_ONLY_get_internal_buffer_DEBUG() const {
    static const std::vector<char> empty_stub_for_non_test_builds;
    return empty_stub_for_non_test_builds;
  }
#endif

 private:

  // Private nested struct to hold information about a parsed entry
  struct ParsedEntryInfo {
    Result status;
    Slice key;
    Slice value_in_block;
    ValueTag tag;
    size_t entry_size_in_block;

    // Explicit default constructor
    ParsedEntryInfo()
        : status(Result::OK()),
          key(),
          value_in_block(),
          tag(ValueTag::kData),
          entry_size_in_block(0) {}
  };

  // Helper to parse the next key-value entry from the current internal_block_buffer_
  ParsedEntryInfo ParseNextEntry(const char* block_data_start,
                                 size_t block_size,
                                 size_t current_offset_in_block);

  std::string filename_;
  std::ifstream file_stream_;
  ZSTD_DCtx* zstd_dctx_;
  bool is_open_;
  uint64_t file_size_;
  std::vector<char> internal_block_buffer_; // Stores the decompressed block data
};

#endif // SSTABLE_READER_HPP