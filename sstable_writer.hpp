#ifndef SSTABLE_WRITER_HPP
#define SSTABLE_WRITER_HPP

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "mem_table.hpp" // Assumed to provide MemTable and SortedTableIterator
#include "result.hpp"
#include "slice.hpp"
#include "zstd.h"

inline uint32_t ReadLittleEndian32(const char* buffer) {
    return static_cast<uint32_t>(static_cast<unsigned char>(buffer[0])) |
           (static_cast<uint32_t>(static_cast<unsigned char>(buffer[1])) << 8) |
           (static_cast<uint32_t>(static_cast<unsigned char>(buffer[2])) << 16) |
           (static_cast<uint32_t>(static_cast<unsigned char>(buffer[3])) << 24);
}

namespace CompressionType {
        static constexpr char kNoCompression = 0x00;
        static constexpr char kZstdCompressed = 0x01;
    }

// Forward declaration if SortedTableIterator is not fully defined via mem_table.hpp
// class SortedTableIterator;

struct IndexEntry {
  const Slice& key;
  uint64_t data_offset;
  size_t compressed_size;
  size_t uncompressed_size;
};

struct SSTableWriter {
 public:
  SSTableWriter(bool enable_compression, int compression_level = 1,
                size_t target_block_size = 4096);
  ~SSTableWriter();

  SSTableWriter(const SSTableWriter&) = delete;
  SSTableWriter& operator=(const SSTableWriter&) = delete;

  Result Init();
  Result WriteMemTableToFile(const MemTable& memtable,
                               const std::string& filename);

 private:
  void AppendLittleEndian32(std::vector<char>& buf, uint32_t value);
  void AppendBytesToBuffer(std::vector<char>& buf, const std::byte* data,
                             size_t size);

  ZSTD_CCtx* zstd_cctx_;
  int compression_level_;
  bool compression_enabled_;
  size_t target_block_size_;
};

#endif  // SSTABLE_WRITER_HPP