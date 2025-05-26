#include "sstable_writer.hpp"

#include <algorithm>
#include <fstream>
#include <ios>
#include <iterator>
#include <memory>  // For std::unique_ptr
#include <vector>
#include <iostream> // For temporary debugging output, if needed

SSTableWriter::SSTableWriter(bool enable_compression, int compression_level,
                             size_t target_block_size)
    : zstd_cctx_(nullptr),
      compression_level_(compression_level),
      compression_enabled_(enable_compression),
      target_block_size_(target_block_size > 0 ? target_block_size : 4096) { // Ensure target_block_size is positive
      }

SSTableWriter::~SSTableWriter() {
  if (zstd_cctx_ != nullptr) {
    ZSTD_freeCCtx(zstd_cctx_);
    zstd_cctx_ = nullptr;
  }
}

Result SSTableWriter::Init() {
  // If compression is disabled, ensure any existing context is freed
  if (!compression_enabled_) {
    if (zstd_cctx_ != nullptr) {
        ZSTD_freeCCtx(zstd_cctx_);
        zstd_cctx_ = nullptr;
    }
    return Result::OK();
  }

  // Compression is enabled, try to create/reset context
  if (zstd_cctx_ == nullptr) {
    zstd_cctx_ = ZSTD_createCCtx();
  } else {
    // Reset existing context for potential reuse if Init is called multiple times
    // (though typically Init is called once per SSTableWriter lifetime)
    ZSTD_CCtx_reset(zstd_cctx_, ZSTD_reset_session_and_parameters);
  }

  if (zstd_cctx_ == nullptr) {
    // Critical failure: couldn't get a ZSTD context when compression was requested
    return Result::IOError(
        "SSTableWriter: Failed to create/initialize ZSTD compression context during Init.");
  }
  return Result::OK();
}

Result SSTableWriter::WriteMemTableToFile(const MemTable& memtable,
                                          const std::string& filename) {
  if (compression_enabled_ && zstd_cctx_ == nullptr) {
      return Result::NotSupported("SSTableWriter: Compression enabled but ZSTD context not initialized. Call Init() first.");
  }

  std::ofstream out_file(filename, std::ios::binary | std::ios::trunc);
  if (!out_file.is_open()) {
    return Result::IOError("SSTableWriter: Failed to open SSTable file for writing: " +
                           filename);
  }

  std::unique_ptr<SortedTableIterator> iter(memtable.NewIterator());
  if (!iter) {
    return Result::Corruption(
        "SSTableWriter: Failed to create iterator from memtable.");
  }

  iter->SeekToFirst();
  std::vector<char> current_data_block_buffer;
  std::vector<char> compressed_block_buffer; 
  // uint64_t current_file_offset = 0; // For index later

  while (iter->Valid()) {
    Slice key = iter->key();
    ValueEntry value_entry = iter->value(); // This comes from SkipList, should be correct

    // Serialize K/V to current_data_block_buffer
    AppendLittleEndian32(current_data_block_buffer,
                         static_cast<uint32_t>(key.size()));
    AppendBytesToBuffer(current_data_block_buffer, key.data(), key.size());

    // Correctly write the tag
    current_data_block_buffer.push_back(static_cast<char>(value_entry.type)); 
    
    uint32_t value_size = 0;
    if (value_entry.IsValue()) { // Only write value if it's a data entry
        value_size = static_cast<uint32_t>(value_entry.value_slice.size());
    }
    // For tombstones, value_size will be 0, and IsValue() is false.
    // The ValueEntry constructor for tombstones should ensure value_slice is empty.

    AppendLittleEndian32(current_data_block_buffer, value_size);
    if (value_entry.IsValue() && value_size > 0) {
      AppendBytesToBuffer(current_data_block_buffer,
                          value_entry.value_slice.data(), value_size);
    }

    iter->Next();

    bool should_flush = current_data_block_buffer.size() >= target_block_size_;
    if (!iter->Valid() && !current_data_block_buffer.empty()) {
        should_flush = true; 
    }
    std::cout << "WRITER_DEBUG: Iter Valid? " << iter->Valid()
           << ", Buffer Size: " << current_data_block_buffer.size()
           << ", Target Size: " << target_block_size_
           << ", Should Flush Now? " << should_flush << std::endl;
    if (iter->Valid()) { std::cout << "WRITER_DEBUG: Next Key from iter: " << iter->key().ToString() << std::endl;}


    if (should_flush) {
      uint32_t uncompressed_size =
          static_cast<uint32_t>(current_data_block_buffer.size());
      std::cout << "------------------------------------------------------------\n"
              << "WRITER: FLUSHING BLOCK\n"
              << "WRITER: Target Block Size for this writer instance: " << target_block_size_ << "\n"
              << "WRITER: Current Data Block Buffer Size (uncompressed_size to be written): " << uncompressed_size << std::endl;
      std::cout << "WRITER: Content of buffer being flushed:" << std::endl;
      size_t temp_pos = 0;
      const char* temp_buf_ptr = current_data_block_buffer.data();
      int entry_count_in_writer_buffer = 0;
      while(temp_pos < uncompressed_size) {
          entry_count_in_writer_buffer++;
          uint32_t k_len = ReadLittleEndian32(temp_buf_ptr + temp_pos); temp_pos += 4;
          std::string k_str(temp_buf_ptr + temp_pos, k_len); temp_pos += k_len;
          char tag_char = *(temp_buf_ptr + temp_pos); temp_pos += 1;
          uint32_t v_len = ReadLittleEndian32(temp_buf_ptr + temp_pos); temp_pos += 4;
          temp_pos += v_len;
          std::cout << "  Entry " << entry_count_in_writer_buffer << ": Key='" << k_str 
                    << "', Tag=" << static_cast<int>(static_cast<ValueTag>(tag_char))
                    << ", ValLen=" << v_len << std::endl;
          if (temp_pos > uncompressed_size) { std::cout << "  ERROR: temp_pos overran uncompressed_size in writer debug print!" << std::endl; break;}
      }
      std::cout << "WRITER: Total entries in this flushed buffer: " << entry_count_in_writer_buffer << std::endl;
      
      const char* data_to_write_ptr = current_data_block_buffer.data();
      uint32_t on_disk_size = uncompressed_size;
      char current_compression_flag = CompressionType::kNoCompression;

      if (compression_enabled_ && zstd_cctx_ != nullptr && uncompressed_size > 0) {
        size_t estimated_compressed_bound =
            ZSTD_compressBound(uncompressed_size);
        if (compressed_block_buffer.size() < estimated_compressed_bound) {
             compressed_block_buffer.resize(estimated_compressed_bound);
        }

        size_t actual_compressed_size_zstd = ZSTD_compressCCtx(
            zstd_cctx_, compressed_block_buffer.data(), compressed_block_buffer.size(),
            current_data_block_buffer.data(), uncompressed_size,
            compression_level_);

        if (!ZSTD_isError(actual_compressed_size_zstd) && actual_compressed_size_zstd < uncompressed_size) {
            on_disk_size = static_cast<uint32_t>(actual_compressed_size_zstd);
            data_to_write_ptr = compressed_block_buffer.data();
            current_compression_flag = CompressionType::kZstdCompressed;
        } else if (ZSTD_isError(actual_compressed_size_zstd)) {
          std::cerr << "SSTableWriter: Warning - ZSTD compression failed for a block: "
                    << ZSTD_getErrorName(actual_compressed_size_zstd)
                    << ". Writing uncompressed." << std::endl;
          // Defaults for uncompressed are already set:
          // current_compression_flag = CompressionType::kNoCompression;
          // on_disk_size = uncompressed_size;
          // data_to_write_ptr = current_data_block_buffer.data();
        }
        // Else (Zstd didn't shrink it or error already handled): write uncompressed (defaults)
      }

      std::vector<char> block_header_buffer;
      block_header_buffer.reserve(9); 

      AppendLittleEndian32(block_header_buffer, uncompressed_size);
      AppendLittleEndian32(block_header_buffer, on_disk_size);
      block_header_buffer.push_back(current_compression_flag);
      
      out_file.write(block_header_buffer.data(), block_header_buffer.size());
      if (!out_file) {
        current_data_block_buffer.clear(); 
        return Result::IOError(
            "SSTableWriter: Failed to write block header to file: " + filename);
      }
      // current_file_offset += block_header_buffer.size(); // For index
      
      if (on_disk_size > 0) { // Only write if there's data payload
        out_file.write(data_to_write_ptr, on_disk_size);
        if (!out_file) {
            current_data_block_buffer.clear();
            return Result::IOError(
                "SSTableWriter: Failed to write block data payload to file: " + filename);
        }
        // current_file_offset += on_disk_size; // For index
      }


      current_data_block_buffer.clear();
    }
  } 

  out_file.close();
  if (!out_file.good()) { 
    return Result::IOError("SSTableWriter: Error reported after closing SSTable file: " +
                           filename);
  }
  return Result::OK();
}

void SSTableWriter::AppendLittleEndian32(std::vector<char>& buf,
                                         uint32_t value) {
  buf.push_back(static_cast<char>(value & 0xFF));
  buf.push_back(static_cast<char>((value >> 8) & 0xFF));
  buf.push_back(static_cast<char>((value >> 16) & 0xFF));
  buf.push_back(static_cast<char>((value >> 24) & 0xFF));
}

void SSTableWriter::AppendBytesToBuffer(std::vector<char>& buf,
                                        const std::byte* data, size_t size) {
  if (data != nullptr && size > 0) {
    const char* char_data_ptr = reinterpret_cast<const char*>(data);
    buf.insert(buf.end(), char_data_ptr, char_data_ptr + size);
  }
}