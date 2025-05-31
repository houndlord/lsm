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
  std::cout << "[SSTableWriter::WriteMemTableToFile] ENTER. Filename: " << filename 
            << ", TargetBlockSize: " << target_block_size_ << std::endl;

  if (compression_enabled_ && zstd_cctx_ == nullptr) {
      std::cerr << "[SSTableWriter::WriteMemTableToFile] ERROR: Compression enabled but ZSTD context not initialized." << std::endl;
      return Result::NotSupported("SSTableWriter: Compression enabled but ZSTD context not initialized. Call Init() first.");
  }

  if (memtable.ApproximateMemoryUsage() == 0) {
      std::cout << "[SSTableWriter::WriteMemTableToFile] Memtable is empty. Writing an empty SSTable file." << std::endl;
      std::ofstream empty_out_file(filename, std::ios::binary | std::ios::trunc);
      if (!empty_out_file.is_open()) {
          std::cerr << "[SSTableWriter::WriteMemTableToFile] ERROR: Failed to open file for writing empty SSTable: " << filename << std::endl;
          return Result::IOError("SSTableWriter: Failed to open file for writing empty SSTable: " + filename);
      }
      empty_out_file.close(); // Creates an empty file
      std::cout << "[SSTableWriter::WriteMemTableToFile] EXIT - Wrote empty file." << std::endl;
      return Result::OK();
  }

  std::ofstream out_file(filename, std::ios::binary | std::ios::trunc);
  if (!out_file.is_open()) {
    std::cerr << "[SSTableWriter::WriteMemTableToFile] ERROR: Failed to open SSTable file for writing: " << filename << std::endl;
    return Result::IOError("SSTableWriter: Failed to open SSTable file for writing: " + filename);
  }
  std::cout << "[SSTableWriter::WriteMemTableToFile] File opened successfully: " << filename << std::endl;

  std::unique_ptr<SortedTableIterator> iter(memtable.NewIterator());
  if (!iter) {
    std::cerr << "[SSTableWriter::WriteMemTableToFile] ERROR: Failed to create iterator from memtable." << std::endl;
    out_file.close(); // Close the opened file before returning error
    return Result::Corruption("SSTableWriter: Failed to create iterator from memtable.");
  }
  std::cout << "[SSTableWriter::WriteMemTableToFile] Created iterator from memtable." << std::endl;

  iter->SeekToFirst();
  std::cout << "[SSTableWriter::WriteMemTableToFile] Iterator SeekToFirst() called. Initial iter->Valid(): " << iter->Valid() << std::endl;

  std::vector<char> current_data_block_buffer;
  std::vector<char> compressed_block_buffer; // Used if compression shrinks data
  int total_entries_written = 0;
  int safety_loop_count = 0; // For detecting runaway loops

  while (iter->Valid()) {
    safety_loop_count++;
    if (safety_loop_count > 100000) { // Adjust limit as needed
        std::cerr << "[SSTableWriter::WriteMemTableToFile] ERROR: Exceeded safety loop count (" << safety_loop_count 
                  << "), likely an infinite loop in iterator or flush logic. Aborting." << std::endl;
        out_file.close();
        return Result::Error("SSTableWriter: Infinite loop detected while processing memtable entries.");
    }

    Slice key = iter->key();
    ValueEntry value_entry = iter->value(); 

    std::cout << "[SSTableWriter::WriteMemTableToFile] Loop Iteration " << safety_loop_count
              << ". Processing Key: '" << key.ToString() 
              << "', ValueTag: " << static_cast<int>(value_entry.type) 
              << ", ValueSliceSize: " << (value_entry.IsValue() ? value_entry.value_slice.size() : 0)
              << std::endl;

    // Serialize K/V to current_data_block_buffer
    // Store current buffer size before appending this entry for potential rollback or exact size calc
    size_t buffer_size_before_append = current_data_block_buffer.size();

    AppendLittleEndian32(current_data_block_buffer, static_cast<uint32_t>(key.size()));
    AppendBytesToBuffer(current_data_block_buffer, key.data(), key.size());
    current_data_block_buffer.push_back(static_cast<char>(value_entry.type)); 
    
    uint32_t value_size_to_write = 0;
    if (value_entry.IsValue()) {
        value_size_to_write = static_cast<uint32_t>(value_entry.value_slice.size());
    }
    AppendLittleEndian32(current_data_block_buffer, value_size_to_write);
    if (value_entry.IsValue() && value_size_to_write > 0) {
      AppendBytesToBuffer(current_data_block_buffer, value_entry.value_slice.data(), value_size_to_write);
    }
    total_entries_written++;
    std::cout << "[SSTableWriter::WriteMemTableToFile]   Appended entry. Buffer size now: " << current_data_block_buffer.size() << std::endl;

    iter->Next(); // Advance iterator for the next loop or to check iter->Valid() for flush condition

    bool should_flush_this_block = current_data_block_buffer.size() >= target_block_size_;
    // Also flush if this was the last entry from the iterator and there's data in the buffer
    if (!iter->Valid() && !current_data_block_buffer.empty()) {
        std::cout << "[SSTableWriter::WriteMemTableToFile]   Iterator became invalid, will flush remaining data." << std::endl;
        should_flush_this_block = true;
    }

    std::cout << "[SSTableWriter::WriteMemTableToFile]   Iter Valid after Next()? " << iter->Valid()
              << ", Buffer Size: " << current_data_block_buffer.size()
              << ", Target Size: " << target_block_size_
              << ", Should Flush Now? " << should_flush_this_block << std::endl;
    if (iter->Valid()) { 
        std::cout << "[SSTableWriter::WriteMemTableToFile]   Next Key from iter (if valid): '" << iter->key().ToString() << "'" << std::endl;
    }


    if (should_flush_this_block) {
      uint32_t uncompressed_size = static_cast<uint32_t>(current_data_block_buffer.size());
      std::cout << "------------------------------------------------------------\n"
                << "[SSTableWriter::WriteMemTableToFile] FLUSHING BLOCK START\n"
                << "  Target Block Size for writer: " << target_block_size_ << "\n"
                << "  Current Data Block Buffer Size (uncompressed): " << uncompressed_size << std::endl;
      
      // Debug print for entries in the block being flushed (optional, can be verbose)
      if (uncompressed_size > 0) {
          std::cout << "  Content of buffer being flushed (parsed):" << std::endl;
          size_t temp_pos = 0;
          const char* temp_buf_ptr = current_data_block_buffer.data();
          int entry_count_in_block_debug = 0;
          while(temp_pos < uncompressed_size) {
              entry_count_in_block_debug++;
              if (temp_pos + 4 > uncompressed_size) { std::cout << "    DebugParse: Ran out of data for k_len" << std::endl; break;}
              uint32_t k_len = ReadLittleEndian32(temp_buf_ptr + temp_pos); temp_pos += 4;
              if (temp_pos + k_len > uncompressed_size) { std::cout << "    DebugParse: Ran out of data for key" << std::endl; break;}
              std::string k_str(temp_buf_ptr + temp_pos, k_len); temp_pos += k_len;
              if (temp_pos + 1 > uncompressed_size) { std::cout << "    DebugParse: Ran out of data for tag" << std::endl; break;}
              char tag_char = *(temp_buf_ptr + temp_pos); temp_pos += 1;
              if (temp_pos + 4 > uncompressed_size) { std::cout << "    DebugParse: Ran out of data for v_len" << std::endl; break;}
              uint32_t v_len = ReadLittleEndian32(temp_buf_ptr + temp_pos); temp_pos += 4;
              if (temp_pos + v_len > uncompressed_size) { std::cout << "    DebugParse: Ran out of data for value" << std::endl; break;}
              temp_pos += v_len;
              std::cout << "    Entry " << entry_count_in_block_debug << ": Key='" << k_str 
                        << "', Tag=" << static_cast<int>(static_cast<ValueTag>(tag_char))
                        << ", ValLen=" << v_len << std::endl;
          }
           std::cout << "  Total entries debug-parsed in this block: " << entry_count_in_block_debug << std::endl;
      } else {
          std::cout << "  Block buffer is empty, nothing to flush content-wise." << std::endl;
      }
      
      const char* data_to_write_ptr = current_data_block_buffer.data();
      uint32_t on_disk_size = uncompressed_size;
      char current_compression_flag = CompressionType::kNoCompression;

      if (compression_enabled_ && zstd_cctx_ != nullptr && uncompressed_size > 0) {
        std::cout << "[SSTableWriter::WriteMemTableToFile]   Attempting ZSTD compression. Uncompressed size: " << uncompressed_size << std::endl;
        size_t estimated_compressed_bound = ZSTD_compressBound(uncompressed_size);
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
            std::cout << "[SSTableWriter::WriteMemTableToFile]   Compression successful. Compressed size: " << on_disk_size << std::endl;
        } else {
            std::cout << "[SSTableWriter::WriteMemTableToFile]   Compression did not reduce size or failed: " 
                      << (ZSTD_isError(actual_compressed_size_zstd) ? ZSTD_getErrorName(actual_compressed_size_zstd) : "No size reduction")
                      << ". Writing uncompressed." << std::endl;
          // Defaults for uncompressed are already set
        }
      }

      std::vector<char> block_header_buffer;
      block_header_buffer.reserve(9); // sizeof(uint32_t) * 2 + sizeof(char)

      AppendLittleEndian32(block_header_buffer, uncompressed_size);
      AppendLittleEndian32(block_header_buffer, on_disk_size);
      block_header_buffer.push_back(current_compression_flag);
      
      std::cout << "[SSTableWriter::WriteMemTableToFile]   Writing block header: uncomp=" << uncompressed_size 
                << ", on_disk=" << on_disk_size << ", flag=" << (int)current_compression_flag << std::endl;
      out_file.write(block_header_buffer.data(), block_header_buffer.size());
      if (!out_file) {
        std::cerr << "[SSTableWriter::WriteMemTableToFile] ERROR: Failed to write block header to file: " << filename << std::endl;
        current_data_block_buffer.clear(); 
        return Result::IOError("SSTableWriter: Failed to write block header to file: " + filename);
      }
      
      if (on_disk_size > 0) {
        std::cout << "[SSTableWriter::WriteMemTableToFile]   Writing block data payload. Size: " << on_disk_size << std::endl;
        out_file.write(data_to_write_ptr, on_disk_size);
        if (!out_file) {
            std::cerr << "[SSTableWriter::WriteMemTableToFile] ERROR: Failed to write block data payload to file: " << filename << std::endl;
            current_data_block_buffer.clear();
            return Result::IOError("SSTableWriter: Failed to write block data payload to file: " + filename);
        }
      }
      std::cout << "[SSTableWriter::WriteMemTableToFile] FLUSHING BLOCK END" << std::endl;
      current_data_block_buffer.clear(); // Clear buffer for the next block
    }
  } // End while (iter->Valid())

  std::cout << "[SSTableWriter::WriteMemTableToFile] Finished iterating memtable. Total entries written to SSTable (across all blocks): " << total_entries_written << std::endl;

  out_file.close();
  if (out_file.fail()) { // Check fail bit after close
    std::cerr << "[SSTableWriter::WriteMemTableToFile] ERROR: Error reported after closing SSTable file: " << filename << std::endl;
    return Result::IOError("SSTableWriter: Error reported after closing SSTable file: " + filename);
  }

  std::cout << "[SSTableWriter::WriteMemTableToFile] EXIT - Successfully wrote SSTable." << std::endl;
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