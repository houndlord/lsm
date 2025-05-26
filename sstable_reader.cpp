#include "sstable_reader.hpp"
#include "result.hpp"
#include "sstable_writer.hpp"

SSTableReader::SSTableReader(std::string filename) : filename_(filename) { };

SSTableReader::~SSTableReader() {
  if (file_stream_.is_open()) {
    file_stream_.close();
  }
  if (zstd_dctx_ != nullptr) {
    ZSTD_freeDCtx(zstd_dctx_);
    zstd_dctx_ = nullptr;
  }
}
Result SSTableReader::Init() {
  file_stream_ = std::ifstream(filename_, std::ios::binary);
  if (!file_stream_.is_open()) {
    return Result::IOError("Failed to open SSTable: " + filename_);
  }
  file_stream_.seekg(0, std::ios::end);
  file_size_ = file_stream_.tellg();
  file_stream_.seekg(0, std::ios::beg);
  if (file_stream_.fail()) { // Check for errors after seeking
      file_stream_.close();
      return Result::IOError("SSTableReader: Failed to determine file size or seek for: " + filename_);
  }

  zstd_dctx_ = ZSTD_createDCtx();
  if (zstd_dctx_ == nullptr) {
      file_stream_.close(); // Clean up already opened file
      return Result::IOError("SSTableReader: Failed to create ZSTD decompression context.");
  }
  
  is_open_ = true;
  return Result::OK();
}

Result SSTableReader::LoadBlockIntoBuffer(uint64_t block_offset, uint64_t* block_size_on_disk_out) {
  if (!is_open_) {
    return Result::NotSupported("SSTableReader is not open.");
  }
  if (block_offset > file_size_) { // Simplified: if offset > size, it's definitely out.
     return Result::IOError("Block offset out of bounds.");
  }
  // Special case for trying to read block 0 of an empty file
  if (file_size_ == 0 && block_offset == 0) {
      internal_block_buffer_.clear();
      if (block_size_on_disk_out) *block_size_on_disk_out = 0;
      return Result::NotFound("EOF reached, no more blocks (empty file).");
  }


  internal_block_buffer_.clear();
  file_stream_.seekg(block_offset);
  if (file_stream_.fail()) {
      return Result::IOError("Failed to seek to block offset: " + std::to_string(block_offset));
  }

  char header_buf[9];
  file_stream_.read(header_buf, 9);

  if (file_stream_.gcount() == 0 && file_stream_.eof() && block_offset == file_size_) {
      if (block_size_on_disk_out) *block_size_on_disk_out = 0;
      return Result::NotFound("EOF reached, no more blocks.");
  }
  if (file_stream_.gcount() != 9) {
    if (file_stream_.eof()) {
        return Result::IOError("EOF while trying to read block header at offset " + std::to_string(block_offset));
      }
    return Result::Corruption("Failed to read full 9-byte block header at offset " + std::to_string(block_offset));
  }
  uint32_t uncompressed_size = ReadLittleEndian32(header_buf);
  uint32_t payload_size = ReadLittleEndian32(header_buf + 4);
  char compression_flag = header_buf[8];

  if (block_offset + 9 + payload_size > file_size_) {
    return Result::Corruption("Block size exceeds file bounds. Offset: " + std::to_string(block_offset) + ", Header indicates payload " + std::to_string(payload_size));
  }

  std::vector<char> raw_disk_data(payload_size);
  if (payload_size > 0) {
    // Corrected: use .data() for std::vector with istream::read
    file_stream_.read(raw_disk_data.data(), payload_size);
    if (file_stream_.gcount() != payload_size) {
        return Result::Corruption("Failed to read full block payload. Expected " + std::to_string(payload_size) + ", got " + std::to_string(file_stream_.gcount()));
    }
  }

  if (compression_flag == CompressionType::kZstdCompressed) {
    if (uncompressed_size == 0 && payload_size > 0) { // Though ZSTD typically won't compress to non-zero if input is 0
        return Result::Corruption("ZSTD block has 0 uncompressed size but non-zero payload.");
    }
    if (uncompressed_size > 0) {
        internal_block_buffer_.resize(uncompressed_size);
        size_t decompressed_size = ZSTD_decompressDCtx(zstd_dctx_, internal_block_buffer_.data(), uncompressed_size, raw_disk_data.data(), payload_size);
        if (ZSTD_isError(decompressed_size) || decompressed_size != uncompressed_size) {
            return Result::Corruption("Zstd decompression error or size mismatch. Error: " + std::string(ZSTD_getErrorName(decompressed_size)));
        }
    } else {
        internal_block_buffer_.clear();
    }
  } else if (compression_flag == CompressionType::kNoCompression) {
    if (uncompressed_size != payload_size) {
      return Result::Corruption("Size mismatch for uncompressed block.");
    }
    if (uncompressed_size > 0) {
      // Corrected: use assign to copy contents from one vector to another
      internal_block_buffer_.assign(raw_disk_data.begin(), raw_disk_data.end());
    } else {
        internal_block_buffer_.clear();
    }
  } else {
      return Result::NotSupported("Unknown compression flag: " + std::to_string(static_cast<int>(compression_flag)));
  }

  if (block_size_on_disk_out) *block_size_on_disk_out = 9 + payload_size;
  return Result::OK();
}

Result SSTableReader::Get(const Slice& search_key, Arena* arena) {
  if (!isOpen()) {
      return Result::NotSupported("SSTableReader not open. Call Init() first.");
  }
  if (search_key.empty()) {
      return Result::InvalidArgument("Search key cannot be empty.");
  }
  if (arena == nullptr) {
      return Result::InvalidArgument("Arena cannot be null for Get operation.");
  }

  uint64_t current_block_disk_offset = 0;

  // Loop condition handles empty file (file_size_ == 0) correctly
  while (current_block_disk_offset < file_size_ || (file_size_ == 0 && current_block_disk_offset == 0) ) {
    uint64_t current_block_total_size_on_disk = 0; // Initialize
    Result load_res = LoadBlockIntoBuffer(current_block_disk_offset, &current_block_total_size_on_disk);

    if (!load_res.ok()) {
        if (load_res.code() == ResultCode::kNotFound) {
            break;
        }
        return load_res;
    }

    if (internal_block_buffer_.empty() && load_res.ok()){
        if (current_block_total_size_on_disk == 0) {
            if (file_size_ > 0) return Result::Corruption("Loaded zero-sized block from non-empty SSTable.");
            else break; 
        }
        current_block_disk_offset += current_block_total_size_on_disk;
        continue;
    }

    const char* block_data_start = internal_block_buffer_.data();
    uint32_t uncompressed_block_size = static_cast<uint32_t>(internal_block_buffer_.size());
    size_t offset_in_block = 0;

    while (offset_in_block < uncompressed_block_size) {
        if (offset_in_block + sizeof(uint32_t) > uncompressed_block_size) {
            return Result::Corruption("Corrupted block: cannot read key length at offset " + std::to_string(offset_in_block));
        }
        uint32_t key_length = ReadLittleEndian32(block_data_start + offset_in_block);
        offset_in_block += sizeof(uint32_t);

        if (offset_in_block + key_length > uncompressed_block_size) {
            return Result::Corruption("Corrupted block: key data extends beyond boundary from offset " + std::to_string(offset_in_block));
        }
        const std::byte* key_data_ptr = reinterpret_cast<const std::byte*>(block_data_start + offset_in_block);
        Slice current_key(key_data_ptr, key_length);

        int cmp = current_key.compare(search_key);
        offset_in_block += key_length;

        if (offset_in_block + sizeof(char) > uncompressed_block_size) {
            return Result::Corruption("Corrupted block: cannot read value tag for key '" + current_key.ToString() + "'");
        }
        char tag_byte = *(block_data_start + offset_in_block);
        ValueTag current_tag = static_cast<ValueTag>(static_cast<unsigned char>(tag_byte));
        offset_in_block += sizeof(char);

        if (offset_in_block + sizeof(uint32_t) > uncompressed_block_size) {
            return Result::Corruption("Corrupted block: cannot read value length for key '" + current_key.ToString() + "'");
        }
        uint32_t value_length = ReadLittleEndian32(block_data_start + offset_in_block);
        offset_in_block += sizeof(uint32_t);

        const std::byte* value_data_ptr = nullptr;
        if (value_length > 0) {
              if (offset_in_block + value_length > uncompressed_block_size) {
                return Result::Corruption("Corrupted block: value data for key '" + current_key.ToString() + "' extends beyond boundary");
            }
            value_data_ptr = reinterpret_cast<const std::byte*>(block_data_start + offset_in_block);
        }

        if (cmp == 0) {
            if (current_tag == ValueTag::kTombstone) {
                return Result::NotFound(search_key.ToString() + " (is a tombstone)");
            }
            if (current_tag == ValueTag::kData) {
                void* arena_mem = arena->Allocate(value_length, alignof(std::byte));
                if (value_length > 0 && arena_mem == nullptr) {
                    return Result::ArenaAllocationFail("Failed to allocate memory in arena for value.");
                }
                if (value_length > 0) {
                   std::memcpy(arena_mem, value_data_ptr, value_length);
                }
                Slice value_slice_in_arena(static_cast<const std::byte*>(arena_mem), value_length);
                return Result::OK(value_slice_in_arena);
            }
            return Result::Corruption("Unknown value tag for key '" + search_key.ToString() + "'");
        }
        offset_in_block += value_length;
    }

    if (current_block_total_size_on_disk == 0) {
         if (file_size_ > 0 && !internal_block_buffer_.empty()) {
            return Result::Corruption("Internal error: Processed block with zero reported disk size but non-zero content.");
         }
         break; 
    }
    current_block_disk_offset += current_block_total_size_on_disk;
    // If file_size_ is 0, and we started with offset 0, and total_size_on_disk is 0, this will exit.
    if (current_block_disk_offset >= file_size_ && file_size_ == 0 && current_block_total_size_on_disk == 0) break;

  }

  return Result::NotFound(search_key.ToString() + " (not found in any block)");
}