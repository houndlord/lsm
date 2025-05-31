#include "sstable_reader.hpp"

#include <cstring> // For std::memcpy
#include <iostream> // For debug prints

#include "sstable_writer.hpp" // For ReadLittleEndian32 and CompressionType
#include "result.hpp"       // Ensure this is the updated Result.hpp
#include "value.hpp"        // For ValueTag

// SSTableReader Constructor, Destructor, Init, LoadBlockIntoBuffer remain the same
// as your last provided version.

SSTableReader::SSTableReader(std::string filename)
    : filename_(std::move(filename)),
      zstd_dctx_(nullptr),
      is_open_(false),
      file_size_(0) {
    std::cout << "[SSTableReader Constructor] Filename: " << filename_ << std::endl;
}

SSTableReader::~SSTableReader() {
  std::cout << "[SSTableReader Destructor] Closing: " << filename_ << std::endl;
  if (file_stream_.is_open()) {
    file_stream_.close();
  }
  if (zstd_dctx_ != nullptr) {
    ZSTD_freeDCtx(zstd_dctx_);
    zstd_dctx_ = nullptr;
  }
}

Result SSTableReader::Init() {
  std::cout << "[SSTableReader::Init] Initializing for: " << filename_ << std::endl;
  if (is_open_) {
    return Result::NotSupported("SSTableReader already initialized.");
  }

  file_stream_.open(filename_, std::ios::binary);
  if (!file_stream_.is_open() || file_stream_.fail()) { // Check fail immediately
    std::cout << "[SSTableReader::Init] Failed to open SSTable (is_open or fail): " << filename_ << std::endl;
    if(file_stream_.is_open()) file_stream_.close(); // Ensure closed if open failed partially
    return Result::IOError("Failed to open SSTable: " + filename_);
  }

  file_stream_.seekg(0, std::ios::end);
  if (file_stream_.fail()) {
    std::cout << "[SSTableReader::Init] Failed to seek to end of file: " << filename_ << std::endl;
    file_stream_.close();
    return Result::IOError("SSTableReader: Failed to seek to end of file for: " + filename_);
  }
  file_size_ = file_stream_.tellg();
  std::cout << "[SSTableReader::Init] File size: " << file_size_ << " for " << filename_ << std::endl;
  file_stream_.seekg(0, std::ios::beg);
  if (file_stream_.fail()) {
    std::cout << "[SSTableReader::Init] Failed to seek to beginning of file: " << filename_ << std::endl;
    file_stream_.close();
    return Result::IOError("SSTableReader: Failed to seek to beginning of file for: " + filename_);
  }

  zstd_dctx_ = ZSTD_createDCtx();
  if (zstd_dctx_ == nullptr) {
    std::cout << "[SSTableReader::Init] Failed to create ZSTD decompression context." << std::endl;
    file_stream_.close();
    return Result::IOError("SSTableReader: Failed to create ZSTD decompression context.");
  }

  is_open_ = true;
  std::cout << "[SSTableReader::Init] Successfully initialized: " << filename_ << std::endl;
  return Result::OK();
}

Result SSTableReader::LoadBlockIntoBuffer(
    uint64_t block_offset, uint64_t* block_size_on_disk_out) {
  // std::cout << "[SSTableReader::LoadBlockIntoBuffer] Offset: " << block_offset << std::endl; // Can be noisy
  if (!is_open_) {
    return Result::NotSupported("SSTableReader is not open.");
  }
  if (block_size_on_disk_out) {
    *block_size_on_disk_out = 0;
  }

  if (block_offset >= file_size_) {
    // This condition also handles file_size_ == 0 correctly if block_offset is 0 or more.
    internal_block_buffer_.clear();
    // std::cout << "[SSTableReader::LoadBlockIntoBuffer] EOF (offset >= file_size)." << std::endl; // Can be noisy
    return Result::NotFound("EOF (offset out of bounds or empty file).");
  }

  internal_block_buffer_.clear();
  file_stream_.seekg(block_offset);
  if (file_stream_.fail() || file_stream_.eof()) { // Check eof too after seek
    // If seekg goes past EOF, eofbit is set, failbit might be set.
    std::cout << "[SSTableReader::LoadBlockIntoBuffer] Failed to seek to block offset " << block_offset 
              << " or hit EOF. fail(): " << file_stream_.fail() << ", eof(): " << file_stream_.eof() << std::endl;
    return Result::IOError("Failed to seek to block offset: " + std::to_string(block_offset));
  }

  char header_buf[sizeof(uint32_t) + sizeof(uint32_t) + sizeof(char)]; // 9 bytes
  file_stream_.read(header_buf, sizeof(header_buf));

  if (static_cast<size_t>(file_stream_.gcount()) != sizeof(header_buf)) {
    // This can happen if we try to read a header at the very end of the file with < 9 bytes remaining.
    std::cout << "[SSTableReader::LoadBlockIntoBuffer] Failed to read full block header at offset " 
              << block_offset << "; read " << file_stream_.gcount() << " bytes. EOF: " << file_stream_.eof() << std::endl;
    if (file_stream_.eof() && file_stream_.gcount() == 0 && block_offset == file_size_) {
        return Result::NotFound("Clean EOF reached at header read attempt."); // Attempt to read header at exact EOF
    }
    return Result::Corruption("Failed to read full block header at offset " + std::to_string(block_offset));
  }

  uint32_t uncompressed_size = ReadLittleEndian32(header_buf);
  uint32_t on_disk_payload_size = ReadLittleEndian32(header_buf + sizeof(uint32_t));
  char compression_flag = header_buf[sizeof(uint32_t) + sizeof(uint32_t)];
  // std::cout << "[SSTableReader::LoadBlockIntoBuffer] Header: uncomp=" << uncompressed_size 
  //           << ", on_disk_payload=" << on_disk_payload_size << ", flag=" << (int)compression_flag << std::endl;


  if (block_offset + sizeof(header_buf) + on_disk_payload_size > file_size_) {
    std::cout << "[SSTableReader::LoadBlockIntoBuffer] Corruption: Block physical size " 
              << (sizeof(header_buf) + on_disk_payload_size) << " from offset " << block_offset 
              << " exceeds file_size_ " << file_size_ << std::endl;
    return Result::Corruption("Block physical size exceeds file bounds.");
  }

  std::vector<char> raw_disk_data(on_disk_payload_size);
  if (on_disk_payload_size > 0) {
    file_stream_.read(raw_disk_data.data(), on_disk_payload_size);
    if (static_cast<uint32_t>(file_stream_.gcount()) != on_disk_payload_size) {
      std::cout << "[SSTableReader::LoadBlockIntoBuffer] Corruption: Failed to read full block payload. Expected "
                << on_disk_payload_size << ", got " << file_stream_.gcount() << std::endl;
      return Result::Corruption("Failed to read full block payload.");
    }
  }

  if (compression_flag == CompressionType::kZstdCompressed) {
    if (uncompressed_size == 0 && on_disk_payload_size > 0) {
      return Result::Corruption("ZSTD block has 0 uncompressed size but non-zero payload.");
    }
    if (uncompressed_size > 0) { // Only decompress if there's something to decompress to
        if (on_disk_payload_size == 0) { // Cannot decompress from an empty payload if uncompressed_size > 0
            return Result::Corruption("ZSTD block expects uncompressed data but on-disk payload is empty.");
        }
      internal_block_buffer_.resize(uncompressed_size);
      size_t decompressed_size = ZSTD_decompressDCtx(
          zstd_dctx_, internal_block_buffer_.data(), uncompressed_size,
          raw_disk_data.data(), on_disk_payload_size);
      if (ZSTD_isError(decompressed_size) || decompressed_size != uncompressed_size) {
        std::cout << "[SSTableReader::LoadBlockIntoBuffer] Zstd decompression error or size mismatch. ZSTD Err: " 
                  << ZSTD_getErrorName(decompressed_size) << ", Expected size: " << uncompressed_size 
                  << ", Got: " << decompressed_size << std::endl;
        return Result::Corruption("Zstd decompression error or size mismatch. Error: " + std::string(ZSTD_getErrorName(decompressed_size)));
      }
    } else { // uncompressed_size is 0
        internal_block_buffer_.clear(); // Ensure buffer is empty
    }
  } else if (compression_flag == CompressionType::kNoCompression) {
    if (uncompressed_size != on_disk_payload_size) {
      return Result::Corruption("Size mismatch for uncompressed block: uncompressed=" + std::to_string(uncompressed_size) + ", payload=" + std::to_string(on_disk_payload_size));
    }
    if (uncompressed_size > 0) {
      internal_block_buffer_.assign(raw_disk_data.begin(), raw_disk_data.end());
    } else {
        internal_block_buffer_.clear();
    }
  } else {
    return Result::NotSupported("Unknown compression flag: " + std::to_string(static_cast<int>(compression_flag)));
  }

  if (block_size_on_disk_out) {
    *block_size_on_disk_out = sizeof(header_buf) + on_disk_payload_size;
  }
  // std::cout << "[SSTableReader::LoadBlockIntoBuffer] Successfully loaded and processed block. Internal buffer size: " << internal_block_buffer_.size() << std::endl;
  return Result::OK();
}

SSTableReader::ParsedEntryInfo SSTableReader::ParseNextEntry(
    const char* block_data_start, size_t block_size,
    size_t current_offset_in_block_param) { // Renamed param for clarity
  ParsedEntryInfo entry_info; // entry_info.entry_size_in_block is 0 by default

  // Capture the starting offset of the current entry
  size_t offset_at_entry_start = current_offset_in_block_param;
  // Use a local variable for iterating within this function
  size_t current_offset_in_block = current_offset_in_block_param;


  // std::cout << "[SSTableReader::ParseNextEntry] offset_in_block: " << current_offset_in_block << ", block_size: " << block_size << std::endl;

  if (current_offset_in_block == block_size) { // Clean end of block
    entry_info.status = Result::NotFound("Clean end of block.");
    return entry_info;
  }
  // Check if there's enough space for even the smallest possible entry (key_len + tag + val_len)
  if (current_offset_in_block + sizeof(uint32_t) + sizeof(char) + sizeof(uint32_t) > block_size) {
    entry_info.status = Result::Corruption("Corrupted block: not enough space for minimal entry headers at offset " + std::to_string(current_offset_in_block));
    return entry_info;
  }

  uint32_t key_length = ReadLittleEndian32(block_data_start + current_offset_in_block);
  current_offset_in_block += sizeof(uint32_t);

  if (current_offset_in_block + key_length > block_size) {
    entry_info.status = Result::Corruption("Corrupted block: key data extends beyond boundary from offset " + std::to_string(current_offset_in_block));
    return entry_info;
  }
  entry_info.key = Slice(reinterpret_cast<const std::byte*>(block_data_start + current_offset_in_block), key_length);
  current_offset_in_block += key_length;

  if (current_offset_in_block + sizeof(char) > block_size) {
    entry_info.status = Result::Corruption("Corrupted block: cannot read value tag for key '" + entry_info.key.ToString() + "'");
    return entry_info;
  }
  entry_info.tag = static_cast<ValueTag>(static_cast<unsigned char>(*(block_data_start + current_offset_in_block)));
  current_offset_in_block += sizeof(char);

  if (current_offset_in_block + sizeof(uint32_t) > block_size) {
    entry_info.status = Result::Corruption("Corrupted block: cannot read value length for key '" + entry_info.key.ToString() + "'");
    return entry_info;
  }
  uint32_t value_length = ReadLittleEndian32(block_data_start + current_offset_in_block);
  current_offset_in_block += sizeof(uint32_t);

  if (value_length > 0) {
    if (current_offset_in_block + value_length > block_size) {
      entry_info.status = Result::Corruption("Corrupted block: value data for key '" + entry_info.key.ToString() + "' extends beyond boundary");
      return entry_info;
    }
    entry_info.value_in_block = Slice(reinterpret_cast<const std::byte*>(block_data_start + current_offset_in_block), value_length);
  } else {
    entry_info.value_in_block = Slice(); 
  }
  current_offset_in_block += value_length; // current_offset_in_block is now at the end of this entry

  // --- THIS IS THE FIX ---
  if (entry_info.status.ok()) {
    entry_info.entry_size_in_block = current_offset_in_block - offset_at_entry_start;
  }
  // No need for 'else' for entry_size_in_block, as if status is not ok, 
  // the Get loop will break/return anyway.

  // The old commented-out section related to entry_size_in_block can be removed.
  
  return entry_info;
}


Result SSTableReader::Get(const Slice& search_key, Arena* arena_for_value_copy) {
  std::cout << "[SSTableReader::Get Arena*] Key: " << search_key.ToString() << std::endl;
  if (!is_open_) {
    return Result::NotSupported("SSTableReader not open. Call Init() first.");
  }
  if (search_key.empty()) {
    return Result::InvalidArgument("Search key cannot be empty.");
  }
  if (arena_for_value_copy == nullptr) {
    return Result::InvalidArgument("Arena cannot be null for Get operation requiring Arena copy.");
  }

  uint64_t current_block_disk_offset = 0;
  while (current_block_disk_offset < file_size_) {
    uint64_t current_block_total_size_on_disk = 0;
    Result load_res = LoadBlockIntoBuffer(current_block_disk_offset, &current_block_total_size_on_disk);

    if (!load_res.ok()) {
      if (load_res.code() == ResultCode::kNotFound) { // EOF reached
        std::cout << "[SSTableReader::Get Arena*] LoadBlock returned NotFound (EOF)." << std::endl;
        break;
      }
      std::cout << "[SSTableReader::Get Arena*] LoadBlock failed: " << load_res.message() << std::endl;
      return load_res; // Corruption or IO error
    }

    if (internal_block_buffer_.empty()) {
      current_block_disk_offset += current_block_total_size_on_disk;
       if (current_block_total_size_on_disk == 0 && current_block_disk_offset < file_size_) {
         return Result::Corruption("Encountered zero-sized block in non-empty SSTable before EOF.");
      }
      continue;
    }

    const char* block_data_start = internal_block_buffer_.data();
    size_t uncompressed_block_size = internal_block_buffer_.size();
    size_t offset_in_block = 0;

    while (offset_in_block < uncompressed_block_size) {
      ParsedEntryInfo entry_info = ParseNextEntry(block_data_start, uncompressed_block_size, offset_in_block);

      if (!entry_info.status.ok()) {
        if (entry_info.status.code() == ResultCode::kNotFound) { // Clean end of block
            break; 
        }
        std::cout << "[SSTableReader::Get Arena*] ParseNextEntry failed: " << entry_info.status.message() << std::endl;
        return entry_info.status; // Corruption in block
      }

      if (entry_info.key.compare(search_key) == 0) {
        if (entry_info.tag == ValueTag::kTombstone) {
          std::cout << "[SSTableReader::Get Arena*] Found TOMBSTONE for key " << search_key.ToString() << std::endl;
          return Result::OkTombstone(); // Use the new static factory
        }
        if (entry_info.tag == ValueTag::kData) {
          std::cout << "[SSTableReader::Get Arena*] Found DATA for key " << search_key.ToString() << ". Value size: " << entry_info.value_in_block.size() << std::endl;
          void* arena_mem = arena_for_value_copy->Allocate(
              entry_info.value_in_block.size(), alignof(std::byte));
          if (entry_info.value_in_block.size() > 0 && arena_mem == nullptr) {
            return Result::ArenaAllocationFail("Failed to allocate memory in arena for value.");
          }
          if (entry_info.value_in_block.size() > 0) {
            std::memcpy(arena_mem, entry_info.value_in_block.data(), entry_info.value_in_block.size());
          }
          // Result::OK(Slice) constructor correctly sets value_tag_ to kData
          std::cout << "[SSTableReader::Get Arena*] Found DATA for key " << search_key.ToString() 
                    << ". Value slice ptr in arena: " << (void*)arena_mem << std::endl; // DEBUG
          return Result::OK(Slice(static_cast<const std::byte*>(arena_mem), entry_info.value_in_block.size()));
        }
        // Should not happen if tags are only kData or kTombstone
        return Result::Corruption("Unknown value tag encountered for key '" + search_key.ToString() + "'");
      }
      // If current entry's key is greater than search_key, and keys are sorted,
      // search_key cannot be in this block or any subsequent block in this file.
      // This is an optimization if your SSTable blocks store keys in sorted order.
      // if (entry_info.key.compare(search_key) > 0) {
      //    std::cout << "[SSTableReader::Get Arena*] Current key " << entry_info.key.ToString() 
      //              << " > search_key " << search_key.ToString() << ". Key not in this block/SSTable." << std::endl;
      //    return Result::NotFound(search_key.ToString() + " (not found by sorted order check)");
      // }
      offset_in_block += entry_info.entry_size_in_block; // This was missing!
    }
    current_block_disk_offset += current_block_total_size_on_disk;
  }

  std::cout << "[SSTableReader::Get Arena*] Key " << search_key.ToString() << " not found in any block." << std::endl;
  return Result::NotFound(search_key.ToString() + " (not found in this SSTable)");
}

// Get version for std::string output
Result SSTableReader::Get(const Slice& search_key, std::string* value_out) {
  std::cout << "[SSTableReader::Get string*] Key: " << search_key.ToString() << std::endl;
  if (value_out == nullptr) {
    return Result::InvalidArgument("Output string pointer is null.");
  }
  value_out->clear();

  if (!is_open_) {
    return Result::NotSupported("SSTableReader not open. Call Init() first.");
  }
  if (search_key.empty()) {
    return Result::InvalidArgument("Search key cannot be empty.");
  }

  // This Get version does not need an external arena to copy into,
  // as it copies directly to std::string.
  // We will use the internal_block_buffer_ which LoadBlockIntoBuffer populates.
  uint64_t current_block_disk_offset = 0;
  while (current_block_disk_offset < file_size_) {
    uint64_t current_block_total_size_on_disk = 0;
    Result load_res = LoadBlockIntoBuffer(current_block_disk_offset, &current_block_total_size_on_disk);

    if (!load_res.ok()) {
      if (load_res.code() == ResultCode::kNotFound) { // EOF
         std::cout << "[SSTableReader::Get string*] LoadBlock returned NotFound (EOF)." << std::endl;
        break;
      }
      std::cout << "[SSTableReader::Get string*] LoadBlock failed: " << load_res.message() << std::endl;
      return load_res;
    }

    if (internal_block_buffer_.empty()) {
      current_block_disk_offset += current_block_total_size_on_disk;
       if (current_block_total_size_on_disk == 0 && current_block_disk_offset < file_size_) {
         return Result::Corruption("Encountered zero-sized block in non-empty SSTable before EOF.");
      }
      continue;
    }

    const char* block_data_start = internal_block_buffer_.data();
    size_t uncompressed_block_size = internal_block_buffer_.size();
    size_t offset_in_block = 0;

    while (offset_in_block < uncompressed_block_size) {
      ParsedEntryInfo entry_info = ParseNextEntry(block_data_start, uncompressed_block_size, offset_in_block);

      if (!entry_info.status.ok()) {
        if (entry_info.status.code() == ResultCode::kNotFound) { // Clean end of block
            break; 
        }
         std::cout << "[SSTableReader::Get string*] ParseNextEntry failed: " << entry_info.status.message() << std::endl;
        return entry_info.status; // Corruption
      }

      if (entry_info.key.compare(search_key) == 0) {
        if (entry_info.tag == ValueTag::kTombstone) {
           std::cout << "[SSTableReader::Get string*] Found TOMBSTONE for key " << search_key.ToString() << std::endl;
          return Result::OkTombstone(); // Signal tombstone correctly
        }
        if (entry_info.tag == ValueTag::kData) {
          std::cout << "[SSTableReader::Get string*] Found DATA for key " << search_key.ToString() << ". Value size: " << entry_info.value_in_block.size() << std::endl;
          if (entry_info.value_in_block.size() > 0) {
            value_out->assign(
                reinterpret_cast<const char*>(entry_info.value_in_block.data()),
                entry_info.value_in_block.size());
          }
          // Result::OK() default constructor is fine here, value_tag_ will be kData by Slice constructor
          // if we returned Result::OK(entry_info.value_in_block), but we don't need to for string version.
          // Just Result::OK() is enough as data is in value_out.
          // However, to be consistent with DB::GetInternal expectations, the Result should indicate it's data.
          // The public DB::Get(string*) will get Result::OK() from this path, and its internal_res.value_tag_ will be kData
          // because the value_slice IS populated by the Result(Slice) constructor.
          // So, this should be:
          std::cout << "[SSTableReader::Get string*] Found DATA for key " << search_key.ToString() 
                    << ". Value copied to string." << std::endl; // DEBUG
          return Result::OK(entry_info.value_in_block); // Pass the slice, Result ctor will set tag.
        }
        return Result::Corruption("Unknown value tag encountered for key '" + search_key.ToString() + "'");
      }
      // Optional optimization:
      // if (entry_info.key.compare(search_key) > 0) {
      //    return Result::NotFound(search_key.ToString() + " (not found by sorted order check)");
      // }
      offset_in_block += entry_info.entry_size_in_block; // This was missing!
    }
    current_block_disk_offset += current_block_total_size_on_disk;
  }
  std::cout << "[SSTableReader::Get string*] Key " << search_key.ToString() << " not found in any block." << std::endl;
  return Result::NotFound(search_key.ToString() + " (not found in this SSTable)");
}