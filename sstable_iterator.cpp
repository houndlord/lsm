#include "sstable_iterator.hpp"
#include "sstable_writer.hpp"

#include <vector>
#include <iostream>

// Assuming SSTableReader has methods like:
// const std::vector<char>& SSTableReader::internal_block_buffer() const; (or similar access)
// Result SSTableReader::LoadBlockIntoBuffer(uint64_t block_offset, uint64_t* block_size_on_disk_out);
// uint64_t SSTableReader::file_size() const;
// bool SSTableReader::isOpen() const;

SSTableIterator::SSTableIterator(SSTableReader* reader)
    : reader_(reader),
      current_block_start_offset_(0),
      current_block_total_size_(0),
      next_entry_offset_(0),
      current_value_(ValueTag::kTombstone),
      valid_(false),
      status_(Result::OK()) {
  if (reader_ == nullptr) {
    status_ = Result::InvalidArgument("SSTableIterator: Reader cannot be null.");
    valid_ = false; // valid_ is already false, but for clarity
    return;
  }
  if (!reader_->isOpen()) {
    status_ = Result::NotSupported("SSTableIterator: Reader is not open.");
    valid_ = false; // valid_ is already false
  }
  // Iterator starts invalid; SeekToFirst() or Seek() will position it.
}

bool SSTableIterator::Valid() const {
  return valid_ && status_.ok();
}

Slice SSTableIterator::key() const {
  if (!Valid()) {
    printf("DEBUG ITER::key() called when !Valid(). Returning empty Slice.\n");
    return Slice(); // Return default/empty Slice
  }
  printf("DEBUG ITER::key() called. current_key_ ptr: %p, size: %zu, str: '%.*s'\n",
          (void*)current_key_.data(), current_key_.size(),
          (int)current_key_.size(), current_key_.data() ? (const char*)current_key_.data() : "");
  return current_key_;
}

ValueEntry SSTableIterator::value() const {
  if (!Valid()) {
    // Return a default-constructed ValueEntry which might be a kData with empty slice,
    // or a specific "invalid" marker if ValueEntry supported it.
    // For now, let's make it a tombstone to signify invalid/default.
    printf("DEBUG ITER::value() called when !Valid(). Returning tombstone.\n");
    return ValueEntry(ValueTag::kTombstone); 
  }
  printf("DEBUG ITER::value() called. current_value_ type: %d, slice ptr: %p, size: %zu, str: '%.*s'\n",
          (int)current_value_.type,
          (void*)current_value_.value_slice.data(), current_value_.value_slice.size(),
          (int)current_value_.value_slice.size(), 
          current_value_.value_slice.data() ? (const char*)current_value_.value_slice.data() : "");
  return current_value_;
}

Result SSTableIterator::status() const {
  return status_;
}

Result SSTableIterator::ParseEntryFromBuffer(
    size_t parse_from_offset, Slice* key_out, ValueEntry* value_out,
    size_t* next_entry_start_offset_out) {
  // ... (null checks and initial buffer checks) ...

  const std::vector<char>& block_buffer = reader_->internal_block_buffer();
  const char* block_data_start = block_buffer.data();
  const size_t current_block_uncompressed_size = block_buffer.size();
  size_t current_offset_in_block = parse_from_offset;

  if (current_offset_in_block >= current_block_uncompressed_size) {
    return Result::NotFound("End of block data reached for parsing.");
  }
  // --- DEBUG PRINT: Start of Parse ---
   printf("DEBUG PARSE_ENTRY: Called with parse_from_offset = %zu. Block size = %zu.\n", 
          parse_from_offset, current_block_uncompressed_size);
   printf("DEBUG PARSE_ENTRY: First 16 bytes of block_buffer from offset %zu (hex): ", parse_from_offset);
   for(size_t i = 0; i < 16 && (parse_from_offset + i) < current_block_uncompressed_size; ++i) {
       printf("%02x ", static_cast<unsigned char>(block_buffer[parse_from_offset + i]));
   }
   printf("\n"); // Added missing newline

  // 1. Parse Key Length
  if (current_offset_in_block + sizeof(uint32_t) > current_block_uncompressed_size) {
    return Result::Corruption("ParseEntry: Cannot read key length.");
  }
  uint32_t key_length = ReadLittleEndian32(block_data_start + current_offset_in_block);
  printf("DEBUG PARSE_ENTRY: Read key_length = %u at original offset %zu.\n", key_length, current_offset_in_block);
  current_offset_in_block += sizeof(uint32_t);

  // 2. Parse Key Data
  if (current_offset_in_block + key_length > current_block_uncompressed_size) {
    return Result::Corruption("ParseEntry: Key data extends beyond block boundary.");
  }
  const std::byte* key_data_ptr =
      reinterpret_cast<const std::byte*>(block_data_start + current_offset_in_block);
  *key_out = Slice(key_data_ptr, key_length); // This is where current_key_ gets its value
  current_offset_in_block += key_length;     // Offset is now AFTER key data

  // 3. Parse Value Tag
  if (current_offset_in_block + sizeof(char) > current_block_uncompressed_size) {
    printf("DEBUG PARSE_ENTRY: Corruption - cannot read value tag.\n");
    return Result::Corruption("ParseEntry: Cannot read value tag.");
  }
  // current_offset_in_block is now pointing at the tag
  char tag_byte = *(block_data_start + current_offset_in_block);
  ValueTag current_tag = static_cast<ValueTag>(static_cast<unsigned char>(tag_byte));
  printf("DEBUG PARSE_ENTRY: Read tag = %d at original offset %zu.\n", (int)current_tag, current_offset_in_block);
  current_offset_in_block += sizeof(char); // Offset is now AFTER tag

  // 4. Parse Value Length
  if (current_offset_in_block + sizeof(uint32_t) > current_block_uncompressed_size) {
    return Result::Corruption("ParseEntry: Cannot read value length.");
  }
  // current_offset_in_block is now pointing at value_length
  uint32_t value_length = ReadLittleEndian32(block_data_start + current_offset_in_block);
  printf("DEBUG PARSE_ENTRY: Read value_length = %u at original offset %zu.\n", value_length, current_offset_in_block);
  current_offset_in_block += sizeof(uint32_t); // Offset is now AFTER value_length

  // ... (rest of the function) ...
  // Add the "About to assign" and "Assigned to outputs" printf blocks from my previous suggestion
  // to see the state of the Slices being formed.

  // --- DEBUG PRINT: Before assigning to output parameters ---
  Slice temp_parsed_key_slice = *key_out; // Get what was just assigned
  Slice temp_parsed_value_slice; // Will be set below

  const std::byte* value_data_ptr_for_slice = nullptr; // Renamed to avoid confusion
  if (value_length > 0) {
    // We already checked bounds for value_data_ptr earlier
    value_data_ptr_for_slice =
        reinterpret_cast<const std::byte*>(block_data_start + current_offset_in_block); // This offset is AFTER value_length
    temp_parsed_value_slice = Slice(value_data_ptr_for_slice, value_length);
  } else {
    temp_parsed_value_slice = Slice(); // Empty slice
  }

  printf("<<< DEBUG PARSE_ENTRY: About to construct ValueEntry / assign to outputs >>>\n");
  printf("    Current key_out (already assigned): ptr=%p, size=%zu, str='%.*s'\n",
         (void*)key_out->data(), key_out->size(),
         (int)key_out->size(), key_out->data() ? (const char*)key_out->data() : "");
  printf("    Temp parsed_value_slice for ValueEntry: ptr=%p, size=%zu, str='%.*s'\n",
         (void*)temp_parsed_value_slice.data(), temp_parsed_value_slice.size(),
         (int)temp_parsed_value_slice.size(), temp_parsed_value_slice.data() ? (const char*)temp_parsed_value_slice.data() : "");
  printf("    Tag to be used for ValueEntry: %d\n", (int)current_tag);


  if (current_tag == ValueTag::kData) {
    *value_out = ValueEntry(temp_parsed_value_slice, ValueTag::kData);
  } else if (current_tag == ValueTag::kTombstone) {
    if (value_length != 0) {
      return Result::Corruption("ParseEntry: Tombstone has non-zero value length.");
    }
    *value_out = ValueEntry(ValueTag::kTombstone); // value_slice will be empty by ctor
  } else {
    return Result::Corruption("ParseEntry: Unknown value tag encountered.");
  }

  // --- DEBUG PRINT: After assigning to output parameters ---
  printf("<<< DEBUG PARSE_ENTRY: ValueEntry constructed / assigned to value_out >>>\n");
  printf("    value_out (ValEnt): type=%d, slice_ptr=%p, slice_size=%zu, slice_str='%.*s'\n",
         (int)value_out->type,
         (void*)value_out->value_slice.data(), value_out->value_slice.size(),
         (int)value_out->value_slice.size(), 
         value_out->value_slice.data() ? (const char*)value_out->value_slice.data() : "");


  current_offset_in_block += value_length; // Advance past value data for the next entry's start
  *next_entry_start_offset_out = current_offset_in_block;

  return Result::OK();
}

void SSTableIterator::LoadBlockAndReposition(uint64_t block_start_file_offset) {
  valid_ = false; // Invalidate current state before loading
  if (!reader_ || !reader_->isOpen() || !status_.ok()) {
    if (status_.ok()) { // Only overwrite status if it was previously OK
        status_ = Result::NotSupported("Iterator's reader not usable or iterator in error state.");
    }
    return;
  }

  current_block_start_offset_ = block_start_file_offset; // Update current block offset FIRST
  current_block_total_size_ = 0; // Reset, will be set by LoadBlockIntoBuffer
  next_entry_offset_ = 0;      // Will parse from the start of the new block

  if (current_block_start_offset_ >= reader_->FileSize()) {
    status_ = Result::OK(); // Clean EOF, no more blocks
    valid_ = false;
    return;
  }

  status_ = reader_->LoadBlockIntoBuffer(current_block_start_offset_, &current_block_total_size_);
  if (!status_.ok()) {
    // kNotFound from LoadBlockIntoBuffer means EOF if trying to read past actual file end
    if (status_.code() == ResultCode::kNotFound) {
        status_ = Result::OK(); // Iterator itself is fine, just no more data
    }
    valid_ = false;
    return;
  }

  // If block loaded successfully (even if empty content), try to parse first entry
  if (reader_->internal_block_buffer().empty()) { // Block was empty
      valid_ = false; // No entry to point to
      // Try to load the *next* actual block
      if (current_block_total_size_ > 0) { // Ensure we can advance
        LoadBlockAndReposition(current_block_start_offset_ + current_block_total_size_);
      } else {
        // This case means LoadBlockIntoBuffer returned OK for an empty block at EOF,
        // or a zero-sized block. Iterator is done.
        status_ = Result::OK();
        valid_ = false;
      }
      return;
  }
  std::cout << "DEBUG: Loaded block buffer content (first 32 bytes hex):" << std::endl;
  for (size_t i = 0; i < std::min(reader_->internal_block_buffer().size(), (size_t)32); ++i) {
      printf("%02x ", static_cast<unsigned char>(reader_->internal_block_buffer()[i]));
  }
  std::cout << std::endl;

  Result parse_res = ParseEntryFromBuffer(
      0, /* parse from start of newly loaded buffer */
      &current_key_, &current_value_,
      &next_entry_offset_ /* store offset for next call to ParseEntryFromBuffer */);

  if (parse_res.ok()) {
    valid_ = true;
    status_ = Result::OK();
  } else if (parse_res.code() == ResultCode::kNotFound) {
    // This means the loaded block was valid but contained no entries (e.g. only padding, or empty)
    // This is unusual if LoadBlockIntoBuffer returned OK and buffer wasn't empty.
    // Let's try to load the next block.
    valid_ = false;
    status_ = Result::OK(); // Not an iterator error yet
    if (current_block_total_size_ > 0) {
        LoadBlockAndReposition(current_block_start_offset_ + current_block_total_size_);
    } else {
        // Infinite loop guard for zero-sized blocks
        status_ = Result::Corruption("Zero-sized block encountered during iteration.");
    }
  } else {
    // Corruption or other error during parsing
    status_ = parse_res;
    valid_ = false;
  }
}

void SSTableIterator::SeekToFirst() {
  status_ = Result::OK(); // Reset status
  valid_ = false;
  if (!reader_ || !reader_->isOpen()) {
    status_ = Result::NotSupported(
        "Iterator's reader is not usable for SeekToFirst.");
    return;
  }
  // current_block_start_offset_ and current_block_total_size_ will be set by LoadBlockAndReposition
  LoadBlockAndReposition(0); // Load block starting at file offset 0
}

void SSTableIterator::Next() {
  if (!valid_) {
    // If status_ is not OK, Valid() would be false, so this call is a no-op.
    // If status_ is OK but valid_ is false, it means we are at EOF.
    return;
  }
  if (!status_.ok()) { // Already in error state
    valid_ = false;
    return;
  }

  Result parse_res = ParseEntryFromBuffer(
      next_entry_offset_, &current_key_, &current_value_, &next_entry_offset_);

  if (parse_res.ok()) {
    valid_ = true;
    status_ = Result::OK();
    return;
  }

  if (parse_res.code() == ResultCode::kNotFound) { // End of current block
    status_ = Result::OK(); // Not an iterator error
    // Try to load the next block
    if (current_block_total_size_ == 0 && current_block_start_offset_ < reader_->FileSize()) {
        // This can happen if LoadBlockIntoBuffer returned OK but set total_size to 0
        // for an empty block, or if we are at the very end.
        // Attempt to move to the next logical position, if any.
        // This state might indicate an issue with LoadBlockIntoBuffer not properly signaling EOF.
        // For safety, if total_size is 0, consider it end of data unless file_size also suggests it.
        // A robust LoadBlockIntoBuffer should probably return NotFound if it can't load a meaningful block.
         status_ = Result::Corruption("Encountered zero-sized block during Next operation.");
         valid_ = false;
         return;
    }
    LoadBlockAndReposition(current_block_start_offset_ + current_block_total_size_);
  } else {
    // Corruption or other error from parsing
    status_ = parse_res;
    valid_ = false;
  }
}

void SSTableIterator::Seek(const Slice& target) {
  status_ = Result::OK();
  valid_ = false;
  if (!reader_ || !reader_->isOpen()) {
    status_ = Result::NotSupported("Iterator's reader not usable for Seek.");
    return;
  }

  // Inefficient seek: Go to the start and scan forward.
  // An index would make this much faster.
  SeekToFirst();
  if (!Valid()) { // SeekToFirst failed or SSTable is empty
    return;
  }

  while (Valid()) {
    if (current_key_.compare(target) >= 0) {
      // Found: current_key_ is the first key >= target
      return;
    }
    Next(); // Advances to the next entry, updates valid_ and status_
  }
  // If loop finishes, iterator is invalid (either EOF or error occurred in Next).
  // valid_ is false. status_ will hold OK for EOF or an error code.
}