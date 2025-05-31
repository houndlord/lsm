#include "sstable_iterator.hpp"

#include <algorithm> // For std::min
#include <vector>
#include <cstdio> // For printf
#include <iostream> // For std::cout, std::endl (used in debug prints)

// Ensure sstable_writer.hpp is included for ReadLittleEndian32
#include "sstable_writer.hpp"


SSTableIterator::SSTableIterator(SSTableReader* reader)
    : reader_(reader),
      current_block_start_offset_(0),
      current_block_total_size_(0),
      current_entry_offset_in_block_(0),
      current_value_(ValueTag::kTombstone),
      valid_(false),
      status_(Result::OK()) {
  if (reader_ == nullptr) {
    status_ = Result::InvalidArgument("SSTableIterator: Reader cannot be null.");
    valid_ = false;
    return;
  }
  if (!reader_->IsOpen()) {
    status_ = Result::NotSupported("SSTableIterator: Reader is not open.");
    valid_ = false;
  }
}

bool SSTableIterator::Valid() const {
  return valid_ && status_.ok();
}

Slice SSTableIterator::key() const {
  if (!Valid()) {
    printf("DEBUG ITER::key() called when !Valid(). Returning empty Slice.\n");
    return Slice();
  }
   printf("DEBUG ITER::key() called. current_key_ ptr: %p, size: %zu, str: '%.*s'\n",
          (void*)current_key_.data(), current_key_.size(),
          (int)current_key_.size(), current_key_.data() ? (const char*)current_key_.data() : "");
  return current_key_;
}

ValueEntry SSTableIterator::value() const {
  if (!Valid()) {
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
  if (reader_ == nullptr || !reader_->IsOpen()) {
    return Result::NotSupported("ParseEntry: Reader not usable.");
  }
  if (key_out == nullptr || value_out == nullptr || next_entry_start_offset_out == nullptr) {
    return Result::InvalidArgument("ParseEntry: Output parameters cannot be null.");
  }

  // Assuming SSTableReader has a public getter: const std::vector<char>& GetBlockBuffer() const;
  const std::vector<char>& block_buffer = reader_->GetBlockBuffer();
  if (block_buffer.empty() && parse_from_offset == 0) {
      return Result::NotFound("End of block data (empty block).");
  }

  const char* block_data_start = block_buffer.data();
  const size_t current_block_uncompressed_size = block_buffer.size();
  size_t current_offset_in_block = parse_from_offset;

  if (current_offset_in_block >= current_block_uncompressed_size) {
    return Result::NotFound("End of block data reached for parsing.");
  }
   printf("DEBUG PARSE_ENTRY: Called with parse_from_offset = %zu. Block size = %zu.\n",
          parse_from_offset, current_block_uncompressed_size);
   printf("DEBUG PARSE_ENTRY: First 16 bytes of block_buffer from offset %zu (hex): ", parse_from_offset);
   for(size_t i = 0; i < 16 && (parse_from_offset + i) < current_block_uncompressed_size; ++i) {
       printf("%02x ", static_cast<unsigned char>(block_buffer[parse_from_offset + i]));
   }
   printf("\n");

  uint32_t key_length;
  if (current_offset_in_block + sizeof(uint32_t) > current_block_uncompressed_size) {
    return Result::Corruption("ParseEntry: Cannot read key length.");
  }
  key_length = ReadLittleEndian32(block_data_start + current_offset_in_block);
  printf("DEBUG PARSE_ENTRY: Read key_length = %u at original offset %zu.\n", key_length, current_offset_in_block);
  current_offset_in_block += sizeof(uint32_t);

  if (current_offset_in_block + key_length > current_block_uncompressed_size) {
    return Result::Corruption("ParseEntry: Key data extends beyond block boundary.");
  }
  const std::byte* key_data_ptr =
      reinterpret_cast<const std::byte*>(block_data_start + current_offset_in_block);
  *key_out = Slice(key_data_ptr, key_length);
  current_offset_in_block += key_length;

  char tag_byte;
  if (current_offset_in_block + sizeof(char) > current_block_uncompressed_size) {
    printf("DEBUG PARSE_ENTRY: Corruption - cannot read value tag.\n");
    return Result::Corruption("ParseEntry: Cannot read value tag.");
  }
  tag_byte = *(block_data_start + current_offset_in_block);
  ValueTag current_tag = static_cast<ValueTag>(static_cast<unsigned char>(tag_byte));
  printf("DEBUG PARSE_ENTRY: Read tag = %d at original offset %zu.\n", (int)current_tag, current_offset_in_block);
  current_offset_in_block += sizeof(char);

  uint32_t value_length;
  if (current_offset_in_block + sizeof(uint32_t) > current_block_uncompressed_size) {
    return Result::Corruption("ParseEntry: Cannot read value length.");
  }
  value_length = ReadLittleEndian32(block_data_start + current_offset_in_block);
  printf("DEBUG PARSE_ENTRY: Read value_length = %u at original offset %zu.\n", value_length, current_offset_in_block);
  current_offset_in_block += sizeof(uint32_t);

  Slice temp_parsed_value_slice;
  if (value_length > 0) {
    if (current_offset_in_block + value_length > current_block_uncompressed_size) {
      return Result::Corruption("ParseEntry: Value data extends beyond block boundary.");
    }
    const std::byte* value_data_ptr_for_slice =
        reinterpret_cast<const std::byte*>(block_data_start + current_offset_in_block);
    temp_parsed_value_slice = Slice(value_data_ptr_for_slice, value_length);
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
    *value_out = ValueEntry(ValueTag::kTombstone);
  } else {
    return Result::Corruption("ParseEntry: Unknown value tag encountered.");
  }

  printf("<<< DEBUG PARSE_ENTRY: ValueEntry constructed / assigned to value_out >>>\n");
  printf("    value_out (ValEnt): type=%d, slice_ptr=%p, slice_size=%zu, slice_str='%.*s'\n",
         (int)value_out->type,
         (void*)value_out->value_slice.data(), value_out->value_slice.size(),
         (int)value_out->value_slice.size(),
         value_out->value_slice.data() ? (const char*)value_out->value_slice.data() : "");

  current_offset_in_block += value_length;
  *next_entry_start_offset_out = current_offset_in_block;

  return Result::OK();
}

void SSTableIterator::LoadBlockAndReposition(uint64_t block_start_file_offset) {
  printf("DEBUG ITER::LoadBlockAndReposition called with offset %lu\n", (unsigned long)block_start_file_offset);
  valid_ = false;
  if (!reader_ || !reader_->IsOpen()) {
    if (status_.ok()) {
        status_ = Result::NotSupported("Iterator's reader not usable or iterator in error state.");
    }
    printf("DEBUG ITER::LoadBlockAndReposition returning early - reader not usable.\n");
    return;
  }
   if (!status_.ok() && status_.code() != ResultCode::kNotFound) {
    printf("DEBUG ITER::LoadBlockAndReposition returning early - iterator in error state %d.\n", (int)status_.code());
    return;
   }


  current_block_start_offset_ = block_start_file_offset;
  current_block_total_size_ = 0; // Reset before call
  current_entry_offset_in_block_ = 0;

  if (current_block_start_offset_ >= reader_->FileSize()) {
    status_ = Result::OK();
    valid_ = false;
    printf("DEBUG ITER::LoadBlockAndReposition - At or past EOF (offset %lu >= filesize %lu).\n",
           (unsigned long)current_block_start_offset_, (unsigned long)reader_->FileSize());
    return;
  }

  // Assuming LoadBlockIntoBuffer updates the size via a pointer/reference.
  // If LoadBlockIntoBuffer's signature is Result LoadBlockIntoBuffer(uint64_t offset, uint64_t* size_out);
  status_ = reader_->LoadBlockIntoBuffer(current_block_start_offset_, &current_block_total_size_);
  if (!status_.ok()) {
    if (status_.code() == ResultCode::kNotFound) {
        status_ = Result::OK();
        printf("DEBUG ITER::LoadBlockAndReposition - LoadBlockIntoBuffer returned NotFound (EOF).\n");
    } else {
        printf("DEBUG ITER::LoadBlockAndReposition - LoadBlockIntoBuffer failed with code %d: %s\n",
               (int)status_.code(), status_.message().c_str());
    }
    valid_ = false;
    return;
  }
   printf("DEBUG ITER::LoadBlockAndReposition - Successfully loaded block. Total disk size: %lu\n",
          (unsigned long)current_block_total_size_);
   printf("DEBUG ITER::LoadBlockAndReposition - Loaded block buffer content (first 32 bytes hex):\n");
   // Assuming SSTableReader has a public getter: const std::vector<char>& GetBlockBuffer() const;
   for (size_t i = 0; i < std::min(reader_->GetBlockBuffer().size(), (size_t)32); ++i) {
       printf("%02x ", static_cast<unsigned char>(reader_->GetBlockBuffer()[i]));
   }
   printf("\n");


  // Assuming SSTableReader has a public getter: const std::vector<char>& GetBlockBuffer() const;
  if (reader_->GetBlockBuffer().empty()) {
      printf("DEBUG ITER::LoadBlockAndReposition - Loaded block buffer is empty.\n");
      valid_ = false;
      if (current_block_total_size_ > 0) { // If block had size but was empty (e.g. all padding/corrupt)
        LoadBlockAndReposition(current_block_start_offset_ + current_block_total_size_);
      } else { // True end of useful data or zero size block at end
        status_ = Result::OK();
        printf("DEBUG ITER::LoadBlockAndReposition - Empty block and zero total size, considered EOF.\n");
      }
      return;
  }

  size_t next_entry_parse_offset_after_first;
  Result parse_res = ParseEntryFromBuffer(
      0, /* parse from start of newly loaded buffer */
      &current_key_, &current_value_,
      &next_entry_parse_offset_after_first);

  if (parse_res.ok()) {
    valid_ = true;
    status_ = Result::OK();
    current_entry_offset_in_block_ = next_entry_parse_offset_after_first;
    printf("DEBUG ITER::LoadBlockAndReposition - First entry parsed successfully. Next parse offset: %zu\n", current_entry_offset_in_block_);

  } else if (parse_res.code() == ResultCode::kNotFound) {
    printf("DEBUG ITER::LoadBlockAndReposition - ParseEntryFromBuffer found no entries in non-empty block.\n");
    valid_ = false;
    status_ = Result::OK();
    if (current_block_total_size_ > 0) {
        LoadBlockAndReposition(current_block_start_offset_ + current_block_total_size_);
    } else {
        status_ = Result::Corruption("Zero-sized block despite non-empty buffer during iteration.");
         printf("DEBUG ITER::LoadBlockAndReposition - Zero-sized block error.\n");
    }
  } else {
    status_ = parse_res;
    valid_ = false;
     printf("DEBUG ITER::LoadBlockAndReposition - ParseEntryFromBuffer failed: %s\n", status_.message().c_str());
  }
}

void SSTableIterator::SeekToFirst() {
  printf("DEBUG ITER::SeekToFirst called.\n");
  status_ = Result::OK();
  valid_ = false;
  if (!reader_ || !reader_->IsOpen()) {
    status_ = Result::NotSupported(
        "Iterator's reader is not usable for SeekToFirst.");
    printf("DEBUG ITER::SeekToFirst - Reader not usable.\n");
    return;
  }
  LoadBlockAndReposition(0);
}

void SSTableIterator::Next() {
  printf("DEBUG ITER::Next called. Valid was: %d, Status was: %d\n", valid_, (int)status_.code());
  if (!Valid()) {
    printf("DEBUG ITER::Next - Called when invalid or in error state. No-op.\n");
    return;
  }

  size_t next_entry_parse_offset_after_this;
  Result parse_res = ParseEntryFromBuffer(
      current_entry_offset_in_block_,
      &current_key_, &current_value_,
      &next_entry_parse_offset_after_this
  );

  if (parse_res.ok()) {
    valid_ = true;
    status_ = Result::OK();
    current_entry_offset_in_block_ = next_entry_parse_offset_after_this;
    printf("DEBUG ITER::Next - Successfully parsed next entry. Next parse offset: %zu\n", current_entry_offset_in_block_);
    return;
  }

  if (parse_res.code() == ResultCode::kNotFound) {
    printf("DEBUG ITER::Next - End of current block. Attempting to load next block.\n");
    status_ = Result::OK();
    valid_ = false;

    if (current_block_total_size_ == 0) {
        if (current_block_start_offset_ < reader_->FileSize()) {
             status_ = Result::Corruption("Encountered zero-sized block during Next operation.");
             printf("DEBUG ITER::Next - Zero-sized block error.\n");
        } else {
            printf("DEBUG ITER::Next - Zero-sized block at EOF, iterator ends.\n");
        }
        return;
    }
    if (current_block_start_offset_ + current_block_total_size_ <= current_block_start_offset_ && current_block_total_size_ > 0) {
        status_ = Result::Corruption("Block size did not advance offset, potential infinite loop.");
        printf("DEBUG ITER::Next - Block size did not advance offset (start: %lu, total: %lu).\n",
               (unsigned long)current_block_start_offset_, (unsigned long)current_block_total_size_);
        valid_ = false;
        return;
    }
    LoadBlockAndReposition(current_block_start_offset_ + current_block_total_size_);
  } else {
    status_ = parse_res;
    valid_ = false;
    printf("DEBUG ITER::Next - ParseEntryFromBuffer failed: %s\n", status_.message().c_str());
  }
}

void SSTableIterator::Seek(const Slice& target) {
  printf("DEBUG ITER::Seek called for target '%.*s'.\n", (int)target.size(), target.data() ? (const char*)target.data() : "");
  status_ = Result::OK();
  valid_ = false;
  if (!reader_ || !reader_->IsOpen()) {
    status_ = Result::NotSupported("Iterator's reader not usable for Seek.");
    printf("DEBUG ITER::Seek - Reader not usable.\n");
    return;
  }

  SeekToFirst();
  if (!Valid()) {
    printf("DEBUG ITER::Seek - SeekToFirst failed or SSTable is empty. Current status: %d\n", (int)status_.code());
    return;
  }

  while (Valid()) {
    printf("DEBUG ITER::Seek - Comparing current_key '%.*s' with target '%.*s'.\n",
           (int)current_key_.size(), current_key_.data() ? (const char*)current_key_.data() : "",
           (int)target.size(), target.data() ? (const char*)target.data() : "");
    if (current_key_.compare(target) >= 0) {
      printf("DEBUG ITER::Seek - Found key >= target.\n");
      return;
    }
    Next();
  }
  printf("DEBUG ITER::Seek - Target not found or error in Next. Iterator invalid. Status: %d\n", (int)status_.code());
}