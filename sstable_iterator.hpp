#ifndef SSTABLE_ITERATOR_HPP
#define SSTABLE_ITERATOR_HPP

#include "result.hpp"
#include "sorted_table.hpp"
#include "sstable_reader.hpp"
#include "slice.hpp"
#include "value.hpp"


class SSTableIterator : public SortedTableIterator {
 public:
  explicit SSTableIterator(SSTableReader* reader);
  ~SSTableIterator() override = default;

  // SortedTableIterator interface
  bool Valid() const override;
  void SeekToFirst() override;
  void Seek(const Slice& target) override;
  void Next() override;
  Slice key() const override;
  ValueEntry value() const override;
  Result status() const override;

 private:
  // This private method will parse from the reader's internal buffer
  Result ParseEntryFromBuffer(size_t parse_from_offset, Slice* key_out,
                                ValueEntry* value_out,
                                size_t* next_entry_start_offset_out);

  // Helper to load the next available block (via reader_)
  // and then try to parse its first entry using ParseEntryFromBuffer.
  // Sets valid_ and status_ internally.
  void LoadBlockAndReposition(uint64_t block_start_file_offset);

  SSTableReader* reader_; // Non-owning
  
  // State related to the current block AS KNOWN BY THE ITERATOR
  uint64_t current_block_start_offset_; // File offset of the start of the block currently in reader's buffer
  uint64_t current_block_total_size_;   // Total size on disk (header + data) of the loaded block

  // State related to the current entry being pointed to
  size_t next_entry_offset_; // Offset *within the reader's current block buffer* for the start of the NEXT entry
  Slice current_key_;        // Points into reader_->internal_block_buffer_
  ValueEntry current_value_; // Its value_slice points into reader_->internal_block_buffer_
  
  bool valid_ = false;
  Result status_ = Result::OK();
};

#endif // SSTABLE_ITERATOR_HPP