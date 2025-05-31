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

  SSTableIterator(const SSTableIterator&) = delete;
  SSTableIterator& operator=(const SSTableIterator&) = delete;
  SSTableIterator(SSTableIterator&&) = delete;
  SSTableIterator& operator=(SSTableIterator&&) = delete;

  bool Valid() const override;
  void SeekToFirst() override;
  void Seek(const Slice& target) override;
  void Next() override;
  Slice key() const override;
  ValueEntry value() const override;
  Result status() const override;

 private:
  Result ParseEntryFromBuffer(size_t parse_from_offset, Slice* key_out,
                                ValueEntry* value_out,
                                size_t* next_entry_start_offset_out);

  void LoadBlockAndReposition(uint64_t block_start_file_offset);

  SSTableReader* reader_; 
  
  uint64_t current_block_start_offset_; 
  uint64_t current_block_total_size_;   

  size_t current_entry_offset_in_block_; // Offset *within the reader's current block buffer* for the start of the CURRENT entry
  Slice current_key_;        
  ValueEntry current_value_; 
  
  bool valid_;
  Result status_;
};

#endif // SSTABLE_ITERATOR_HPP