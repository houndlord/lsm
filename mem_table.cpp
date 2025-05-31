#include "mem_table.hpp"
#include "result.hpp"
#include "slice.hpp"
#include "sorted_table.hpp"
#include <iostream>

MemTable::MemTable(Arena& arena) : arena_ref_(arena) {
  std::cout << "[MemTable Constructor] Called. Arena ref: " << &arena_ref_ << std::endl; // DEBUG
  table_ = std::make_unique<SkipList>(arena_ref_);
  if (!table_) {
        std::cout << "[MemTable Constructor] FAILED to create SkipList (table_)." << std::endl; // DEBUG
        // This would be a problem, but std::make_unique throws on failure, make_unique_nothrow would return nullptr
        // If using make_unique_nothrow, you'd need to handle the nullptr case.
        // For std::make_unique, if this fails, an exception is thrown.
    } else {
        std::cout << "[MemTable Constructor] SkipList (table_) created. Ptr: " << table_.get() << std::endl; // DEBUG
    }
}

MemTable::~MemTable() {};

Result MemTable::Put(const Slice& key, const Slice& value) {
  Result result = table_->Put(key, value);
  return result;
}

Result MemTable::Get(const Slice& key) const {
  return table_->Get(key);
}

Result MemTable::Delete(const Slice& key) {
  return table_->Delete(key);
}

SortedTableIterator* MemTable::NewIterator() const {
  return table_->NewIterator();
}

size_t MemTable::ApproximateMemoryUsage() const {
  return table_->ApproximateMemoryUsage();
}

