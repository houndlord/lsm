#include "mem_table.hpp"
#include "result.hpp"
#include "slice.hpp"
#include "sorted_table.hpp"

MemTable::MemTable(Arena& arena) : arena_ref_(arena) {
  table_ = std::make_unique<SkipList>(arena_ref_);
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

