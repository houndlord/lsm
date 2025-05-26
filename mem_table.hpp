#ifndef MEM_TABLE_HPP
#define MEM_TABLE_HPP


#include "result.hpp"
#include "slice.hpp"
#include "skip_list.hpp"  
#include "arena.hpp"
#include "sorted_table.hpp"
#include "result.hpp"
#include <memory>

struct MemTable {
 public:
  MemTable(Arena& arena);
  ~MemTable();

  Result Put(const Slice& key, const Slice& value);
  Result Get(const Slice& key) const;
  Result Delete(const Slice& key);
  SortedTableIterator* NewIterator() const;

  size_t ApproximateMemoryUsage() const;
  
 private:
  Arena& arena_ref_;
  std::unique_ptr<SortedTable> table_;
};

#endif // MEM_TABLE_HPP