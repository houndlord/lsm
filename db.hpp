#include "headers"
#include "mem_table.hpp"
#include "result.hpp"
#include <filesystem>
#include <memory>
#include <string>
#include <system_error>

struct DB {
  DB(std::string db_directory, std::size_t threshold) : db_dir_(db_directory), threshold_(threshold) {};

  Result Init();



 private:
  std::unique_ptr<MemTable> active_memtable_;
  std::unique_ptr<MemTable> immutable_memtable_;
  std::vector<std::string> l0__filenames_;
  size_t threshold_;
  std::unique_ptr<Arena> memtable_arena_;
  std::string db_dir_;
  uint64_t next__sstable_id_;
};