#ifndef DB_HPP
#define DB_HPP

#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <iostream> // For std::cout in debug prints

#include "arena.hpp"
#include "mem_table.hpp"
#include "result.hpp"
#include "slice.hpp"

// Forward declaration for SSTableReader to be used as an opaque pointer in GetInternal if needed
// Though current GetInternal creates it locally.
// struct SSTableReader;

struct DB {
  DB(std::string db_directory, std::size_t threshold);
  ~DB() = default;

  DB(const DB&) = delete;
  DB& operator=(const DB&) = delete;
  DB(DB&&) = delete;
  DB& operator=(DB&&) = delete;

  Result Init();

  Result Put(const Slice& key, const Slice& value);

  Result Get(const Slice& key, std::string* value_out);

  // Get method that copies the value into the provided Arena,
  // and the Result object contains the Slice pointing to it.
  //Result Get(const Slice& key, Arena& result_arena);

  Result Delete(const Slice& key);

 private:
  Result FlushMemTable();
  std::string GenerateSSTableFilename();

  // Helper for Get logic to avoid code duplication.
  struct GetInternalResult {
    Result status;
    Slice data_slice;       // Valid if status.ok() (and not a tombstone situation)
    bool is_tombstone = false; // True if a tombstone was the definitive entry

    GetInternalResult() : status(Result::OK()) {} // Default to OK initially

    static GetInternalResult ValueFound(Slice s) {
      GetInternalResult res;
      res.status = Result::OK(); // Explicitly OK
      res.data_slice = s;
      res.is_tombstone = false;
      return res;
    }
    static GetInternalResult TombstoneFound() {
      GetInternalResult res;
      // For the caller (public Get), a tombstone means the key is "not found"
      res.status = Result::NotFound("Key is a tombstone");
      res.is_tombstone = true;
      return res;
    }
    static GetInternalResult TrulyNotFound() {
      GetInternalResult res;
      res.status = Result::NotFound("Key not found in DB");
      res.is_tombstone = false; // Not a tombstone, just absent
      return res;
    }
    static GetInternalResult Error(Result r) {
        GetInternalResult res;
        res.status = r; // Propagate error status
        res.is_tombstone = false; // Not relevant for errors
        return res;
    }
  };
  // Arena* parameter is for the target arena if data is found in an SSTable
  // and needs to be copied into a user-provided arena.
  GetInternalResult GetInternal(const Slice& key, Arena* sstable_target_arena_for_copy);


  std::unique_ptr<Arena> active_memtable_arena_;
  std::unique_ptr<MemTable> active_memtable_;

  std::unique_ptr<Arena> immutable_memtable_arena_;
  std::unique_ptr<MemTable> immutable_memtable_;

  std::vector<std::string> l0_sstables_;
  size_t threshold_;
  std::string db_dir_;
  uint64_t next_sstable_id_;

  // TODO (Performance): Consider adding an SSTableReader cache (e.g., LRUCache)
  // to avoid re-opening and re-initializing readers for frequently accessed SSTables,
  // especially if L0 can grow large or for higher levels.
  // For now, GetInternal creates readers on-the-fly.
};
#endif // DB_HPP