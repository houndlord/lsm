#include "db.hpp"
#include "headers"
#include "result.hpp"
#include <filesystem>
#include <memory>
#include <system_error>


Result DB::Init() {
  std::error_code ec;

  bool path_exists = std::filesystem::exists(std::filesystem::path(db_dir_), ec);
  if (ec) {
    return Result::IOError("Filesystem error checking existence of directory '" + db_dir_ + "': " + ec.message());
  }
  if (path_exists) {
    if (!std::filesystem::is_directory(db_dir_, ec)) {
      if (ec) { 
          return Result::IOError("Filesystem error checking if path '" + db_dir_ + "' is a directory: " + ec.message());
      } 
    } else {
      if (!std::filesystem::create_directories(db_dir_, ec)) {
        if (ec) {
          return Result::IOError("Failed to create directory '" + db_dir_ + "': " + ec.message());
        } else {
          return Result::IOError("Failed to create directory '" + db_dir_ + "' (unknown reason).");
        }
      }
    }
  }
  memtable_arena_ = std::make_unique<Arena>();
  active_memtable_ = std::make_unique<MemTable>(memtable_arena_);
}

Result DB::Put(const Slice& key, const Slice& value) {
  return active_memtable_->Put(key, value);
}