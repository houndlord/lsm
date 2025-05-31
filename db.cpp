#include "db.hpp"

#include <algorithm>    // For std::reverse if needed for L0 iteration (not currently used)
#include <filesystem>   // For directory operations
#include <iomanip>      // For std::setw, std::setfill
#include <iostream>     // For std::cout debug prints
#include <sstream>      // For std::ostringstream
#include <cstring>      // For std::memcpy

#include "make_unique_nothrow.hpp"
#include "sstable_reader.hpp"
#include "sstable_writer.hpp"

DB::DB(std::string db_directory, std::size_t threshold)
    : db_dir_(std::move(db_directory)),
      threshold_(threshold),
      next_sstable_id_(1), // Start SSTable IDs from 1
      active_memtable_arena_(nullptr),
      active_memtable_(nullptr),
      immutable_memtable_arena_(nullptr),
      immutable_memtable_(nullptr) {
  std::cout << "[DB Constructor] Called. Dir: " << db_dir_ << ", Threshold: " << threshold_ << std::endl;
}

std::string DB::GenerateSSTableFilename() {
  std::ostringstream filename_stream;
  // Format: 000001.sst, 000002.sst etc.
  filename_stream << std::setw(6) << std::setfill('0') << next_sstable_id_
                  << ".sst";
  return filename_stream.str();
}

Result DB::Init() {
  std::cout << "[DB::Init] Called." << std::endl;
  std::error_code ec;
  std::filesystem::path db_path = db_dir_;

  if (std::filesystem::exists(db_path, ec)) {
    if (ec) {
      std::cout << "[DB::Init] Filesystem error checking existence of directory '" << db_dir_ << "': " << ec.message() << std::endl;
      return Result::IOError("Filesystem error checking existence of directory '" + db_dir_ + "': " + ec.message());
    }
    if (!std::filesystem::is_directory(db_path, ec)) {
      if (ec) {
          std::cout << "[DB::Init] Filesystem error checking if path '" << db_dir_ << "' is a directory: " << ec.message() << std::endl;
          return Result::IOError("Filesystem error checking if path '" + db_dir_ + "' is a directory: " + ec.message());
      }
      std::cout << "[DB::Init] Path '" << db_dir_ << "' exists but is not a directory." << std::endl;
      return Result::IOError("Path '" + db_dir_ + "' exists but is not a directory.");
    }
    std::cout << "[DB::Init] Directory '" << db_dir_ << "' already exists." << std::endl;
  } else {
    std::cout << "[DB::Init] Directory '" << db_dir_ << "' does not exist. Creating." << std::endl;
    if (!std::filesystem::create_directories(db_path, ec) || ec) {
      std::cout << "[DB::Init] Failed to create directory " << db_dir_ << ". Error: " << (ec ? ec.message() : "create_directories returned false") << std::endl;
      return Result::IOError("Failed to create directory '" + db_dir_ + "': " + (ec ? ec.message() : "create_directories returned false"));
    }
    std::cout << "[DB::Init] Directory '" << db_dir_ << "' created." << std::endl;
  }

  std::cout << "[DB::Init] Directory OK. Creating active_memtable_arena_." << std::endl;
  active_memtable_arena_ = make_unique_nothrow<Arena>();
  if (!active_memtable_arena_) {
    std::cout << "[DB::Init] Failed to allocate Arena object for active MemTable." << std::endl;
    return Result::ArenaAllocationFail("Failed to allocate Arena object for active MemTable in Init.");
  }
  std::cout << "[DB::Init] active_memtable_arena_ CREATED. Ptr: " << active_memtable_arena_.get() << std::endl;
  
  std::cout << "[DB::Init] Creating active_memtable_." << std::endl;
  active_memtable_ = make_unique_nothrow<MemTable>(*active_memtable_arena_);
  if (!active_memtable_) {
    std::cout << "[DB::Init] Failed to allocate MemTable object for active MemTable." << std::endl;
    active_memtable_arena_.reset(); 
    return Result::ArenaAllocationFail("Failed to allocate MemTable object for active MemTable in Init.");
  }
  std::cout << "[DB::Init] active_memtable_ CREATED. Ptr: " << active_memtable_.get() << std::endl;

  // TODO(Persistence): In a real DB, scan db_dir_ for existing *.sst files here.
  // Populate l0_sstables_ (sorted newest to oldest, or sort after scan).
  // Update next_sstable_id_ to be max_existing_id + 1.

  std::cout << "[DB::Init] Returning OK." << std::endl;
  return Result::OK();
}

Result DB::FlushMemTable() {
  std::cout << "[DB::FlushMemTable] Called."
            << (immutable_memtable_ ? " immutable_memtable_ EXISTS!" : " immutable_memtable_ is null.")
            << std::endl;
  if (!active_memtable_) {
    std::cout << "[DB::FlushMemTable] No active memtable exists. Nothing to flush." << std::endl;
    return Result::IOError("FlushMemTable called but no active memtable exists.");
  }
   // Check if active memtable is empty (after it's confirmed to exist)
  if (active_memtable_->ApproximateMemoryUsage() == 0) {
    std::cout << "[DB::FlushMemTable] Active memtable is empty. While a new active memtable will be created, no SSTable will be written for the current one." << std::endl;
    // No data to write, but we still cycle to a new active memtable.
    // If we don't want to cycle for an empty memtable, this logic would change.
    // For now, proceed with cycling.
  }

  if (immutable_memtable_) {
    std::cout << "[DB::FlushMemTable] Error: An immutable memtable already exists." << std::endl;
    return Result::IOError("FlushMemTable: An immutable memtable already exists; cannot flush concurrently (in synchronous mode).");
  }
  std::cout << "[DB::FlushMemTable] Moving active to immutable." << std::endl;
  immutable_memtable_arena_ = std::move(active_memtable_arena_);
  immutable_memtable_ = std::move(active_memtable_);

  std::cout << "[DB::FlushMemTable] Creating new active_memtable_arena_." << std::endl;
  active_memtable_arena_ = make_unique_nothrow<Arena>();
  if (!active_memtable_arena_) {
    std::cout << "[DB::FlushMemTable] Failed to allocate Arena for new active MemTable. Restoring state." << std::endl;
    active_memtable_ = std::move(immutable_memtable_);
    active_memtable_arena_ = std::move(immutable_memtable_arena_);
    return Result::ArenaAllocationFail("Failed to allocate Arena for new active MemTable during flush.");
  }
  std::cout << "[DB::FlushMemTable] New active_memtable_arena_ CREATED. Ptr: " << active_memtable_arena_.get() << std::endl;

  std::cout << "[DB::FlushMemTable] Creating new active_memtable_." << std::endl;
  active_memtable_ = make_unique_nothrow<MemTable>(*active_memtable_arena_);
  if (!active_memtable_) {
    std::cout << "[DB::FlushMemTable] Failed to allocate new active MemTable. Restoring state." << std::endl;
    active_memtable_arena_.reset();
    active_memtable_ = std::move(immutable_memtable_);
    active_memtable_arena_ = std::move(immutable_memtable_arena_);
    return Result::ArenaAllocationFail("Failed to allocate new active MemTable during flush.");
  }
  std::cout << "[DB::FlushMemTable] New active_memtable_ CREATED. Ptr: " << active_memtable_.get() << std::endl;

  // Only write SSTable if immutable memtable has data
  if (immutable_memtable_ && immutable_memtable_->ApproximateMemoryUsage() > 0) {
    std::string sstable_basename = GenerateSSTableFilename();
    std::cout << "[DB::FlushMemTable] Generating SSTable filename: " << sstable_basename << std::endl;
    std::filesystem::path sstable_path = std::filesystem::path(db_dir_) / sstable_basename;

    SSTableWriter writer(true /* compression_enabled */);
    Result writer_init_res = writer.Init();
    std::cout << "[DB::FlushMemTable] SSTableWriter.Init() result. ok(): " << (writer_init_res.ok() ? "true" : "false") << ", code(): " << static_cast<int>(writer_init_res.code()) << ", message(): '" << writer_init_res.message() << "'" << std::endl;
    if (!writer_init_res.ok()) {
      std::cout << "[DB::FlushMemTable] SSTableWriter::Init failed. Restoring state." << std::endl;
      active_memtable_arena_.reset();
      active_memtable_.reset();
      active_memtable_ = std::move(immutable_memtable_);
      active_memtable_arena_ = std::move(immutable_memtable_arena_);
      return Result::IOError("SSTableWriter Init failed during flush: " + writer_init_res.message());
    }

    Result write_result = writer.WriteMemTableToFile(*immutable_memtable_, sstable_path.string());
    std::cout << "[DB::FlushMemTable] writer.WriteMemTableToFile result. ok(): " << (write_result.ok() ? "true" : "false") << ", code(): " << static_cast<int>(write_result.code()) << ", message(): '" << write_result.message() << "'" << std::endl;

    if (!write_result.ok()) {
      std::cout << "[DB::FlushMemTable] Failed to write SSTable file. Resetting immutable memtable (data loss for this flush)." << std::endl;
      // Note: The new active memtable is already in place. The flushed data is lost.
      // For robustness, one might retry or keep immutable_memtable_ for recovery.
      immutable_memtable_.reset();
      immutable_memtable_arena_.reset();
      return Result::IOError("Failed to write SSTable file: " + sstable_path.string() + " - " + write_result.message());
    }

    std::cout << "[DB::FlushMemTable] SSTable write successful. Path: " << sstable_path.string() << std::endl;
    l0_sstables_.insert(l0_sstables_.begin(), sstable_path.string()); // Newest first
    next_sstable_id_++;
    std::cout << "[DB::FlushMemTable] Adding to L0: " << sstable_path.string() << ". Next ID: " << next_sstable_id_ << std::endl;
  } else {
      std::cout << "[DB::FlushMemTable] Immutable memtable is null or empty, skipping SSTable write." << std::endl;
  }

  std::cout << "[DB::FlushMemTable] Resetting (any remaining) immutable memtable." << std::endl;
  immutable_memtable_.reset();
  immutable_memtable_arena_.reset();

  Result final_ok_res = Result::OK();
  std::cout << "[DB::FlushMemTable] Returning OK. ok(): " << (final_ok_res.ok() ? "true" : "false") << ", code(): " << static_cast<int>(final_ok_res.code()) << ", message(): '" << final_ok_res.message() << "'" << std::endl;
  return final_ok_res;
}

Result DB::Put(const Slice& key, const Slice& value) {
  std::cout << "[DB::Put] ENTER. Key: " << key.ToString() << std::endl;
  if (!active_memtable_) {
    Result err_res = Result::IOError("Active memtable not available; DB may not be initialized or in error state.");
    std::cout << "[DB::Put] EXIT - Error: active_memtable_ is null!. ok(): " << (err_res.ok() ? "true" : "false") << ", code(): " << static_cast<int>(err_res.code()) << ", message(): '" << err_res.message() << "'" << std::endl;
    return err_res;
  }

  Result put_res = active_memtable_->Put(key, value);
  std::cout << "[DB::Put] active_memtable_->Put result. ok(): " << (put_res.ok() ? "true" : "false") << ", code(): " << static_cast<int>(put_res.code()) << ", message(): '" << put_res.message() << "'" << std::endl;
  if (!put_res.ok()) {
    std::cout << "[DB::Put] EXIT - Error from active_memtable_->Put." << std::endl;
    return put_res;
  }

  size_t current_usage = active_memtable_->ApproximateMemoryUsage();
  std::cout << "[DB::Put] Memtable usage: " << current_usage << ", Threshold: " << threshold_ << std::endl;
  if (current_usage >= threshold_) {
    std::cout << "[DB::Put] Threshold met. Calling FlushMemTable." << std::endl;
    Result flush_res = FlushMemTable();
    std::cout << "[DB::Put] FlushMemTable result. ok(): " << (flush_res.ok() ? "true" : "false") << ", code(): " << static_cast<int>(flush_res.code()) << ", message(): '" << flush_res.message() << "'" << std::endl;
    if (!flush_res.ok()) {
      std::cout << "[DB::Put] EXIT - Error from FlushMemTable." << std::endl;
      return flush_res;
    }
  }
  Result final_ok_res = Result::OK();
  std::cout << "[DB::Put] EXIT - Returning OK. ok(): " << (final_ok_res.ok() ? "true" : "false") << ", code(): " << static_cast<int>(final_ok_res.code()) << ", message(): '" << final_ok_res.message() << "'" << std::endl;
  return final_ok_res;
}

Result DB::Delete(const Slice& key) {
  std::cout << "[DB::Delete] ENTER. Key: " << key.ToString() << std::endl;
  if (!active_memtable_) {
    Result err_res = Result::IOError("Active memtable not available; DB may not be initialized or in error state.");
    std::cout << "[DB::Delete] EXIT - Error: active_memtable_ is null!. ok(): " << (err_res.ok() ? "true" : "false") << ", code(): " << static_cast<int>(err_res.code()) << ", message(): '" << err_res.message() << "'" << std::endl;
    return err_res;
  }
  Result del_res = active_memtable_->Delete(key); // This should use the new MemTable::Delete that returns a rich Result
   std::cout << "[DB::Delete] active_memtable_->Delete result. ok(): " << (del_res.ok() ? "true" : "false") << ", code(): " << static_cast<int>(del_res.code()) << ", message(): '" << del_res.message() << "'" << std::endl;
  if (!del_res.ok()) {
    // If MemTable::Delete itself fails (e.g., arena allocation for tombstone key), propagate.
    return del_res;
  }

  size_t current_usage = active_memtable_->ApproximateMemoryUsage();
  std::cout << "[DB::Delete] Memtable usage: " << current_usage << ", Threshold: " << threshold_ << std::endl;
  if (current_usage >= threshold_) {
    std::cout << "[DB::Delete] Threshold met. Calling FlushMemTable." << std::endl;
    Result flush_res = FlushMemTable();
    std::cout << "[DB::Delete] FlushMemTable result. ok(): " << (flush_res.ok() ? "true" : "false") << ", code(): " << static_cast<int>(flush_res.code()) << ", message(): '" << flush_res.message() << "'" << std::endl;
    if (!flush_res.ok()) {
      return flush_res;
    }
  }
  Result final_ok_res = Result::OK(); // Delete operation itself is OK, even if it's for a non-existent key
  std::cout << "[DB::Delete] EXIT - Returning OK. ok(): " << (final_ok_res.ok() ? "true" : "false") << ", code(): " << static_cast<int>(final_ok_res.code()) << ", message(): '" << final_ok_res.message() << "'" << std::endl;
  return final_ok_res;
}

// Internal helper to find a key across all storage layers.
DB::GetInternalResult DB::GetInternal(const Slice& key, Arena* sstable_target_arena_for_copy) {
  std::cout << "[DB::GetInternal] ENTER for key: " << key.ToString()
            << (sstable_target_arena_for_copy ? " (SSTable target arena provided)" : " (No SSTable target arena)")
            << std::endl;

  // 1. Check active MemTable
  if (active_memtable_) {
    Result res = active_memtable_->Get(key); // Assumes MemTable::Get returns Result with value_tag_
    std::cout << "[DB::GetInternal] Active memtable Get. ok(): " << res.ok()
              << ", code: " << static_cast<int>(res.code())
              << ", msg: " << res.message()
              << ", has_value_slice: " << res.value_slice().has_value()
              << ", value_tag: " << (res.value_tag().has_value() ? static_cast<int>(res.value_tag().value()) : -1)
              << std::endl;
    if (res.ok()) {
      if (res.value_tag().has_value() && res.value_tag().value() == ValueTag::kTombstone) {
        std::cout << "[DB::GetInternal] Found TOMBSTONE in active memtable." << std::endl;
        return GetInternalResult::TombstoneFound();
      } else if (res.value_slice().has_value() && res.value_tag().has_value() && res.value_tag().value() == ValueTag::kData) {
        std::cout << "[DB::GetInternal] Found DATA in active memtable. Slice points to memtable's arena." << std::endl;
        return GetInternalResult::ValueFound(res.value_slice().value());
      } else {
        // This state (OK but no valid data/tombstone tag) should ideally not be reached if MemTable::Get is robust.
        std::cout << "[DB::GetInternal] Active MemTable::Get OK but inconsistent state (e.g. no value_slice for data, or no tag)." << std::endl;
        return GetInternalResult::Error(Result::Corruption("Active MemTable::Get returned OK with inconsistent state"));
      }
    } else if (res.code() == ResultCode::kNotFound) {
      // kNotFound from MemTable means key is TRULY not in this memtable's map. Continue search.
      std::cout << "[DB::GetInternal] kNotFound (truly not in map) from active memtable. Continuing search..." << std::endl;
      // Fall through to check immutable memtable
    } else { // Other error from MemTable::Get (e.g. corruption within MemTable)
        std::cout << "[DB::GetInternal] Error from active memtable Get: " << res.message() << std::endl;
        return GetInternalResult::Error(res); // Propagate error
    }
  }

  // 2. Check immutable MemTable
  if (immutable_memtable_) {
    Result res = immutable_memtable_->Get(key);
     std::cout << "[DB::GetInternal] Immutable memtable Get. ok(): " << res.ok()
              << ", code: " << static_cast<int>(res.code())
              << ", msg: " << res.message()
              << ", has_value_slice: " << res.value_slice().has_value()
              << ", value_tag: " << (res.value_tag().has_value() ? static_cast<int>(res.value_tag().value()) : -1)
              << std::endl;
    if (res.ok()) {
      if (res.value_tag().has_value() && res.value_tag().value() == ValueTag::kTombstone) {
        std::cout << "[DB::GetInternal] Found TOMBSTONE in immutable memtable." << std::endl;
        return GetInternalResult::TombstoneFound();
      } else if (res.value_slice().has_value() && res.value_tag().has_value() && res.value_tag().value() == ValueTag::kData) {
        std::cout << "[DB::GetInternal] Found DATA in immutable memtable. Slice points to memtable's arena." << std::endl;
        return GetInternalResult::ValueFound(res.value_slice().value());
      } else {
        std::cout << "[DB::GetInternal] Immutable MemTable::Get OK but inconsistent state." << std::endl;
        return GetInternalResult::Error(Result::Corruption("Immutable MemTable::Get returned OK with inconsistent state"));
      }
    } else if (res.code() == ResultCode::kNotFound) {
      std::cout << "[DB::GetInternal] kNotFound (truly not in map) from immutable memtable. Continuing search..." << std::endl;
      // Fall through to check SSTables
    } else { // Other error
        std::cout << "[DB::GetInternal] Error from immutable memtable Get: " << res.message() << std::endl;
        return GetInternalResult::Error(res);
    }
  }

  // 3. Iterate L0 SSTables (newest to oldest)
  std::cout << "[DB::GetInternal] Key '" << key.ToString() << "' not in memtables. Checking " << l0_sstables_.size() << " L0 SSTables." << std::endl;
  for (const std::string& sstable_filename : l0_sstables_) {
    std::cout << "[DB::GetInternal] Checking SSTable: " << sstable_filename << " for key " << key.ToString() << std::endl;

    SSTableReader local_reader_instance(sstable_filename);
    Result reader_init_res = local_reader_instance.Init();
    if (!reader_init_res.ok()) {
        std::cout << "[DB::GetInternal] Failed to init reader for " << sstable_filename << ". Skipping. Msg: " << reader_init_res.message() << std::endl;
        // Consider if this should be a propagated error if any SSTable is unreadable.
        // For now, we try to find the key in other readable tables.
        continue;
    }

    Arena temp_local_arena_for_sst_read; // Used if sstable_target_arena_for_copy is nullptr
    Arena* arena_to_use_for_sst_get = sstable_target_arena_for_copy ? sstable_target_arena_for_copy : &temp_local_arena_for_sst_read;
    std::cout << "[DB::GetInternal] Using arena " << arena_to_use_for_sst_get
              << (arena_to_use_for_sst_get == sstable_target_arena_for_copy ? " (caller-provided)" : " (temporary local)")
              << " for SSTableReader::Get." << std::endl;

    // ASSUMPTION: SSTableReader::Get is updated to return Result with value_tag_
    Result sst_read_res = local_reader_instance.Get(key, arena_to_use_for_sst_get);

    std::cout << "[DB::GetInternal] SSTableReader (" << sstable_filename << ") Get result. ok(): "
              << (sst_read_res.ok() ? "true" : "false") << ", code(): " << static_cast<int>(sst_read_res.code())
              << ", message(): '" << sst_read_res.message() << "'"
              << ", has_value_slice: " << sst_read_res.value_slice().has_value()
              << ", value_tag: " << (sst_read_res.value_tag().has_value() ? static_cast<int>(sst_read_res.value_tag().value()) : -1)
              << std::endl;

    if (sst_read_res.ok()) {
      if (sst_read_res.value_tag().has_value() && sst_read_res.value_tag().value() == ValueTag::kTombstone) {
        std::cout << "[DB::GetInternal] Found TOMBSTONE in SSTable " << sstable_filename << "." << std::endl;
        return GetInternalResult::TombstoneFound();
      } else if (sst_read_res.value_slice().has_value() && sst_read_res.value_tag().has_value() && sst_read_res.value_tag().value() == ValueTag::kData) {
        // Actual data found; slice points into arena_to_use_for_sst_get
        std::cout << "[DB::GetInternal] Found DATA in SSTable " << sstable_filename << ". Slice points to arena: " << arena_to_use_for_sst_get << std::endl;
        return GetInternalResult::ValueFound(sst_read_res.value_slice().value());
      } else {
        std::cout << "[DB::GetInternal] SSTableReader::Get OK but inconsistent state for " << sstable_filename << std::endl;
        return GetInternalResult::Error(Result::Corruption("SSTableReader::Get returned OK with inconsistent state for " + sstable_filename));
      }
    } else if (sst_read_res.code() == ResultCode::kNotFound) {
      // kNotFound from SSTableReader means key truly not in this file (neither data nor tombstone). Continue search.
      std::cout << "[DB::GetInternal] kNotFound (truly not in file) from SSTable " << sstable_filename << ". Continuing search..." << std::endl;
      // Fall through to check next SSTable
    } else { // Any other error from SSTableReader (e.g., kIOError, kCorruption)
      std::cout << "[DB::GetInternal] Error from SSTableReader " << sstable_filename << ": " << sst_read_res.message() << ". Aborting GetInternal." << std::endl;
      return GetInternalResult::Error(sst_read_res); // Propagate the error
    }
  }
  std::cout << "[DB::GetInternal] Key '" << key.ToString() << "' truly not found after all checks." << std::endl;
  return GetInternalResult::TrulyNotFound();
}


Result DB::Get(const Slice& key, std::string* value_out) {
  if (value_out == nullptr) {
    return Result::InvalidArgument("Output string pointer (value_out) is null.");
  }
  value_out->clear();
  std::cout << "[DB::Get string*] ENTER for key: " << key.ToString() << std::endl;

  // GetInternal uses a temporary arena for SSTable reads if sstable_target_arena_for_copy is nullptr.
  GetInternalResult internal_res = GetInternal(key, nullptr);

  std::cout << "[DB::Get string*] GetInternal result. status.ok(): " << internal_res.status.ok()
            << ", is_tombstone: " << internal_res.is_tombstone
            << ", data_slice.size (if any): " << (internal_res.status.ok() && !internal_res.is_tombstone && internal_res.data_slice.data() ? internal_res.data_slice.size() : 0)
            << std::endl;

  if (internal_res.status.ok() && !internal_res.is_tombstone) { // Value was found
    // internal_res.data_slice points to memory in a memtable's arena or a temporary arena from GetInternal.
    // Copy it to the user's string.
    if (internal_res.data_slice.data() != nullptr) { // Ensure data pointer is valid before using
        value_out->assign(reinterpret_cast<const char*>(internal_res.data_slice.data()),
                         internal_res.data_slice.size());
        std::cout << "[DB::Get string*] Copied to string. Value: " << *value_out << std::endl;
    } else if (internal_res.data_slice.size() == 0) { // Empty value is valid
        std::cout << "[DB::Get string*] Found empty value, string is empty." << std::endl;
    } else { // Should not happen: null data with non-zero size
        return Result::Corruption("GetInternal returned OK with null data slice for non-empty size");
    }
    return Result::OK();
  } else {
    // This will be kNotFound if tombstone or truly not found, or an error.
    std::cout << "[DB::Get string*] GetInternal indicated not found/tombstone or error. Final Status code: "
              << static_cast<int>(internal_res.status.code()) << ", Msg: " << internal_res.status.message() << std::endl;
    return internal_res.status;
  }
}

Result DB::Get(const Slice& key, Arena& result_arena) {
  std::cout << "[DB::Get Arena&] ENTER for key: " << key.ToString() << ", using provided result_arena: " << &result_arena << std::endl;

  // Pass the caller's result_arena to GetInternal.
  GetInternalResult internal_res = GetInternal(key, &result_arena);

   std::cout << "[DB::Get Arena&] GetInternal result. status.ok(): " << internal_res.status.ok()
            << ", is_tombstone: " << internal_res.is_tombstone
            << ", data_slice.data (if any): " << (internal_res.status.ok() && !internal_res.is_tombstone && internal_res.data_slice.data() ? (void*)internal_res.data_slice.data() : nullptr)
            << std::endl;

  if (internal_res.status.ok() && !internal_res.is_tombstone) { // Value was found
    const Slice& original_slice = internal_res.data_slice;

    // If data came from SSTable, GetInternal already used result_arena via sstable_target_arena_for_copy.
    // If data came from MemTable, original_slice points to MemTable's arena.
    // This Get overload MUST ensure the final Slice points into result_arena.

    bool data_already_in_result_arena = false;
#ifdef LSM_PROJECT_ENABLE_TESTING_HOOKS // Or a general Arena::Contains method
    if (original_slice.size() > 0 && original_slice.data() != nullptr && result_arena.IsAddressInCurrentBlock(original_slice.data())) {
        // Heuristic: data might already be in the current block of result_arena.
        // This happens if GetInternal read from an SSTable into result_arena.
        data_already_in_result_arena = true;
        std::cout << "[DB::Get Arena&] Data slice appears to be ALREADY in result_arena's current block." << std::endl;
    }
#endif
    // A more robust check would be Arena::Contains(ptr), checking all blocks.
    // For now, if the heuristic doesn't confirm it, or if no testing hooks, we copy.
    // A simpler approach: If original_slice.data() is not null and its arena is not &result_arena
    // (this would require Slice to know its Arena, or a more complex check).
    // Given GetInternal's logic, if sstable_target_arena_for_copy was used and that was result_arena,
    // and data came from SSTable, it's already there. If from MemTable, it's not.

    if (data_already_in_result_arena) {
        std::cout << "[DB::Get Arena&] Re-using slice already in result_arena." << std::endl;
        return Result::OK(original_slice);
    } else {
        std::cout << "[DB::Get Arena&] Data slice from memtable or copy required. Allocating and copying to result_arena." << std::endl;
        void* copied_value_ptr = result_arena.Allocate(original_slice.size(), alignof(std::byte));
        if (original_slice.size() > 0 && original_slice.data() == nullptr) { // Should not happen for non-empty slice from valid source
             return Result::Corruption("GetInternal returned OK with null data for non-empty slice");
        }
        if (original_slice.size() > 0 && !copied_value_ptr) { // Allocation failure
          std::cout << "[DB::Get Arena&] Failed to allocate in result_arena for copying." << std::endl;
          return Result::ArenaAllocationFail("Failed to copy/allocate value in result_arena.");
        }

        if (original_slice.size() > 0) {
            std::memcpy(copied_value_ptr, original_slice.data(), original_slice.size());
            std::cout << "[DB::Get Arena&] Copied data from " << (void*)original_slice.data()
                      << " to " << copied_value_ptr << " in result_arena." << std::endl;
        } else {
            std::cout << "[DB::Get Arena&] Original slice was empty, allocated 0 bytes in result_arena (ptr: " << copied_value_ptr << ")." << std::endl;
        }
        Slice final_slice(static_cast<const std::byte*>(copied_value_ptr), original_slice.size());
        return Result::OK(final_slice);
    }

  } else {
    std::cout << "[DB::Get Arena&] GetInternal indicated not found/tombstone or error. Final Status code: "
              << static_cast<int>(internal_res.status.code()) << ", Msg: " << internal_res.status.message() << std::endl;
    return internal_res.status;
  }
}