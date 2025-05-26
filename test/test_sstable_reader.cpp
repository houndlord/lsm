// sstable_reader_test.cpp
#include "gtest/gtest.h"
#include "sstable_reader.hpp"
#include "sstable_writer.hpp"
#include "sstable_iterator.hpp"
#include "mem_table.hpp"
#include "arena.hpp"
#include "slice.hpp"
#include "value.hpp"
#include "result.hpp"
#include "test_utils.hpp" // Include the test utilities header

#include <vector>
#include <string>
#include <cstdio>
#include <algorithm>


// --- Test Fixture ---
class SSTableReaderAndIteratorTest : public ::testing::Test {
protected:
    std::unique_ptr<Arena> arena_for_writes_; // For populating memtable for SSTableWriter
    std::unique_ptr<Arena> arena_for_reads_;  // For SSTableReader::Get() results
    
    std::string temp_sstable_filename_ = "temp_sstable_for_reader_test.dat";
    static constexpr size_t DEFAULT_TARGET_BLOCK_SIZE = 4096;

    // Populates a MemTable. Used by WriteTestSSTable.
    std::unique_ptr<MemTable> CreateAndPopulateMemTable(const std::vector<TestEntry>& entries) {
        // arena_for_writes_ must be valid before calling this
        auto memtable = std::make_unique<MemTable>(*arena_for_writes_);
        for (const auto& entry : entries) {
            Slice key_slice = StringToSlice(*arena_for_writes_, entry.key);
            if (entry.tag == ValueTag::kData) {
                Slice value_slice = StringToSlice(*arena_for_writes_, entry.value);
                Result res = memtable->Put(key_slice, value_slice);
                EXPECT_TRUE(res.ok()) << "MemTable Put failed for key '" << entry.key << "': " << res.message();
            } else { // Tombstone
                Result res = memtable->Delete(key_slice);
                EXPECT_TRUE(res.ok()) << "MemTable Delete failed for key '" << entry.key << "': " << res.message();
            }
        }
        return memtable;
    }

    // Creates an SSTable file with the given entries.
    void WriteTestSSTable(const std::vector<TestEntry>& entries, 
                          bool compression_enabled, 
                          size_t target_block_size = DEFAULT_TARGET_BLOCK_SIZE) {
        arena_for_writes_ = std::make_unique<Arena>(); // Fresh arena for each write setup
        auto memtable = CreateAndPopulateMemTable(entries);

        SSTableWriter writer(compression_enabled, 1 /*compression_level*/, target_block_size);
        Result init_res = writer.Init();
        ASSERT_TRUE(init_res.ok()) << "SSTableWriter Init failed: " << init_res.message();
        
        Result write_res = writer.WriteMemTableToFile(*memtable, temp_sstable_filename_);
        ASSERT_TRUE(write_res.ok()) << "SSTableWriter::WriteMemTableToFile failed: " << write_res.message();
    }

    void SetUp() override {
        arena_for_reads_ = std::make_unique<Arena>();
        // Ensure file from previous test (if any) is removed.
        std::remove(temp_sstable_filename_.c_str());
    }

    void TearDown() override {
        std::remove(temp_sstable_filename_.c_str());
        arena_for_writes_.reset(); // Release arena used for writing
        arena_for_reads_.reset();  // Release arena used for reading
    }
};

// --- SSTableReader Tests ---

TEST_F(SSTableReaderAndIteratorTest, Reader_Init_NonExistentFile) {
    SSTableReader reader("non_existent_file.sstable");
    Result res = reader.Init();
    ASSERT_FALSE(res.ok());
    ASSERT_EQ(res.code(), ResultCode::kIOError); // Or specific code for file not found
    ASSERT_FALSE(reader.isOpen());
}

TEST_F(SSTableReaderAndIteratorTest, Reader_Init_EmptySSTableFile) {
    WriteTestSSTable({}, false); // Write an empty SSTable (no compression)
    
    SSTableReader reader(temp_sstable_filename_);
    Result res = reader.Init();
    ASSERT_TRUE(res.ok()) << "Init failed for empty SSTable: " << res.message();
    ASSERT_TRUE(reader.isOpen());

    // Get on an empty SSTable should be NotFound
    Slice test_key("anykey");
    Result get_res = reader.Get(test_key, arena_for_reads_.get());
    ASSERT_FALSE(get_res.ok());
    ASSERT_EQ(get_res.code(), ResultCode::kNotFound);
}

TEST_F(SSTableReaderAndIteratorTest, Reader_Init_ValidSSTableFile) {
    std::vector<TestEntry> entries = {{"key1", "value1"}};
    WriteTestSSTable(entries, false); // No compression
    
    SSTableReader reader(temp_sstable_filename_);
    Result res = reader.Init();
    ASSERT_TRUE(res.ok()) << "Init failed for valid SSTable: " << res.message();
    ASSERT_TRUE(reader.isOpen());
}

TEST_F(SSTableReaderAndIteratorTest, Reader_Get_SingleBlock_KeyExists_NoCompression) {
    std::vector<TestEntry> entries = {{"key1", "value1_nc"}, {"key2", "value2_nc"}};
    WriteTestSSTable(entries, false); // No compression

    SSTableReader reader(temp_sstable_filename_);
    ASSERT_TRUE(reader.Init().ok());

    Slice key_to_find("key1");
    Result get_res = reader.Get(key_to_find, arena_for_reads_.get());
    ASSERT_TRUE(get_res.ok()) << "Get failed for key1: " << get_res.message();
    ASSERT_TRUE(get_res.value_slice().has_value());
    ASSERT_EQ(get_res.value_slice().value().ToString(), "value1_nc");
    ASSERT_TRUE(IsPointerDistinctFromBuffer(get_res.value_slice().value().data(), reader.TEST_ONLY_get_internal_buffer_DEBUG()))
        << "Value slice should point to arena memory, not reader's internal buffer.";
}

TEST_F(SSTableReaderAndIteratorTest, Reader_Get_SingleBlock_KeyExists_WithCompression) {
    std::vector<TestEntry> entries = {
        {"keyA", std::string(100, 'a')}, 
        {"keyB", std::string(100, 'b')}
    };
    WriteTestSSTable(entries, true); // With compression

    SSTableReader reader(temp_sstable_filename_);
    ASSERT_TRUE(reader.Init().ok());

    Slice key_to_find("keyA");
    Result get_res = reader.Get(key_to_find, arena_for_reads_.get());
    ASSERT_TRUE(get_res.ok()) << "Get failed for keyA (compressed): " << get_res.message();
    ASSERT_TRUE(get_res.value_slice().has_value());
    ASSERT_EQ(get_res.value_slice().value().ToString(), std::string(100, 'a'));
    ASSERT_TRUE(IsPointerDistinctFromBuffer(get_res.value_slice().value().data(), reader.TEST_ONLY_get_internal_buffer_DEBUG()));

}

TEST_F(SSTableReaderAndIteratorTest, Reader_Get_KeyNotExists) {
    std::vector<TestEntry> entries = {{"key1", "value1"}};
    WriteTestSSTable(entries, false);

    SSTableReader reader(temp_sstable_filename_);
    ASSERT_TRUE(reader.Init().ok());

    Slice key_to_find("non_existent_key");
    Result get_res = reader.Get(key_to_find, arena_for_reads_.get());
    ASSERT_FALSE(get_res.ok());
    ASSERT_EQ(get_res.code(), ResultCode::kNotFound);
}

TEST_F(SSTableReaderAndIteratorTest, Reader_Get_KeyIsTombstone_ReturnsNotFound) {
    std::vector<TestEntry> entries = {
        {"live_key", "live_value"},
        {"deleted_key", "", ValueTag::kTombstone} // Writer handles empty value for tombstone
    };
    WriteTestSSTable(entries, false);

    SSTableReader reader(temp_sstable_filename_);
    ASSERT_TRUE(reader.Init().ok());

    Slice key_to_find("deleted_key");
    Result get_res = reader.Get(key_to_find, arena_for_reads_.get());
    ASSERT_FALSE(get_res.ok()) << "Get for a tombstone should result in NotFound.";
    ASSERT_EQ(get_res.code(), ResultCode::kNotFound);
}


TEST_F(SSTableReaderAndIteratorTest, Reader_Get_MultiBlock_KeyInLaterBlock) {
    std::vector<TestEntry> entries;
    // Create entries that will span multiple blocks.
    // Approx entry size: 4(klen) + key.size + 1(tag) + 4(vlen) + val.size
    // ~10 + key + val. Target block size 100 for test.
    entries.push_back({"key01", std::string(30, 'a')}); // Block 1
    entries.push_back({"key02", std::string(30, 'b')}); // Block 1
    // Approx used by above two: (10+5+30)*2 = 90. Next one should start new block.
    entries.push_back({"key03", std::string(30, 'c')}); // Block 2
    entries.push_back({"key04", std::string(30, 'd')}); // Block 2

    WriteTestSSTable(entries, false, 100 /* small target_block_size */);

    SSTableReader reader(temp_sstable_filename_);
    ASSERT_TRUE(reader.Init().ok());

    Slice key_to_find("key03"); // Expected in the second block
    Result get_res = reader.Get(key_to_find, arena_for_reads_.get());
    ASSERT_TRUE(get_res.ok()) << "Get failed for key03 (multi-block): " << get_res.message();
    ASSERT_TRUE(get_res.value_slice().has_value());
    ASSERT_EQ(get_res.value_slice().value().ToString(), std::string(30, 'c'));
    ASSERT_TRUE(IsPointerDistinctFromBuffer(get_res.value_slice().value().data(), reader.TEST_ONLY_get_internal_buffer_DEBUG()));
}

TEST_F(SSTableReaderAndIteratorTest, Reader_Get_ValueIsEmpty) {
    std::vector<TestEntry> entries = {{"key_with_empty_value", ""}};
    WriteTestSSTable(entries, false);

    SSTableReader reader(temp_sstable_filename_);
    ASSERT_TRUE(reader.Init().ok());
    
    Slice key_to_find("key_with_empty_value");
    Result get_res = reader.Get(key_to_find, arena_for_reads_.get());
    ASSERT_TRUE(get_res.ok()) << get_res.message();
    ASSERT_TRUE(get_res.value_slice().has_value());
    ASSERT_TRUE(get_res.value_slice().value().empty());
    // Arena::Allocate(0) returns nullptr. Slice(nullptr,0) is valid.
    ASSERT_EQ(get_res.value_slice().value().data(), nullptr); 
}


// --- SSTableIterator Tests ---

TEST_F(SSTableReaderAndIteratorTest, Iterator_EmptyFile) {
    WriteTestSSTable({}, false); // Empty SSTable
    
    SSTableReader reader(temp_sstable_filename_);
    ASSERT_TRUE(reader.Init().ok());

    SSTableIterator iter(&reader);
    ASSERT_FALSE(iter.Valid()) << "Iterator on empty SSTable should not be valid initially.";
    ASSERT_TRUE(iter.status().ok());

    iter.SeekToFirst();
    ASSERT_FALSE(iter.Valid()) << "SeekToFirst on empty SSTable should not be valid.";
    ASSERT_TRUE(iter.status().ok());

    iter.Seek(Slice("anykey"));
    ASSERT_FALSE(iter.Valid()) << "Seek on empty SSTable should not be valid.";
    ASSERT_TRUE(iter.status().ok());
}

TEST_F(SSTableReaderAndIteratorTest, Iterator_SingleBlock_ForwardIteration) {
    std::vector<TestEntry> entries_to_write = {
        {"b_key", "val_b"}, {"a_key", "val_a"}, {"c_key", "val_c"}
    };
    // SSTableWriter will sort them: a_key, b_key, c_key
    std::vector<TestEntry> expected_sorted_entries = {
        {"a_key", "val_a"}, {"b_key", "val_b"}, {"c_key", "val_c"}
    };
    WriteTestSSTable(entries_to_write, false); // No compression

    SSTableReader reader(temp_sstable_filename_);
    ASSERT_TRUE(reader.Init().ok());
    SSTableIterator iter(&reader);

    int count = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        ASSERT_LT(count, expected_sorted_entries.size());
        EXPECT_EQ(iter.key().ToString(), expected_sorted_entries[count].key);
        
        ValueEntry val_entry = iter.value();
        EXPECT_FALSE(val_entry.IsTombstone());
        EXPECT_EQ(val_entry.value_slice.ToString(), expected_sorted_entries[count].value);
        
        EXPECT_TRUE(iter.status().ok());
        count++;
    }
    EXPECT_EQ(count, expected_sorted_entries.size());
    EXPECT_FALSE(iter.Valid());
}

TEST_F(SSTableReaderAndIteratorTest, Iterator_MultiBlock_ForwardIteration_WithCompression) {
    std::vector<TestEntry> entries;
    // Will be sorted by writer as: entry01, entry02, ..., entry10
    for (int i = 1; i <= 10; ++i) {
        char k_buf[10];
        snprintf(k_buf, sizeof(k_buf), "key%02d", i);
        // Make values somewhat compressible and large enough to span blocks
        entries.push_back({std::string(k_buf), std::string(50, static_cast<char>('a' + i))});
    }
    // Using target_block_size of 100. Each entry approx: 4+5 + 1 + 4 + 50 = 64 bytes.
    // So, 1 entry per block. (100 is small, so header + 1 entry might exceed).
    // Let's make it 200, so ~3 entries per block.
    WriteTestSSTable(entries, true, 200 /* target_block_size */); 

    SSTableReader reader(temp_sstable_filename_);
    ASSERT_TRUE(reader.Init().ok());
    SSTableIterator iter(&reader);

    // Sort `entries` to get `expected_sorted_entries` as SSTableWriter does
    std::vector<TestEntry> expected_sorted_entries = entries;
    std::sort(expected_sorted_entries.begin(), expected_sorted_entries.end(), 
              [](const TestEntry&a, const TestEntry&b){ return a.key < b.key; });

    int count = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        ASSERT_LT(count, expected_sorted_entries.size()) << "Too many items from iterator";
        EXPECT_EQ(iter.key().ToString(), expected_sorted_entries[count].key);
        EXPECT_EQ(iter.value().value_slice.ToString(), expected_sorted_entries[count].value);
        EXPECT_FALSE(iter.value().IsTombstone());
        EXPECT_TRUE(iter.status().ok());
        count++;
    }
    EXPECT_EQ(count, expected_sorted_entries.size());
    EXPECT_FALSE(iter.Valid());
}


TEST_F(SSTableReaderAndIteratorTest, Iterator_Seek) {
    std::vector<TestEntry> entries_to_write = {
        {"apple", "red"}, {"banana", "yellow"}, {"cherry", "red"},
        {"date", "brown"}, {"elderberry", "purple"}
    }; // Already sorted
    WriteTestSSTable(entries_to_write, false, 100 /* small blocks */);

    SSTableReader reader(temp_sstable_filename_);
    ASSERT_TRUE(reader.Init().ok());
    SSTableIterator iter(&reader);

    // Seek to existing key in a likely later block
    iter.Seek(Slice("date"));
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.key().ToString(), "date");
    ASSERT_EQ(iter.value().value_slice.ToString(), "brown");

    // Seek to non-existing key, should go to next
    iter.Seek(Slice("blueberry")); // Between banana and cherry
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.key().ToString(), "cherry");

    // Seek before first
    iter.Seek(Slice("aardvark"));
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.key().ToString(), "apple");
    
    // Seek after last
    iter.Seek(Slice("fig")); // After elderberry
    ASSERT_FALSE(iter.Valid());

    // Seek to exact first
    iter.Seek(Slice("apple"));
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.key().ToString(), "apple");

    // Seek to exact last
    iter.Seek(Slice("elderberry"));
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.key().ToString(), "elderberry");
    iter.Next();
    ASSERT_FALSE(iter.Valid());
}


TEST_F(SSTableReaderAndIteratorTest, Iterator_IterationIncludesTombstones) {
    std::vector<TestEntry> entries_to_write = {
        {"key1", "value1_live"},
        {"key2_tombstone", "", ValueTag::kTombstone},
        {"key3", "value3_live"}
    };
    // SSTableWriter will sort them.
    std::vector<TestEntry> expected_sorted_entries = {
        {"key1", "value1_live", ValueTag::kData},
        {"key2_tombstone", "", ValueTag::kTombstone},
        {"key3", "value3_live", ValueTag::kData}
    };
     std::sort(expected_sorted_entries.begin(), expected_sorted_entries.end());


    WriteTestSSTable(entries_to_write, false);

    SSTableReader reader(temp_sstable_filename_);
    ASSERT_TRUE(reader.Init().ok());
    SSTableIterator iter(&reader);

    iter.SeekToFirst();
    
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.key().ToString(), expected_sorted_entries[0].key);
    ASSERT_FALSE(iter.value().IsTombstone());
    ASSERT_EQ(iter.value().value_slice.ToString(), expected_sorted_entries[0].value);

    iter.Next();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.key().ToString(), expected_sorted_entries[1].key);
    ASSERT_TRUE(iter.value().IsTombstone()) << "key2_tombstone should be a tombstone";
    ASSERT_TRUE(iter.value().value_slice.empty()); // Tombstone's value slice is empty

    iter.Next();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(iter.key().ToString(), expected_sorted_entries[2].key);
    ASSERT_FALSE(iter.value().IsTombstone());
    ASSERT_EQ(iter.value().value_slice.ToString(), expected_sorted_entries[2].value);

    iter.Next();
    ASSERT_FALSE(iter.Valid());
}

// It would be good to add a way for SSTableReader to expose its internal buffer for testing IsPointerDistinctFromBuffer
// e.g., by adding a TEST_ONLY_get_internal_buffer_DEBUG() method.
// For now, this is how it's referenced in Reader_Get_SingleBlock_KeyExists_NoCompression, etc.
// In SSTableReader.hpp:
/*
public:
  // ... other public methods ...
  #ifdef ENABLE_TEST_HOOKS // Or some other testing macro
  const std::vector<char>& TEST_ONLY_get_internal_buffer_DEBUG() const {
    return internal_block_buffer_;
  }
  #else // Provide a stub if not testing to avoid compilation errors in tests
  const std::vector<char>& TEST_ONLY_get_internal_buffer_DEBUG() const {
    static std::vector<char> empty_stub; // Should not be called in non-test builds
    return empty_stub; 
  }
  #endif
*/
// Then define ENABLE_TEST_HOOKS when compiling tests.