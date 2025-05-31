#include "gtest/gtest.h"
#include "skip_list.hpp"
#include "test_utils.hpp" // Include the test utilities header
#include "arena.hpp"

#include <string>
#include <vector>
#include <map>
#include <memory>

namespace lsm_project {

// Helper to convert Slice to std::string for easy comparison/printing in tests
// This is redundant if Slice::ToString() exists and works as expected.
// std::string SliceDebugString(const Slice& s) {
//     return s.ToString();
// }

class SkipListTest : public ::testing::Test {
 protected:
  static constexpr size_t kArenaSize = 1024 * 1024; // 1MB Arena for tests
  Arena arena_;
  SkipList list_; // Uses the map-backed placeholder SkipList for now

  SkipListTest() : arena_(kArenaSize), list_(arena_) {}

  void PutString(const std::string& k, const std::string& v) {
    Result r = list_.Put(Slice(k), Slice(v));
    ASSERT_TRUE(r.ok()) << "Put failed for key '" << k << "': " << r.ToString();
  }

  Result GetString(const std::string& k, std::string* value_out_str = nullptr) {
    Result r = list_.Get(Slice(k)); // list_.Get() is SkipList::Get()

    std::cout << "[SkipListTest::GetString] For key '" << k << "', list_.Get() result: ok=" << r.ok()
              << ", code=" << static_cast<int>(r.code()) << ", msg='" << r.message()
              << "', has_slice=" << r.value_slice().has_value()
              << ", tag=" << (r.value_tag().has_value() ? static_cast<int>(r.value_tag().value()) : -1)
              << std::endl; // DEBUG

    if (r.ok()) {
        // Check if it's a tombstone first
        if (r.value_tag().has_value() && r.value_tag().value() == ValueTag::kTombstone) {
            std::cout << "[SkipListTest::GetString] Detected TOMBSTONE for key '" << k << "'. Returning NotFound." << std::endl; //DEBUG
            // It's a tombstone. For the purpose of GetString returning a value, this means "not found".
            return Result::NotFound("Key '" + k + "' is a tombstone in SkipList");
        }
        
        // If it's OK and not a tombstone, it must be data and have a value_slice AND data tag.
        if (r.value_slice().has_value() && r.value_tag().has_value() && r.value_tag().value() == ValueTag::kData) {
            if (value_out_str) {
                *value_out_str = r.value_slice().value().ToString();
            }
            // Return the original 'r' because it's already a valid OK_Data result
            return r; 
        } else {
            // This path means r.ok() is true, but it's not a tombstone and not valid data.
            std::cout << "[SkipListTest::GetString] Corruption: OK but not Tombstone and not valid Data for key '" << k << "'" << std::endl; //DEBUG
            return Result::Corruption(
                "Internal error in GetString for key '" + k +
                "': Get() returned OK but not valid data or tombstone. Original Result: " + r.ToString());
        }
    }
    return r; // Return original result if not r.ok() initially (e.g. kNotFound from SkipList::Get)
}
};


TEST_F(SkipListTest, EmptyList) {
    std::string val_str;
    Result r = GetString("any_key", &val_str);
    ASSERT_FALSE(r.ok());
    ASSERT_EQ(r.code(), ResultCode::kNotFound);
    // For the map-backed SkipList, ApproximateMemoryUsage should be the size of the
    // SkipList object itself plus arena usage (which should be minimal or zero if
    // the constructor doesn't allocate anything from the arena by default).
    // The placeholder doesn't have a head_ node in the arena like a real skip list.
    size_t expected_empty_usage = sizeof(SkipList) + arena_.GetTotalBytesUsed();
    ASSERT_EQ(list_.ApproximateMemoryUsage(), expected_empty_usage);
}


TEST_F(SkipListTest, PutAndGetSingle) {
    PutString("key1", "value1");
    std::string val;
    Result r = GetString("key1", &val);
    ASSERT_TRUE(r.ok()) << "Failed to get key1: " << r.ToString();
    ASSERT_EQ(val, "value1");

    Result r_non_exist = GetString("non_existent_key", &val);
    ASSERT_FALSE(r_non_exist.ok());
    ASSERT_EQ(r_non_exist.code(), ResultCode::kNotFound);
}

TEST_F(SkipListTest, PutUpdatesValue) {
    PutString("key1", "value1");
    PutString("key1", "value1_updated"); // This should update the value for "key1"

    std::string val;
    Result r = GetString("key1", &val);
    ASSERT_TRUE(r.ok());
    ASSERT_EQ(val, "value1_updated");
}

TEST_F(SkipListTest, PutMultipleOrdered) {
    PutString("a_key", "val_a");
    PutString("b_key", "val_b");
    PutString("c_key", "val_c");

    std::string val;
    ASSERT_TRUE(GetString("a_key", &val).ok() && val == "val_a");
    ASSERT_TRUE(GetString("b_key", &val).ok() && val == "val_b");
    ASSERT_TRUE(GetString("c_key", &val).ok() && val == "val_c");
}

TEST_F(SkipListTest, PutMultipleReverseOrdered) {
    PutString("c_key", "val_c");
    PutString("b_key", "val_b");
    PutString("a_key", "val_a");

    std::string val;
    ASSERT_TRUE(GetString("a_key", &val).ok() && val == "val_a");
    ASSERT_TRUE(GetString("b_key", &val).ok() && val == "val_b");
    ASSERT_TRUE(GetString("c_key", &val).ok() && val == "val_c");
}

TEST_F(SkipListTest, PutMultipleMixedOrder) {
    PutString("b_key", "val_b");
    PutString("d_key", "val_d");
    PutString("a_key", "val_a");
    PutString("c_key", "val_c");

    std::string val;
    ASSERT_TRUE(GetString("a_key", &val).ok() && val == "val_a");
    ASSERT_TRUE(GetString("b_key", &val).ok() && val == "val_b");
    ASSERT_TRUE(GetString("c_key", &val).ok() && val == "val_c");
    ASSERT_TRUE(GetString("d_key", &val).ok() && val == "val_d");
}

TEST_F(SkipListTest, IteratorEmptyList) {
    std::unique_ptr<SortedTableIterator> iter(list_.NewIterator());
    ASSERT_NE(iter, nullptr);
    iter->SeekToFirst(); // Should be safe to call on empty
    ASSERT_FALSE(iter->Valid());
}

TEST_F(SkipListTest, IteratorForwardIteration) {
    std::map<std::string, std::string> expected;
    expected["apple"] = "red";
    expected["banana"] = "yellow";
    expected["cherry"] = "dark_red";
    expected["date"] = "brown";

    for (const auto& pair : expected) {
        PutString(pair.first, pair.second);
    }

    std::unique_ptr<SortedTableIterator> iter(list_.NewIterator());
    ASSERT_NE(iter, nullptr);
    iter->SeekToFirst();
    size_t count = 0;
    auto expected_it = expected.begin();

    while (iter->Valid() && expected_it != expected.end()) {
        ASSERT_EQ(iter->key().ToString(), expected_it->first);
        // Access ValueEntry, check it's not a tombstone, then get the value_slice
        ValueEntry entry = iter->value();
        ASSERT_FALSE(entry.IsTombstone());
        ASSERT_EQ(entry.value_slice.ToString(), expected_it->second);
        count++;
        iter->Next();
        ++expected_it;
    }
    ASSERT_EQ(count, expected.size());
    ASSERT_FALSE(iter->Valid()); // Should be invalid after iterating through all
    ASSERT_TRUE(expected_it == expected.end());
}

TEST_F(SkipListTest, IteratorSeek) {
    PutString("a", "1");
    PutString("c", "3");
    PutString("e", "5");
    PutString("g", "7");

    std::unique_ptr<SortedTableIterator> iter(list_.NewIterator());
    ASSERT_NE(iter, nullptr);

    // Seek to existing key
    iter->Seek(Slice("c"));
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "c");
    ValueEntry entry_c = iter->value();
    ASSERT_FALSE(entry_c.IsTombstone());
    ASSERT_EQ(entry_c.value_slice.ToString(), "3");

    // Seek to non-existing key (should go to next greater)
    iter->Seek(Slice("b"));
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "c"); // Should point to 'c'

    // Seek to another non-existing key
    iter->Seek(Slice("d"));
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "e"); // Should point to 'e'
    
    // Seek to key greater than all existing keys
    iter->Seek(Slice("z"));
    ASSERT_FALSE(iter->Valid());

    // Seek to key smaller than all existing keys
    iter->Seek(Slice("0")); // Assuming "0" < "a"
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "a");

    // Seek on empty list
    Arena empty_arena(kArenaSize); // Separate arena for this specific test
    SkipList empty_list(empty_arena);
    std::unique_ptr<SortedTableIterator> empty_iter(empty_list.NewIterator());
    ASSERT_NE(empty_iter, nullptr);
    empty_iter->Seek(Slice("any"));
    ASSERT_FALSE(empty_iter->Valid());
}

TEST_F(SkipListTest, IteratorSeekToFirstThenNext) {
    PutString("b", "2");
    PutString("a", "1"); // Insert out of order
    PutString("c", "3");

    std::unique_ptr<SortedTableIterator> iter(list_.NewIterator());
    ASSERT_NE(iter, nullptr);
    iter->SeekToFirst();

    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "a");
    ValueEntry entry_a = iter->value();
    ASSERT_FALSE(entry_a.IsTombstone());
    ASSERT_EQ(entry_a.value_slice.ToString(), "1");

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "b");
    ValueEntry entry_b = iter->value();
    ASSERT_FALSE(entry_b.IsTombstone());
    ASSERT_EQ(entry_b.value_slice.ToString(), "2");

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "c");
    ValueEntry entry_c_iter = iter->value(); // Renamed to avoid conflict
    ASSERT_FALSE(entry_c_iter.IsTombstone());
    ASSERT_EQ(entry_c_iter.value_slice.ToString(), "3");

    iter->Next();
    ASSERT_FALSE(iter->Valid());
}


// Test with a different max_height to ensure flexibility
// (This mostly tests constructor API compatibility for the placeholder)
class SkipListCustomHeightTest : public ::testing::Test {
 protected:
  Arena arena_;
  SkipList list_; // Placeholder SkipList

  SkipListCustomHeightTest() : arena_(1024*1024), list_(arena_, 4 /* max_height=4 */, 0.5 /* probability */) {}
};

TEST_F(SkipListCustomHeightTest, PutAndGetWithLowMaxHeight) {
    for (int i = 0; i < 100; ++i) { // Insert 100 elements
        std::string key = "key" + std::to_string(i);
        std::string val = "val" + std::to_string(i);
        Result r_put = list_.Put(Slice(key), Slice(val));
        ASSERT_TRUE(r_put.ok()) << r_put.ToString();
    }

    for (int i = 0; i < 100; ++i) {
        std::string key = "key" + std::to_string(i);
        std::string expected_val = "val" + std::to_string(i);
        std::string actual_val_str;
        Result r_get = list_.Get(Slice(key)); // Call Get from the list_ member
        
        ASSERT_TRUE(r_get.ok()) << "Failed for key: " << key << ". Result: " << r_get.ToString();
        ASSERT_TRUE(r_get.value_slice().has_value()) << "No value slice for key: " << key;
        actual_val_str = r_get.value_slice().value().ToString();
        ASSERT_EQ(actual_val_str, expected_val);
    }
}

TEST_F(SkipListTest, PutDeleteGet) {
    PutString("key1", "value1");
    std::string val;
    Result r_get = GetString("key1", &val);
    ASSERT_TRUE(r_get.ok());
    ASSERT_EQ(val, "value1");

    Result r_del = list_.Delete(Slice("key1"));
    ASSERT_TRUE(r_del.ok()) << "Delete failed: " << r_del.ToString();

    Result r_get_after_delete = GetString("key1", &val);
    ASSERT_FALSE(r_get_after_delete.ok());
    ASSERT_EQ(r_get_after_delete.code(), ResultCode::kNotFound);
}

TEST_F(SkipListTest, DeleteNonExistentKey) {
    Result r_del = list_.Delete(Slice("key_never_existed"));
    ASSERT_TRUE(r_del.ok()) << "Delete of non-existent key failed: " << r_del.ToString();

    Result r_get = GetString("key_never_existed", nullptr);
    ASSERT_FALSE(r_get.ok());
    ASSERT_EQ(r_get.code(), ResultCode::kNotFound);
}

TEST_F(SkipListTest, IteratorShowsTombstones) {
    PutString("a", "apple");
    PutString("b", "banana");
    Result r_del = list_.Delete(Slice("a"));
    ASSERT_TRUE(r_del.ok()) << r_del.ToString();
    PutString("c", "cherry");

    std::unique_ptr<SortedTableIterator> iter(list_.NewIterator());
    ASSERT_NE(iter, nullptr);
    iter->SeekToFirst();

    // Entry 'a' should be a tombstone
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "a");
    ValueEntry entry_a_iter = iter->value(); // Get ValueEntry
    ASSERT_TRUE(entry_a_iter.IsTombstone()); 

    // Entry 'b' should be a live value
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "b");
    ValueEntry entry_b_iter = iter->value();
    ASSERT_FALSE(entry_b_iter.IsTombstone());
    ASSERT_EQ(entry_b_iter.value_slice.ToString(), "banana");
    
    // Entry 'c' should be a live value
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "c");
    ValueEntry entry_c_iter_shows = iter->value(); // Renamed to avoid conflict
    ASSERT_FALSE(entry_c_iter_shows.IsTombstone());
    ASSERT_EQ(entry_c_iter_shows.value_slice.ToString(), "cherry");

    iter->Next();
    ASSERT_FALSE(iter->Valid());
}

} // namespace lsm_project