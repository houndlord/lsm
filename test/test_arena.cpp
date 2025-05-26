#include "gtest/gtest.h"
#include "arena.hpp" // Your Arena header

#include <string>
#include <vector>
#include <numeric> // For std::iota
#include <cstdint> // For uintptr_t
#include <limits>  // For std::numeric_limits

// Helper to check alignment
static bool is_aligned(void* ptr, size_t alignment) {
    if (alignment == 0) return true; // Or handle as an error
    return reinterpret_cast<uintptr_t>(ptr) % alignment == 0;
}

// A simple struct to test with Arena::Create
struct TestObj {
    int id;
    double data;
    char name[16];

    static int constructor_calls;
    static int destructor_calls; // Arena won't call this unless 'Own' is used

    TestObj(int i, double d, const char* n) : id(i), data(d) {
        snprintf(name, sizeof(name), "%s", n);
        constructor_calls++;
    }

    // Arena typically doesn't call destructors unless it has an 'Own' mechanism
    ~TestObj() {
        destructor_calls++;
    }
};

int TestObj::constructor_calls = 0;
int TestObj::destructor_calls = 0;

// Utility to reset counters for TestObj
void ResetTestObjCounters() {
    TestObj::constructor_calls = 0;
    TestObj::destructor_calls = 0;
}

// Test Fixture for Arena tests
class ArenaTest : public ::testing::Test {
protected:
    const size_t initial_block_size_ = 1024; // Default for most tests
    const size_t small_block_size_ = 100;   // For new block trigger tests

    ArenaTest() {} // Constructor can be empty

    void SetUp() override {
        // Reset counters before each test that uses TestObj
        ResetTestObjCounters();
    }

    void TearDown() override {
        // Clean up after each test if needed
    }
};

// --- Basic Construction and State ---
TEST_F(ArenaTest, DefaultConstructor) {
    Arena arena(initial_block_size_);
    // Requires GetTotalBytesUsed() and GetNumBlocksAllocated() in Arena.hpp
    ASSERT_EQ(arena.GetTotalBytesUsed(), 0);
    ASSERT_EQ(arena.GetNumBlocksAllocated(), 1) << "Arena should allocate one initial block.";
}

TEST_F(ArenaTest, ZeroByteAllocation) {
    Arena arena(initial_block_size_);
    void* p = arena.Allocate(0);
    ASSERT_EQ(p, nullptr) << "Allocating 0 bytes should return nullptr.";
    ASSERT_EQ(arena.GetTotalBytesUsed(), 0);
}

// --- Basic Allocations ---
TEST_F(ArenaTest, SingleAllocation) {
    Arena arena(initial_block_size_);
    size_t alloc_size = 100;
    void* p = arena.Allocate(alloc_size);
    ASSERT_NE(p, nullptr);
    ASSERT_TRUE(is_aligned(p, DEFAULT_ARENA_ALIGNMENT));
    ASSERT_EQ(arena.GetTotalBytesUsed(), alloc_size);
}

TEST_F(ArenaTest, MultipleAllocationsWithinBlock) {
    Arena arena(initial_block_size_);
    size_t s1 = 50, s2 = 70, s3 = 30;

    char* p1 = static_cast<char*>(arena.Allocate(s1));
    ASSERT_NE(p1, nullptr);
    ASSERT_EQ(arena.GetTotalBytesUsed(), s1);

    char* p2 = static_cast<char*>(arena.Allocate(s2));
    ASSERT_NE(p2, nullptr);
    ASSERT_EQ(arena.GetTotalBytesUsed(), s1 + s2);
    ASSERT_GE(p2, p1 + s1) << "p2 should be after p1's data";

    char* p3 = static_cast<char*>(arena.Allocate(s3));
    ASSERT_NE(p3, nullptr);
    ASSERT_EQ(arena.GetTotalBytesUsed(), s1 + s2 + s3);
    ASSERT_GE(p3, p2 + s2) << "p3 should be after p2's data";
}

// --- Aligned Allocations ---
TEST_F(ArenaTest, AlignedAllocationDefault) {
    Arena arena(initial_block_size_);
    void* p = arena.Allocate(16); // Default alignment
    ASSERT_NE(p, nullptr);
    ASSERT_TRUE(is_aligned(p, DEFAULT_ARENA_ALIGNMENT));
    ASSERT_EQ(arena.GetTotalBytesUsed(), 16);
}

TEST_F(ArenaTest, AlignedAllocationSpecific) {
    Arena arena(initial_block_size_);
    size_t alignments_to_test[] = {1, 2, 4, 8, 16, 32, 64, 128};
    size_t total_requested = 0;

    for (size_t alignment : alignments_to_test) {
        if (alignment == 0) continue; // std::align requires non-zero alignment
        size_t alloc_size = alignment * 2; // Ensure size is multiple of alignment for simplicity
        void* p = arena.Allocate(alloc_size, alignment);
        ASSERT_NE(p, nullptr) << "Allocation failed for alignment " << alignment;
        ASSERT_TRUE(is_aligned(p, alignment)) << "Pointer not aligned to " << alignment;
        total_requested += alloc_size;
        ASSERT_EQ(arena.GetTotalBytesUsed(), total_requested) << "TotalBytesUsed mismatch for alignment " << alignment;
    }
}

TEST_F(ArenaTest, AlignedAllocationForcesSkip) {
    Arena arena(initial_block_size_);
    // Allocate 1 byte with default alignment (likely 8 or 16)
    char* p1 = static_cast<char*>(arena.Allocate(1, DEFAULT_ARENA_ALIGNMENT));
    ASSERT_NE(p1, nullptr);
    size_t current_used = 1;
    ASSERT_EQ(arena.GetTotalBytesUsed(), current_used);

    // Now allocate with a large alignment that will likely require skipping bytes
    size_t large_alignment = 64;
    char* p2 = static_cast<char*>(arena.Allocate(10, large_alignment));
    ASSERT_NE(p2, nullptr);
    ASSERT_TRUE(is_aligned(p2, large_alignment));
    current_used += 10;
    ASSERT_EQ(arena.GetTotalBytesUsed(), current_used);

    // p2 should be at least p1 + 1, but potentially much further due to alignment
    ASSERT_GE(p2, p1 + 1);
}


// --- New Block Allocations ---
TEST_F(ArenaTest, AllocationTriggersNewBlock) {
    Arena arena(small_block_size_); // Use a small initial block
    ASSERT_EQ(arena.GetNumBlocksAllocated(), 1);

    // Allocate most of the first block (leave little room to force new block)
    // Consider DEFAULT_ARENA_ALIGNMENT. If small_block_size_ is 100 and alignment is 8,
    // allocating small_block_size_ - 7 might fill it if starting address is aligned.
    // This needs careful calculation based on how current_alloc_ptr starts.
    // Let's allocate something that *almost* fills it.
    void* p1 = arena.Allocate(small_block_size_ - (DEFAULT_ARENA_ALIGNMENT * 2));
    ASSERT_NE(p1, nullptr);
    ASSERT_EQ(arena.GetNumBlocksAllocated(), 1);
    size_t used_after_p1 = arena.GetTotalBytesUsed();

    // This allocation should not fit and trigger a new block
    void* p2 = arena.Allocate(DEFAULT_ARENA_ALIGNMENT * 3); // e.g., 24 bytes if alignment is 8
    ASSERT_NE(p2, nullptr);
    ASSERT_EQ(arena.GetNumBlocksAllocated(), 2) << "A new block should have been allocated.";
    ASSERT_EQ(arena.GetTotalBytesUsed(), used_after_p1 + (DEFAULT_ARENA_ALIGNMENT * 3));
}

TEST_F(ArenaTest, LargeAllocationExceedsInitialBlockSize) {
    Arena arena(small_block_size_);
    ASSERT_EQ(arena.GetNumBlocksAllocated(), 1);

    size_t large_alloc_size = small_block_size_ * 2; // Larger than initial block
    void* p = arena.Allocate(large_alloc_size);
    ASSERT_NE(p, nullptr);
    // The first block might be skipped entirely, or a new larger block allocated.
    // Your Arena::AllocateNewBlock logic determines the new block size.
    // It uses `num_bytes + (alignment - 1)`.
    ASSERT_EQ(arena.GetNumBlocksAllocated(), 2) << "A new block should have been made for the large allocation.";
    ASSERT_EQ(arena.GetTotalBytesUsed(), large_alloc_size);
}

// --- Object Creation with Arena::Create ---
TEST_F(ArenaTest, CreateSingleObject) {
    Arena arena(initial_block_size_);
    ASSERT_EQ(TestObj::constructor_calls, 0);

    TestObj* obj = arena.Create<TestObj>(1, 3.14, "test1");
    ASSERT_NE(obj, nullptr);
    ASSERT_TRUE(is_aligned(obj, alignof(TestObj)));
    ASSERT_EQ(TestObj::constructor_calls, 1);
    ASSERT_EQ(TestObj::destructor_calls, 0); // Arena doesn't call destructors by default

    ASSERT_EQ(obj->id, 1);
    ASSERT_DOUBLE_EQ(obj->data, 3.14);
    ASSERT_STREQ(obj->name, "test1");

    ASSERT_EQ(arena.GetTotalBytesUsed(), sizeof(TestObj));
}

TEST_F(ArenaTest, CreateMultipleObjects) {
    Arena arena(initial_block_size_);
    TestObj* obj1 = arena.Create<TestObj>(1, 1.0, "obj1");
    ASSERT_NE(obj1, nullptr);
    ASSERT_EQ(TestObj::constructor_calls, 1);
    ASSERT_EQ(arena.GetTotalBytesUsed(), sizeof(TestObj));

    TestObj* obj2 = arena.Create<TestObj>(2, 2.0, "obj2");
    ASSERT_NE(obj2, nullptr);
    ASSERT_EQ(TestObj::constructor_calls, 2);
    ASSERT_EQ(arena.GetTotalBytesUsed(), 2 * sizeof(TestObj)); // Assumes contiguous or minimal padding

    ASSERT_EQ(obj1->id, 1);
    ASSERT_EQ(obj2->id, 2);

    // Ensure obj2 is placed after obj1
    ASSERT_TRUE(reinterpret_cast<std::byte*>(obj2) >= reinterpret_cast<std::byte*>(obj1) + sizeof(TestObj));
}

// --- Edge Cases & Stress ---
TEST_F(ArenaTest, AllocateAllInitialMemoryThenNewBlock) {
    Arena arena(small_block_size_);
    // This test is tricky because exact available space depends on initial alignment
    // of the block start and DEFAULT_ARENA_ALIGNMENT.
    // We can try to allocate something close to the block size.
    // A more robust test would be to fill it with small allocations until a new block is needed.

    size_t total_allocated_in_first_block = 0;
    std::vector<void*> allocations;
    // Allocate small chunks until a new block is likely needed.
    // We can't know the exact capacity without knowing the start pointer's alignment.
    // This loop might allocate into the second block already if estimates are off.
    for (int i = 0; i < (small_block_size_ / DEFAULT_ARENA_ALIGNMENT) + 5; ++i) {
        if (arena.GetNumBlocksAllocated() > 1 && !allocations.empty()) {
             // New block triggered, check previous state
            ASSERT_GT(arena.GetTotalBytesUsed(), total_allocated_in_first_block);
            break;
        }
        void* p = arena.Allocate(DEFAULT_ARENA_ALIGNMENT);
        if (!p) break; // Should not happen in this controlled test unless OOM
        allocations.push_back(p);
        if (arena.GetNumBlocksAllocated() == 1) { // Still in the first block
            total_allocated_in_first_block += DEFAULT_ARENA_ALIGNMENT;
        }
    }
    ASSERT_GT(allocations.size(), 0);
    ASSERT_GE(arena.GetNumBlocksAllocated(), 1); // Should be 1 or 2.

    // Now, definitely trigger a new block if not already.
    if (arena.GetNumBlocksAllocated() == 1) {
        void* p_new_block = arena.Allocate(small_block_size_); // Allocate a large chunk
        ASSERT_NE(p_new_block, nullptr);
        ASSERT_EQ(arena.GetNumBlocksAllocated(), 2);
    }
}

// Reminder test for Valgrind
TEST_F(ArenaTest, CheckWithValgrind) {
    Arena arena(1024);
    void* p1 = arena.Allocate(100);
    TestObj* obj1 = arena.Create<TestObj>(1, 1.0, "valgrind_obj");
    void* p2 = arena.Allocate(200);
    (void)p1; (void)obj1; (void)p2; // Suppress unused variable warnings
    SUCCEED() << "Run these tests with Valgrind to check for memory leaks and errors.";
}

// Test for potential OOM (hard to trigger deterministically)
TEST_F(ArenaTest, ExtremelyLargeAllocationAttempt) {
    Arena arena(1024); // Small initial block
    // Attempt to allocate a huge amount of memory.
    // This might return nullptr if ::operator new(std::nothrow) fails.
    // Your current Arena::Allocate might crash if ::operator new fails (see bug note).
    // If Arena::Allocate is fixed, this should return nullptr.
    size_t ridiculously_large_size = std::numeric_limits<size_t>::max() / 2;
    if (std::numeric_limits<size_t>::max() / 2 < 1024*1024*1024 ) { // e.g. on 32-bit, max/2 might be allocatable
        ridiculously_large_size = (sizeof(void*) >= 8) ? 
                                  static_cast<size_t>(1024) * 1024 * 1024 * 50 :  // 50 GB for 64-bit
                                  static_cast<size_t>(1024) * 1024 * 1024 * 1;    // 1 GB for 32-bit (still large)
    }


    void* p = arena.Allocate(ridiculously_large_size);
    // EXPECT_EQ(p, nullptr) << "Extremely large allocation should ideally fail gracefully (return nullptr).";
    // If the Arena::Allocate bug is present, this test might crash instead of failing gracefully.
    // For now, we just check it doesn't crash immediately in common cases (OS might kill process for real OOM)
    if (p != nullptr) {
        // This would be surprising unless the system has vast amounts of memory and swap.
        // If it did allocate, let's check basic properties.
        ASSERT_EQ(arena.GetTotalBytesUsed(), ridiculously_large_size);
        ASSERT_GE(arena.GetNumBlocksAllocated(), 1); // Or 2 if initial block was smaller
    } else {
        // This is the more expected outcome for such a large request.
        ASSERT_EQ(p, nullptr);
        // TotalBytesUsed should not have increased if allocation failed.
        // (This depends on Arena implementation: does it add to total_bytes_used_ before failure?)
        // Current impl adds it *after* successful allocation.
        ASSERT_EQ(arena.GetTotalBytesUsed(), 0) << "TotalBytesUsed should be 0 if initial large alloc fails.";
    }
    // This test is more about observing behavior than strict pass/fail on nullptr,
    // due to system-dependent OOM.
    SUCCEED() << "ExtremelyLargeAllocationAttempt behavior depends on system memory and Arena OOM handling.";
}