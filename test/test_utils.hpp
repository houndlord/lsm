#ifndef TEST_UTILS_HPP
#define TEST_UTILS_HPP

#include <string>
#include <vector>
#include "slice.hpp" // Assuming Slice is needed by StringToSlice
#include "arena.hpp" // Assuming Arena is needed
#include "value.hpp" // For ValueTag in TestEntry

// Declare TestEntry if it's widely used, or define it here if simple enough
struct TestEntry {
    std::string key;
    std::string value;
    ValueTag tag;

    TestEntry(std::string k, std::string v, ValueTag t = ValueTag::kData);
    bool operator<(const TestEntry& other) const;
};

Slice StringToSlice(Arena& arena, const std::string& s);

// You can also move IsPointerDistinctFromBuffer here if used elsewhere
bool IsPointerDistinctFromBuffer(const void* ptr, const std::vector<char>& buffer);

#endif // TEST_UTILS_HPP
