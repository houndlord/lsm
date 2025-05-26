#include "test_utils.hpp"
#include <cstring> // For std::memcpy
#include <stdexcept> // For std::bad_alloc

TestEntry::TestEntry(std::string k, std::string v, ValueTag t)
    : key(std::move(k)), value(std::move(v)), tag(t) {}

bool TestEntry::operator<(const TestEntry& other) const {
    return key < other.key;
}

Slice StringToSlice(Arena& arena, const std::string& s) {
    if (s.empty()) return Slice();
    void* buf = arena.Allocate(s.length());
    if (!buf) {
        throw std::bad_alloc();
    }
    std::memcpy(buf, s.data(), s.length());
    return Slice(static_cast<const std::byte*>(buf), s.length());
}

bool IsPointerDistinctFromBuffer(const void* ptr, const std::vector<char>& buffer) {
    if (buffer.empty()) return true; // If buffer is empty, ptr can't be in it.
    if (ptr == nullptr) return true; // Nullptr is not in the buffer's data range.
    const char* p_char = static_cast<const char*>(ptr);
    const char* buffer_start = buffer.data();
    const char* buffer_end = buffer.data() + buffer.size();
    return (p_char < buffer_start || p_char >= buffer_end);
}
