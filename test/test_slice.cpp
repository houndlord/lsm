#include "gtest/gtest.h"
#include "slice.hpp"
#include <string>
#include <vector>
#include <cstring>

// Helper to convert Slice to std::string for easy comparison/printing in tests
std::string SliceToString(const Slice& s) {
    if (s.empty()) {
        return "";
    }
    return std::string(reinterpret_cast<const char*>(s.data()), s.size());
}


TEST(SliceTest, DefaultConstructor) {
    Slice s;
    ASSERT_TRUE(s.empty());
    ASSERT_EQ(s.size(), 0);
    ASSERT_EQ(s.data(), nullptr);
}

TEST(SliceTest, CStringConstructor) {
    const char* cstr = "hello";
    Slice s(cstr);
    ASSERT_FALSE(s.empty());
    ASSERT_EQ(s.size(), 5);
    ASSERT_NE(s.data(), nullptr);
    ASSERT_EQ(std::memcmp(s.data(), cstr, 5), 0);
    ASSERT_EQ(SliceToString(s), "hello");
}

TEST(SliceTest, StdStringConstructor) {
    std::string str = "world";
    Slice s(str);
    ASSERT_FALSE(s.empty());
    ASSERT_EQ(s.size(), 5);
    ASSERT_NE(s.data(), nullptr);
    ASSERT_EQ(std::memcmp(s.data(), str.data(), 5), 0);
    ASSERT_EQ(SliceToString(s), "world");
}

TEST(SliceTest, StdVectorByteConstructor) {
    std::vector<std::byte> vec = {
        std::byte{'t'}, std::byte{'e'}, std::byte{'s'}, std::byte{'t'}
    };
    Slice s(vec);
    ASSERT_FALSE(s.empty());
    ASSERT_EQ(s.size(), 4);
    ASSERT_NE(s.data(), nullptr);
    ASSERT_EQ(std::memcmp(s.data(), vec.data(), 4), 0);
    ASSERT_EQ(SliceToString(s), "test");
}

TEST(SliceTest, PointerAndSizeConstructor) {
    const char* data = "data_ptr";
    size_t len = 8;
    Slice s(reinterpret_cast<const std::byte*>(data), len);
    ASSERT_FALSE(s.empty());
    ASSERT_EQ(s.size(), len);
    ASSERT_NE(s.data(), nullptr);
    ASSERT_EQ(std::memcmp(s.data(), data, len), 0);
    ASSERT_EQ(SliceToString(s), "data_ptr");
}

TEST(SliceTest, CopyConstructor) {
    std::string str = "copy me";
    Slice original(str);
    Slice copy(original);

    ASSERT_EQ(copy.size(), original.size());
    ASSERT_EQ(copy.data(), original.data()); // Shallow copy: same data pointer
    ASSERT_EQ(SliceToString(copy), "copy me");
}

TEST(SliceTest, MoveConstructor) {
    std::string str = "move me";
    Slice original(str);
    const std::byte* original_data_ptr = original.data();
    size_t original_size = original.size();

    Slice moved(std::move(original));

    ASSERT_EQ(moved.size(), original_size);
    ASSERT_EQ(moved.data(), original_data_ptr);
    ASSERT_EQ(SliceToString(moved), "move me");

    ASSERT_TRUE(original.empty()); // Assuming your move ctor nulls out the source
    ASSERT_EQ(original.data(), nullptr);
}


TEST(SliceTest, OperatorSquareBracketsAndAt) {
    std::string str = "abc";
    Slice s(str);

    ASSERT_TRUE(s.at(0).has_value());
    ASSERT_EQ(s.at(0).value().get(), std::byte{'a'});

    ASSERT_TRUE(s.at(2).has_value());
    ASSERT_EQ(s.at(2).value().get(), std::byte{'c'});

    ASSERT_FALSE(s.at(3).has_value()); // Out of bounds
    ASSERT_FALSE(s.at(100).has_value()); // Far out of bounds
}

TEST(SliceTest, EmptySliceAt) {
    Slice s;
    ASSERT_FALSE(s.at(0).has_value());
}
