#include "slice.hpp"

#include <cstdint>

uint32_t CalculateCRC32(const char* data, size_t length);
uint32_t CalculateCRC32(const Slice& data1, const Slice& data2 = Slice(), const Slice& data3 = Slice());