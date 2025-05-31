#include <cstdint>

enum class WALRecordType : uint8_t {
    kInvalid = 0,     // Or kZeroType, for padding or uninitialized
    kFullRecord = 1,  // A complete Put or Delete operation
    kWriteOp = 2,     // Specific type for a Put
    kDeleteOp = 3,    // Specific type for a Delete
    // Potentially kFirstFragment, kMiddleFragment, kLastFragment for larger records later
};