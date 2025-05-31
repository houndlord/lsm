#include "result.hpp"
#include "slice.hpp"

#include <fstream>
#include <string>
#include <vector>

enum class WALRecordType : uint8_t {
    kInvalid = 0,     // Or kZeroType, for padding or uninitialized
    kFullRecord = 1,  // A complete Put or Delete operation
    kWriteOp = 2,     // Specific type for a Put
    kDeleteOp = 3,    // Specific type for a Delete
    // Potentially kFirstFragment, kMiddleFragment, kLastFragment for larger records later
};



struct WALReader {
 public:
  explicit WALReader(std::string filename);
  ~WALReader();

  Result Open();
  bool ReadRecord(Slice* key, Slice* value, WALRecordType type);
  Result Sync();
  bool IsOpen() const { return is_open_; }

 private:
  std::ifstream file_stream_;
  std::string filename_;
  bool is_open_;
  Result last_read_status_;
  std::vector<char> buf_;
};