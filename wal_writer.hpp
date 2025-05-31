#include "wal_record_type.hpp"

#include "result.hpp"
#include "slice.hpp"

#include <fstream>
#include <string>
#include <vector>



struct WALWriter {
 public:
  explicit WALWriter(std::string filename);
  ~WALWriter();

  Result Open();
  Result AddRecord(const Slice& key, const Slice& value, WALRecordType type);
  Result Sync();
  bool IsOpen() const { return is_open_; }

 private:
  std::ofstream file_stream_;
  std::string filename_;
  bool is_open_;
  Result last_read_status_;
  std::vector<char> buf_;
};