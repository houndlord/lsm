#include "wal_writer.hpp"

WALWriter::WALWriter(std::string filename) : filename_(filename), is_open_(false) {};

Result WALWriter::Open() {
  
}