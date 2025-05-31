#include "wal_reader.hpp"

WALReader::WALReader(std::string filename) : filename_(filename), is_open_(false) {};

