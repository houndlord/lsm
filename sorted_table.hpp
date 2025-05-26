#ifndef SORTED_TABLE_H
#define SORTED_TABLE_H

#include "slice.hpp"
#include "result.hpp"
#include "value.hpp"

class SortedTableIterator; // Forward declaration

class SortedTableIterator;

class SortedTable {
public:
  virtual ~SortedTable() = default;

  virtual Result Put(const Slice& key, const Slice& value) = 0;
  
  // Get now returns a Result containing a ValueEntry on success (if found and not a tombstone),
  // or a Slice view if we want to hide ValueEntry from the direct Get result.
  virtual Result Get(const Slice& key) const = 0;

  virtual Result Delete(const Slice& key) = 0;

  virtual SortedTableIterator* NewIterator() const = 0;
  virtual size_t ApproximateMemoryUsage() const = 0;
};

class SortedTableIterator {
public:
  virtual ~SortedTableIterator() = default;

  virtual bool Valid() const = 0;
  virtual void SeekToFirst() = 0;
  virtual void Seek(const Slice& target) = 0;
  virtual void Next() = 0;

  virtual Slice key() const = 0;
  // value() now needs to indicate if it's a normal value or tombstone.
  // It could return ValueEntry, or two methods:
  // virtual Slice value_data() const; (asserts if not kTypeValue)
  // virtual EntryType entry_type() const;
  // For simplicity, let's make it return ValueEntry.
  virtual ValueEntry value() const = 0; 
  
  virtual Result status() const = 0;
};

#endif // SORTED_TABLE_H