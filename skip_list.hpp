#ifndef SKIP_LIST_HPP
#define SKIP_LIST_HPP

#include <map>
#include <cstring>

#include "sorted_table.hpp"
#include "arena.hpp"
#include "result.hpp"
#include "value.hpp"


class SkipListIterator; // Forward declaration for the iterator (will wrap map iterator)

// Custom comparator for Slice keys in the std::map
struct SliceMapComparatorForSkipListPlaceholder { // Renamed for clarity
    bool operator()(const Slice& a, const Slice& b) const {
        // Ensure Slice has a compare method or implement full comparison here
        return a.compare(b) < 0; 
    }
};

// This class is named SkipList and has its expected API,
// but internally uses std::map as a placeholder.
class SkipList : public SortedTable {
 public:
  // Constructor signature matches a real SkipList,
  // but max_height and probability are ignored by this map-based backend.
  static constexpr int kDefaultMaxHeight = 12;
  static constexpr double kDefaultProbability = 0.25;

  explicit SkipList(Arena& arena, 
                    int max_height = kDefaultMaxHeight, // Ignored by this impl
                    double probability = kDefaultProbability); // Ignored by this impl

  ~SkipList() override;

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;
  SkipList(SkipList&&) = delete;
  SkipList& operator=(SkipList&&) = delete;

  // --- SortedTable Interface Implementation ---
  Result Put(const Slice& key, const Slice& value) override;
  Result Get(const Slice& key) const override;
  Result Delete(const Slice& key) override;
  SortedTableIterator* NewIterator() const override;
  size_t ApproximateMemoryUsage() const override;

 private:
  friend class SkipListIterator; // Grant access to internal map

  using InternalMapType = std::map<Slice, ValueEntry, SliceMapComparatorForSkipListPlaceholder>;
  
  Arena& arena_; // Reference to the arena where actual K/V data is stored
  InternalMapType table_; // The std::map used as the backing store

  // Parameters from constructor, stored but unused by this map implementation
  const int ignored_max_height_; 
  const double ignored_probability_;

  size_t map_nodes_overhead_estimate_; // Rough estimate for map's own structural overhead
};


// This iterator is named SkipListIterator for API consistency,
// but it wraps a std::map iterator for this placeholder implementation.
class SkipListIterator : public SortedTableIterator {
 public:
  explicit SkipListIterator(const SkipList::InternalMapType* map_ptr);
  ~SkipListIterator() override = default;

  bool Valid() const override;
  void SeekToFirst() override;
  void Seek(const Slice& target) override;
  void Next() override;

  Slice key() const override;
  ValueEntry value() const override;
  Result status() const override;

 private:
  const SkipList::InternalMapType* map_ptr_;
  SkipList::InternalMapType::const_iterator current_iter_;
};


#endif // SKIP_LIST_HPP