#include "skip_list.hpp"
#include "value.hpp"
#include <cstddef>

#include <iostream>


// --- SkipList (Map-backed placeholder) ---

SkipList::SkipList(Arena& arena, int max_height, double probability)
	: arena_(arena), 
	  ignored_max_height_(max_height),         // Store but ignore
	  ignored_probability_(probability),       // Store but ignore
	  map_nodes_overhead_estimate_(0) {
	// The head_ node, current_max_level_, random_generator_, etc.
	// from a real SkipList are not needed for this map-based placeholder.
}

SkipList::~SkipList() {
	// Data for Slices in table_ is in arena_, managed externally.
	// std::map (table_) cleans up its own nodes.
}

Result SkipList::Put(const Slice& key_input, const Slice& value_input) {
	if (key_input.empty()) {
		return Result::InvalidArgument("Key cannot be empty for Put.");
	}
	std::byte* key_arena_ptr;
	// 1. Allocate and copy key data into arena
	if (key_input.size() == 0) {
		key_arena_ptr = nullptr;
	} else {
		void* key_mem_raw = arena_.Allocate(key_input.size(), alignof(std::byte));
		if (!key_mem_raw) {
			return Result::ArenaAllocationFail("Failed to allocate for key in Put.");
		}
		key_arena_ptr = static_cast<std::byte*>(key_mem_raw);
		std::memcpy(key_arena_ptr, key_input.data(), key_input.size());
	}
	Slice arena_key(key_arena_ptr, key_input.size());

	// 2. Allocate and copy value data into arena
	std::byte* value_arena_ptr;
	if (value_input.size() == 0) {
		value_arena_ptr = nullptr;
	} else {
		void* value_mem_raw = arena_.Allocate(value_input.size(), alignof(std::byte));
		if (!value_mem_raw) {
			return Result::ArenaAllocationFail("Failed to allocate for value in Put.");
		}
		value_arena_ptr = static_cast<std::byte*>(value_mem_raw);
		std::memcpy(value_arena_ptr, value_input.data(), value_input.size());
	}
	Slice arena_value_slice(value_arena_ptr, value_input.size());

	// 3. Create a ValueEntry for the normal value
	ValueEntry entry(arena_value_slice, ValueTag::kData);

	// 4. Insert or assign into the map
	auto [iter, inserted] = table_.insert_or_assign(arena_key, entry);

	if (inserted) {
		map_nodes_overhead_estimate_ += sizeof(void*) * 3 + sizeof(Slice) + sizeof(ValueEntry); // Approx
	} else {
		// If updated, estimate potential change in ValueEntry size if it varied significantly
		// For fixed size ValueEntry (Slice + enum), this is less of an issue.
	}
	return Result::OK();
}


Result SkipList::Get(const Slice& key) const {
	// Assuming 'table_' is your std::map<Slice, ValueEntry, ...>
	std::cout << "[SkipList::Get] Looking for key: " << key.ToString() << std::endl; // DEBUG
	auto it = table_.find(key);
	if (it == table_.end()) {
			std::cout << "[SkipList::Get] Key not found in map." << std::endl; // DEBUG
			// Key is not in the map at all.
			return Result::NotFound(key.ToString() + " (not in skiplist map)");
	}

	// Key is in the map, 'it->second' is a ValueEntry
	const ValueEntry& entry = it->second;
	std::cout << "[SkipList::Get] Key found in map. Entry type: " 
						<< (entry.IsTombstone() ? "TOMBSTONE" : "DATA") << std::endl; // DEBUG

	if (entry.IsTombstone()) {
			// It's a tombstone. Return an OK Result that indicates this.
			// The public DB::Get will interpret this as the key being "deleted".
			return Result::OkTombstone();
	} else {
			// It's actual data.
			return Result::OK(entry.value_slice); // This Result constructor sets value_tag_ to kData
		}
}

Result SkipList::Delete(const Slice& key_input) {
	if (key_input.empty()) {
		return Result::InvalidArgument("Key cannot be empty for Delete.");
	}

	// 1. Allocate and copy key data into arena (key is still needed for the tombstone)
	void* key_mem_raw = arena_.Allocate(key_input.size(), alignof(std::byte));
	if (!key_mem_raw) {
		return Result::ArenaAllocationFail("Failed to allocate for key in Delete.");
	}
	std::byte* key_arena_ptr = static_cast<std::byte*>(key_mem_raw);
	std::memcpy(key_arena_ptr, key_input.data(), key_input.size());
	Slice arena_key(key_arena_ptr, key_input.size());

	// 2. Create a ValueEntry representing a tombstone
	ValueEntry tombstone_entry(ValueTag::kTombstone); 
	// tombstone_entry.value_slice will be an empty Slice by its constructor

	// 3. Insert or assign the tombstone into the map
	auto [iter, inserted] = table_.insert_or_assign(arena_key, tombstone_entry);

	if (inserted) {
		map_nodes_overhead_estimate_ += sizeof(void*) * 3 + sizeof(Slice) + sizeof(ValueEntry); // Approx
	}
	// If an existing value was "overwritten" by a tombstone, or an old tombstone
	// was overwritten by a new one (idempotent).
	return Result::OK();
}


SortedTableIterator* SkipList::NewIterator() const {
	// The iterator object itself could also be allocated from the arena_
	// if the MemTable (or SkipList owner) wants to manage its memory.
	// return arena_.Create<SkipListIterator>(&table_); 
	return new SkipListIterator(&table_); // Caller owns this for now
}

size_t SkipList::ApproximateMemoryUsage() const {
	return arena_.GetTotalBytesUsed() + map_nodes_overhead_estimate_ + sizeof(*this);
}

//just wrap map iterator for now
SkipListIterator::SkipListIterator(const SkipList::InternalMapType* map_ptr)
	: map_ptr_(map_ptr), current_iter_(map_ptr_->end()) {}

bool SkipListIterator::Valid() const {
	return map_ptr_ && current_iter_ != map_ptr_->end();
}

void SkipListIterator::SeekToFirst() {
	if (map_ptr_) {
		current_iter_ = map_ptr_->begin();
	}
}

void SkipListIterator::Seek(const Slice& target) {
	if (map_ptr_) {
		current_iter_ = map_ptr_->lower_bound(target);
	}
}

void SkipListIterator::Next() {
	if (Valid()) {
		++current_iter_;
	}
}

Slice SkipListIterator::key() const {
	if (!Valid()) return Slice();
	return current_iter_->first;
}

ValueEntry SkipListIterator::value() const {
	if (!Valid()) {
		// Return a default/invalid ValueEntry, perhaps a tombstone or specific error type if ValueEntry supported it
		return ValueEntry(ValueTag::kTombstone); // Or some other convention
    }
	return current_iter_->second; // This is the ValueEntry stored in the map>
}

Result SkipListIterator::status() const {
	return Result::OK();
}
