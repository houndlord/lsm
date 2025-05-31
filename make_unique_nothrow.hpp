
#include <memory>

template<typename T, typename... Args>
std::unique_ptr<T> make_unique_nothrow(Args&&... args) {
	// Attempt to allocate and construct the object using the nothrow version of new.
	// If allocation fails, raw_ptr will be nullptr.
	T* raw_ptr = new (std::nothrow) T(std::forward<Args>(args)...);

	// Wrap the raw pointer in a std::unique_ptr.
	// If raw_ptr is nullptr, this constructs an empty std::unique_ptr.
	return std::unique_ptr<T>(raw_ptr);
}