cmake_minimum_required(VERSION 3.20)
project(LSMTree LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ... (your compiler flags and build type flags remain the same) ...
if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

if(CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_CLANG)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic -Wshadow -Wnon-virtual-dtor -Wold-style-cast -Wcast-align -Wunused -Woverloaded-virtual -Wconversion -Wsign-conversion -Wnull-dereference -Wdouble-promotion -Wformat=2")
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0")
else() # Release or other types
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG")
endif()

include(FetchContent)

# As per zstd documentation: https://github.com/facebook/zstd/blob/dev/build/cmake/README.md
# You might want to control static/shared build. ZSTD_BUILD_STATIC is ON by default if not a subproject.
# set(ZSTD_BUILD_STATIC ON) # Explicitly if you want to be sure
# set(ZSTD_BUILD_SHARED OFF) # Explicitly if you want to be sure

FetchContent_Declare(
    zstd
    URL "https://github.com/facebook/zstd/releases/download/v1.5.5/zstd-1.5.5.tar.gz"
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    SOURCE_SUBDIR build/cmake
)

FetchContent_MakeAvailable(zstd)

add_library(lsm_core
    slice.cpp
    slice.hpp
    arena.cpp
    arena.hpp
    skip_list.hpp
    skip_list.cpp
    sorted_table.hpp
    result.hpp
    result.cpp
    sstable_writer.hpp
    sstable_writer.cpp
    sstable_reader.hpp
    sstable_reader.cpp
    sstable_iterator.hpp
    sstable_iterator.cpp
    mem_table.hpp
    mem_table.cpp
    db.hpp
    db.cpp
)
target_include_directories(lsm_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# I used zstd build readme for instructions
target_link_libraries(lsm_core PUBLIC libzstd_static)

# Took this from there as well. Doc says it is needed only on mac/win, but I've run into problems without.
# In this moment I do not have clear picture why exctly this is needed, it took me some tyme to link local build of zstd.
target_include_directories(lsm_core PUBLIC ${zstd_SOURCE_DIR}/lib)


# --- Testing Subdirectory ---
option(BUILD_TESTS "Build tests for LSMTree" ON)
if(BUILD_TESTS)
  target_compile_definitions(lsm_core PUBLIC LSM_PROJECT_ENABLE_TESTING_HOOKS)
  enable_testing()
  add_subdirectory(test)
endif()