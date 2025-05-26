include(FetchContent)

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip # TODO: more reliable opttion?
)

FetchContent_MakeAvailable(googletest)

add_executable(run_tests
    test_slice.cpp
    test_arena.cpp
    skip_list_test.cpp
    test_sstable_writer.cpp
    test_sstable_reader.cpp
    test_utils.cpp # Add the new test utility source file
)

target_link_libraries(run_tests
    PRIVATE
        GTest::gtest
        GTest::gtest_main
        lsm_core
)

include(GoogleTest)
gtest_add_tests(TARGET run_tests)

message(STATUS "Configuring tests...")
