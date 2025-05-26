#include "gtest/gtest.h"
#include "sstable_writer.hpp"
#include "mem_table.hpp"
#include "arena.hpp"
#include "slice.hpp"
#include "value.hpp"
#include "result.hpp"
#include "zstd.h"
#include "test_utils.hpp" // Include the test utilities header

#include <fstream>
#include <vector>
#include <string>
#include <cstdio>
#include <algorithm>
#include <iostream>
#include <random>

// Helper function to generate a string of random bytes
std::string GenerateRandomString(size_t length) {
    std::mt19937 rng(std::random_device{}()); // Mersenne Twister RNG
    std::uniform_int_distribution<int> dist(0, 255);
    std::string random_string;
    random_string.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        random_string += static_cast<char>(dist(rng));
    }
    return random_string;
}

/*
Slice StringToSlice(Arena& arena, const std::string& s) {
    if (s.empty()) return Slice();
    void* buf = arena.Allocate(s.length());
    if (!buf) throw std::bad_alloc(); 
    std::memcpy(buf, s.data(), s.length());
    return Slice(static_cast<const std::byte*>(buf), s.length());
}   
*/
/*
uint32_t ReadLittleEndian32(const char* buffer) {
    return static_cast<uint32_t>(static_cast<unsigned char>(buffer[0])) |
           (static_cast<uint32_t>(static_cast<unsigned char>(buffer[1])) << 8) |
           (static_cast<uint32_t>(static_cast<unsigned char>(buffer[2])) << 16) |
           (static_cast<uint32_t>(static_cast<unsigned char>(buffer[3])) << 24);
}
*/
/*
struct TestEntry {
    std::string key;
    std::string value;
    ValueTag tag;

    TestEntry(std::string k, std::string v, ValueTag t = ValueTag::kData)
        : key(std::move(k)), value(std::move(v)), tag(t) {}

    // For sorting expected entries
    bool operator<(const TestEntry& other) const {
        return key < other.key;
    }
};
*/
void SSTableWriterTestsLinkerReference() { volatile int i = 0; (void)i; }

class SSTableWriterTest : public ::testing::Test {
protected:
    std::unique_ptr<Arena> arena_;
    std::string temp_filename_ = "test_sstable.dat";
    static constexpr size_t DEFAULT_TARGET_BLOCK_SIZE = 4096; 

    void SetUp() override {
        arena_ = std::make_unique<Arena>();
        std::remove(temp_filename_.c_str());
        SSTableWriterTestsLinkerReference(); 
    }

    void TearDown() override {
        std::remove(temp_filename_.c_str());
        arena_.reset();
    }

    std::unique_ptr<MemTable> CreateAndPopulateMemTable(const std::vector<TestEntry>& entries) {
        auto memtable = std::make_unique<MemTable>(*arena_); 
        for (const auto& entry : entries) {
            Slice key_slice = StringToSlice(*arena_, entry.key);
            if (entry.tag == ValueTag::kData) {
                Slice value_slice = StringToSlice(*arena_, entry.value);
                Result res = memtable->Put(key_slice, value_slice);
                EXPECT_TRUE(res.ok()) << "MemTable Put failed for key '" << entry.key << "': " << res.message();
            } else {
                Result res = memtable->Delete(key_slice);
                EXPECT_TRUE(res.ok()) << "MemTable Delete failed for key '" << entry.key << "': " << res.message();
            }
        }
        return memtable;
    }

    // expected_entries_in_block MUST be sorted by key for this verification to be correct
    void VerifyBlock(std::ifstream& file_stream,
                       const std::vector<TestEntry>& expected_entries_in_block, // MUST BE SORTED
                       bool writer_had_compression_enabled_for_this_test) { 
        ASSERT_TRUE(file_stream.good() && !file_stream.eof()) << "File stream is bad or EOF before reading block header.";

        char header_buf[9]; 
        file_stream.read(header_buf, 9);
        ASSERT_EQ(file_stream.gcount(), 9) << "Failed to read full block header (9 bytes).";
        // Do not assert good() immediately after read if gcount != 9, as that's the primary error.
        // If gcount == 9, then check good().
        if (file_stream.gcount() == 9) {
            ASSERT_TRUE(file_stream.good()) << "File stream is bad after reading block header, though 9 bytes were read.";
        }


        uint32_t uncompressed_size_from_header = ReadLittleEndian32(header_buf);
        uint32_t on_disk_size_from_header = ReadLittleEndian32(header_buf + 4);
        char compression_flag_from_header = header_buf[8];

        std::cout << "------------------------------------------------------------\n"
              << "VERIFY_BLOCK: STARTING\n"
              << "VERIFY_BLOCK: Header Uncompressed Size: " << uncompressed_size_from_header << "\n"
              << "VERIFY_BLOCK: Header On-Disk Size: " << on_disk_size_from_header << "\n"
              << "VERIFY_BLOCK: Header Compression Flag: " << static_cast<int>(compression_flag_from_header) << "\n"
              << "VERIFY_BLOCK: Writer was configured with compression_enabled: " << writer_had_compression_enabled_for_this_test << "\n";
    std::cout << "VERIFY_BLOCK: Expecting " << expected_entries_in_block.size() << " entries in this block:" << std::endl;
    size_t calculated_expected_size = 0;
    int temp_entry_idx = 0;
    for (const auto& entry : expected_entries_in_block) {
        size_t entry_s = 4 + entry.key.size() + 1 + 4 + (entry.tag == ValueTag::kData ? entry.value.size() : 0);
        calculated_expected_size += entry_s;
        std::cout << "  Expected Entry " << temp_entry_idx++ << ": Key='" << entry.key 
                  << "', Tag=" << static_cast<int>(entry.tag) 
                  << ", ValLen=" << (entry.tag == ValueTag::kData ? entry.value.size() : 0)
                  << ", ApproxSize=" << entry_s << std::endl;
    }
    std::cout << "VERIFY_BLOCK: Calculated total serialized size of expected entries: " << calculated_expected_size << std::endl;
        
        // std::cout << "[VerifyBlock] Read Header: Uncomp=" << uncompressed_size_from_header
        //           << ", OnDisk=" << on_disk_size_from_header
        //           << ", Flag=" << static_cast<int>(compression_flag_from_header)
        //           << ", WriterCompressionEnabled=" << writer_had_compression_enabled_for_this_test << std::endl;


        if (expected_entries_in_block.empty()){
             ASSERT_EQ(uncompressed_size_from_header, 0) << "Uncompressed size should be 0 for an empty expected block.";
             ASSERT_EQ(on_disk_size_from_header, 0) << "On-disk data payload size should be 0 for an empty expected block.";
             // Even an empty block (if written, though writer avoids it) would have a no-compression flag
             ASSERT_EQ(compression_flag_from_header, CompressionType::kNoCompression) << "Empty block should not be marked compressed.";
             return; 
        }
        
        if (!writer_had_compression_enabled_for_this_test) {
            ASSERT_EQ(compression_flag_from_header, CompressionType::kNoCompression) 
                << "Block is marked compressed, but writer was configured for no compression in this test.";
            ASSERT_EQ(uncompressed_size_from_header, on_disk_size_from_header) 
                << "Uncompressed and on-disk size should match if writer was configured for no compression.";
        } else { 
            if (compression_flag_from_header == CompressionType::kNoCompression) {
                 ASSERT_EQ(uncompressed_size_from_header, on_disk_size_from_header)
                    << "If stored uncompressed (despite compression being on), sizes should match.";
            } else {
                 ASSERT_EQ(compression_flag_from_header, CompressionType::kZstdCompressed)
                    << "Unexpected compression flag value. Expected ZstdCompressed.";
                 ASSERT_LE(on_disk_size_from_header, uncompressed_size_from_header) 
                    << "If block is marked ZSTD compressed, on_disk_size should be <= uncompressed_size.";
            }
        }

        std::vector<char> block_data_on_disk(on_disk_size_from_header);
        if (on_disk_size_from_header > 0) { 
            file_stream.read(block_data_on_disk.data(), on_disk_size_from_header);
            ASSERT_EQ(file_stream.gcount(), on_disk_size_from_header) << "Failed to read full block data payload.";
             if (file_stream.gcount() == on_disk_size_from_header) {
                ASSERT_TRUE(file_stream.good()) << "File stream is bad after reading block data payload.";
            }
        }

        std::vector<char> actual_block_content_buffer(uncompressed_size_from_header);

        if (compression_flag_from_header == CompressionType::kZstdCompressed) {
            ASSERT_TRUE(writer_had_compression_enabled_for_this_test) 
                << "Block is ZSTD compressed, but test expected no compression from writer.";
            ASSERT_GT(uncompressed_size_from_header, 0) << "Cannot decompress a zero-size uncompressed block if marked compressed.";
            // It's possible for on_disk_size to be 0 if uncompressed was 0 and compression was attempted,
            // but zstd_compressBound(0) is small, and compress(empty) is usually small non-zero.
            // Let's assume if flag is kZstdCompressed, on_disk_size must be > 0 unless uncompressed_size was also 0.
            // If uncompressed_size_from_header is > 0, then on_disk_size_from_header should also be > 0.
            if (uncompressed_size_from_header > 0) {
                 ASSERT_GT(on_disk_size_from_header, 0) << "If uncompressed > 0 and flag is Zstd, on_disk_size should be > 0.";
            }
            
            size_t decompressed_actual_size = ZSTD_decompress(
                actual_block_content_buffer.data(), uncompressed_size_from_header,
                block_data_on_disk.data(), on_disk_size_from_header);
            
            ASSERT_FALSE(ZSTD_isError(decompressed_actual_size))
                << "ZSTD decompression failed: " << ZSTD_getErrorName(decompressed_actual_size)
                << " (UncompH: " << uncompressed_size_from_header << ", OnDiskH: " << on_disk_size_from_header << ")";
            ASSERT_EQ(decompressed_actual_size, uncompressed_size_from_header) << "Decompressed size mismatch with header's uncompressed_size.";
        
        } else if (compression_flag_from_header == CompressionType::kNoCompression) {
            ASSERT_EQ(uncompressed_size_from_header, on_disk_size_from_header) 
                << "Sizes must match for uncompressed block type.";
            if (uncompressed_size_from_header > 0) {
                 actual_block_content_buffer.assign(block_data_on_disk.begin(), block_data_on_disk.end());
            }
        } else {
            FAIL() << "Unknown compression flag in block header: " << static_cast<int>(compression_flag_from_header);
        }

        size_t current_pos = 0;
        size_t entry_idx = 0;
        const char* current_block_ptr = actual_block_content_buffer.data();

        while (current_pos < uncompressed_size_from_header && entry_idx < expected_entries_in_block.size()) {
            ASSERT_LE(current_pos + 4, uncompressed_size_from_header) << "Not enough data for key length. Entry " << entry_idx;
            uint32_t key_len = ReadLittleEndian32(current_block_ptr + current_pos);
            current_pos += 4;

            ASSERT_LE(current_pos + key_len, uncompressed_size_from_header) << "Not enough data for key. Entry " << entry_idx << ", key_len: " << key_len << ", current_pos: " << current_pos << ", uncomp_size: " << uncompressed_size_from_header;
            std::string key_str(current_block_ptr + current_pos, key_len);
            current_pos += key_len;

            ASSERT_LE(current_pos + 1, uncompressed_size_from_header) << "Not enough data for value tag. Entry " << entry_idx;
            ValueTag tag = static_cast<ValueTag>(static_cast<unsigned char>(*(current_block_ptr + current_pos))); // Ensure char is treated as unsigned for enum
            current_pos += 1;

            ASSERT_LE(current_pos + 4, uncompressed_size_from_header) << "Not enough data for value length. Entry " << entry_idx;
            uint32_t value_len = ReadLittleEndian32(current_block_ptr + current_pos);
            current_pos += 4;

            std::string value_str;
            if (value_len > 0) {
                ASSERT_LE(current_pos + value_len, uncompressed_size_from_header) << "Not enough data for value. Entry " << entry_idx;
                value_str.assign(current_block_ptr + current_pos, value_len);
                current_pos += value_len;
            }
            
            const auto& expected = expected_entries_in_block[entry_idx];
            EXPECT_EQ(key_str, expected.key) << "Key mismatch at entry " << entry_idx;
            EXPECT_EQ(tag, expected.tag) << "Tag mismatch at entry " << entry_idx << " (parsed_tag_val=" << static_cast<int>(tag) << ", expected_tag_val=" << static_cast<int>(expected.tag) << ")";
            if (expected.tag == ValueTag::kData) {
                EXPECT_EQ(value_str, expected.value) << "Value mismatch at entry " << entry_idx;
            } else { 
                EXPECT_TRUE(value_str.empty()) << "Tombstone value should be empty. Entry " << entry_idx;
                EXPECT_EQ(value_len, 0) << "Tombstone value length should be 0. Entry " << entry_idx;
            }
            entry_idx++;
        }
        std::cout << "VERIFY_BLOCK: FINISHED PARSING ENTRIES\n"
              << "VERIFY_BLOCK: Parsed entry_idx = " << entry_idx << "\n"
              << "VERIFY_BLOCK: Final current_pos = " << current_pos << "\n"
              << "VERIFY_BLOCK: Uncompressed size from header was = " << uncompressed_size_from_header << std::endl;
        ASSERT_EQ(entry_idx, expected_entries_in_block.size()) << "Mismatch in number of entries parsed vs expected.";
        if (!expected_entries_in_block.empty()) {
             ASSERT_EQ(current_pos, uncompressed_size_from_header) << "Not all data in block consumed or too much data processed.";
        } else {
             ASSERT_EQ(uncompressed_size_from_header, 0) << "If no entries expected, uncompressed size from header should be 0.";
        }
    }
};

TEST_F(SSTableWriterTest, WriteEmptyMemTable) {
    auto memtable = CreateAndPopulateMemTable({});
    SSTableWriter writer(false, 1, DEFAULT_TARGET_BLOCK_SIZE); 
    Result init_res = writer.Init();
    ASSERT_TRUE(init_res.ok()) << "SSTableWriter Init failed: " << init_res.message();
    Result res = writer.WriteMemTableToFile(*memtable, temp_filename_);
    ASSERT_TRUE(res.ok()) << res.message();
    std::ifstream file(temp_filename_, std::ios::binary | std::ios::ate);
    ASSERT_TRUE(file.is_open());
    EXPECT_EQ(file.tellg(), 0) << "File should be empty for an empty memtable."; 
    file.close();
}

TEST_F(SSTableWriterTest, WriteSingleSmallBlockNoCompression) {
    std::vector<TestEntry> entries_to_put = {
        {"key2", "value2"}, 
        {"key1", "value1"} 
    };
    auto memtable = CreateAndPopulateMemTable(entries_to_put);
    
    std::vector<TestEntry> expected_sorted_entries = entries_to_put;
    std::sort(expected_sorted_entries.begin(), expected_sorted_entries.end());

    SSTableWriter writer(false, 1, DEFAULT_TARGET_BLOCK_SIZE); 
    Result init_res = writer.Init();
    ASSERT_TRUE(init_res.ok()) << "SSTableWriter Init failed: " << init_res.message();
    Result res = writer.WriteMemTableToFile(*memtable, temp_filename_);
    ASSERT_TRUE(res.ok()) << res.message();

    std::ifstream file(temp_filename_, std::ios::binary);
    ASSERT_TRUE(file.is_open());
    VerifyBlock(file, expected_sorted_entries, false); 
    
    file.peek(); 
    ASSERT_TRUE(file.eof()) << "Expected end of file after single block.";
    file.close();
}

TEST_F(SSTableWriterTest, WriteSingleSmallBlockWithCompression) {
    std::vector<TestEntry> entries_to_put = {
        {"compressible_key_aaaaaaaaaaaa", std::string(100, 'a')}, 
        {"another_compressible_key_bbbbb", std::string(200, 'b')}
    };
    auto memtable = CreateAndPopulateMemTable(entries_to_put);

    std::vector<TestEntry> expected_sorted_entries = entries_to_put;
    std::sort(expected_sorted_entries.begin(), expected_sorted_entries.end());
    // Expected order: "another...", then "compressible..."

    SSTableWriter writer(true, 3, DEFAULT_TARGET_BLOCK_SIZE); 
    Result init_res = writer.Init();
    ASSERT_TRUE(init_res.ok()) << "SSTableWriter Init failed: " << init_res.message();
    Result res = writer.WriteMemTableToFile(*memtable, temp_filename_);
    ASSERT_TRUE(res.ok()) << res.message();

    std::ifstream file(temp_filename_, std::ios::binary);
    ASSERT_TRUE(file.is_open());
    VerifyBlock(file, expected_sorted_entries, true); 
    
    file.peek();
    ASSERT_TRUE(file.eof()) << "Expected end of file after single block.";
    file.close();
}

TEST_F(SSTableWriterTest, WriteTombstone) {
    std::vector<TestEntry> entries_to_put = {
        {"key1", "value1_data", ValueTag::kData},
        {"key_to_delete", "", ValueTag::kTombstone}, // Value is empty for tombstone
        {"key3", "value3_data", ValueTag::kData}
    };
    auto memtable = CreateAndPopulateMemTable(entries_to_put);

    std::vector<TestEntry> expected_sorted_entries = entries_to_put;
    std::sort(expected_sorted_entries.begin(), expected_sorted_entries.end());
    // Expected sorted order: key1, key3, key_to_delete

    SSTableWriter writer(false, 1, DEFAULT_TARGET_BLOCK_SIZE); 
    Result init_res = writer.Init();
    ASSERT_TRUE(init_res.ok()) << "SSTableWriter Init failed: " << init_res.message();
    Result res = writer.WriteMemTableToFile(*memtable, temp_filename_);
    ASSERT_TRUE(res.ok()) << res.message();

    std::ifstream file(temp_filename_, std::ios::binary);
    ASSERT_TRUE(file.is_open());
    VerifyBlock(file, expected_sorted_entries, false); 
    
    file.peek();
    ASSERT_TRUE(file.eof()) << "Expected end of file after tombstone block.";
    file.close();
}

// Helper to calculate serialized size for WriteMultipleBlocks
size_t CalculateSerializedSize(const std::vector<TestEntry>& entries) {
    size_t total_size = 0;
    for(const auto& e : entries) {
        total_size += 4; // key_len
        total_size += e.key.size();
        total_size += 1; // tag
        total_size += 4; // value_len
        if (e.tag == ValueTag::kData) {
            total_size += e.value.size();
        }
    }
    return total_size;
}


TEST_F(SSTableWriterTest, WriteMultipleBlocks) {
    // Define entries that will be put into MemTable (unsorted)
    std::vector<TestEntry> entries_to_put = {
        {"gamma_key", std::string(30, 'g')}, // Belongs to block 1
        {"alpha_key", std::string(40, 'a')}, // Belongs to block 1
        {"delta_key", std::string(10, 'd')}, // Belongs to block 1
        {"zeta_key",  std::string(60, 'z')}, // Belongs to block 2
        {"beta_key",  std::string(50, 'b')}, // Belongs to block 1
        {"epsilon_key",std::string(20, 'e')}  // Belongs to block 2
    };
    auto memtable = CreateAndPopulateMemTable(entries_to_put);

    // Define what we expect in each block, IN SORTED ORDER
    std::vector<TestEntry> expected_block1_entries = {
        {"alpha_key", std::string(40, 'a'), ValueTag::kData},
        {"beta_key",  std::string(50, 'b'), ValueTag::kData},
        {"delta_key", std::string(10, 'd'), ValueTag::kData},
        {"epsilon_key",std::string(20, 'e'), ValueTag::kData}
    };
    std::vector<TestEntry> expected_block2_entries = {
        {"gamma_key", std::string(30, 'g'), ValueTag::kData},
        {"zeta_key",  std::string(60, 'z'), ValueTag::kData}
    };
    
    // Calculate actual serialized size of entries intended for the first block
    size_t block1_exact_serialized_size = CalculateSerializedSize(expected_block1_entries);
    
    // Set target_block_size to be exactly the size of the first block's content.
    // The writer will flush when current_data_block_buffer.size() >= target_block_size_.
    // So, after processing the last item of block1_entries, the size will be
    // block1_exact_serialized_size. The next item will make it >=, thus flushing.
    size_t test_target_block_size = block1_exact_serialized_size; 

    SSTableWriter writer(false, 1, test_target_block_size); 
    Result init_res = writer.Init();
    ASSERT_TRUE(init_res.ok()) << "SSTableWriter Init failed: " << init_res.message();
    Result res = writer.WriteMemTableToFile(*memtable, temp_filename_);
    ASSERT_TRUE(res.ok()) << res.message();

    std::ifstream file(temp_filename_, std::ios::binary);
    ASSERT_TRUE(file.is_open());

    VerifyBlock(file, expected_block1_entries, false); 
    ASSERT_TRUE(file.good() && !file.eof()) << "File ended prematurely after first block, or stream error.";
    VerifyBlock(file, expected_block2_entries, false); 
    
    file.peek();
    ASSERT_TRUE(file.eof()) << "Expected end of file after second block.";
    file.close();
}

TEST_F(SSTableWriterTest, CompressionFallbackForIncompressibleData) {
    // Create entries with random data, which is typically hard to compress
    std::vector<TestEntry> entries_to_put = {
        {"random_key1", GenerateRandomString(100)},
        {"random_key2", GenerateRandomString(150)} 
        // Add more if needed, but these two should be enough for a single block
        // if DEFAULT_TARGET_BLOCK_SIZE is large enough.
    };

    // Calculate the expected serialized size of these entries
    // This will be our uncompressed_size.
    size_t expected_uncompressed_payload_size = CalculateSerializedSize(entries_to_put);

    auto memtable = CreateAndPopulateMemTable(entries_to_put);
    
    // The writer will sort these, so we must sort our expectation for VerifyBlock
    std::vector<TestEntry> expected_sorted_entries = entries_to_put;
    std::sort(expected_sorted_entries.begin(), expected_sorted_entries.end());

    // Instantiate SSTableWriter with compression ENABLED
    SSTableWriter writer(true, 3, DEFAULT_TARGET_BLOCK_SIZE); // compression_enabled = true
    Result init_res = writer.Init();
    ASSERT_TRUE(init_res.ok()) << "SSTableWriter Init failed: " << init_res.message();

    Result write_res = writer.WriteMemTableToFile(*memtable, temp_filename_);
    ASSERT_TRUE(write_res.ok()) << "WriteMemTableToFile failed: " << write_res.message();

    std::ifstream file(temp_filename_, std::ios::binary);
    ASSERT_TRUE(file.is_open()) << "Failed to open SSTable file for verification.";
    
    // --- Direct Header Verification for this specific test's core expectation ---
    ASSERT_TRUE(file.good() && !file.eof()) << "File stream bad or EOF before reading header for incompressible test.";
    char header_buf[9]; 
    file.read(header_buf, 9);
    ASSERT_EQ(file.gcount(), 9) << "Failed to read full block header (9 bytes) for incompressible test.";
    if (file.gcount() != 9) return; // Stop if header read failed

    uint32_t uncompressed_size_from_header = ReadLittleEndian32(header_buf);
    uint32_t on_disk_size_from_header = ReadLittleEndian32(header_buf + 4);
    char compression_flag_from_header = header_buf[8];

    // std::cout << "[Incompressible Test] Header Read: Uncomp=" << uncompressed_size_from_header
    //           << ", OnDisk=" << on_disk_size_from_header
    //           << ", Flag=" << static_cast<int>(compression_flag_from_header) << std::endl;

    EXPECT_EQ(uncompressed_size_from_header, expected_uncompressed_payload_size)
        << "Header's uncompressed size does not match calculated size of original data.";
    
    // CRITICAL EXPECTATION: For incompressible data, writer should store it uncompressed.
    EXPECT_EQ(compression_flag_from_header, CompressionType::kNoCompression)
        << "Expected kNoCompression flag for incompressible data, but got " 
        << static_cast<int>(compression_flag_from_header);
    EXPECT_EQ(on_disk_size_from_header, uncompressed_size_from_header)
        << "For data stored uncompressed (due to incompressibility), on_disk_size should equal uncompressed_size.";

    // --- Full Block Verification using VerifyBlock ---
    // Reset stream to the beginning to let VerifyBlock read the header again
    file.seekg(0, std::ios::beg); 
    ASSERT_TRUE(file.good()) << "File stream bad after seekg(0).";

    // In VerifyBlock, the parameter 'writer_had_compression_enabled_for_this_test' is true.
    // VerifyBlock's internal logic should then check the compression_flag_from_header.
    // If flag is kNoCompression, it should assert that on_disk_size == uncompressed_size.
    // If flag is kZstdCompressed, it should assert on_disk_size <= uncompressed_size.
    VerifyBlock(file, expected_sorted_entries, true); // true: writer was configured with compression ON
                                                      // VerifyBlock will check the actual flag written.
    
    file.peek(); 
    ASSERT_TRUE(file.eof()) << "Expected end of file after block of incompressible data.";
    file.close();
}

TEST(SSTableWriterSimpleSuite, BasicAssertion) { 
    ASSERT_TRUE(true);
}