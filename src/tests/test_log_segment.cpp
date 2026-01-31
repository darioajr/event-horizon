#include <gtest/gtest.h>
#include "../storage/log_segment.hpp"
#include <filesystem>
#include <vector>

namespace fs = std::filesystem;

class LogSegmentTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = "./test_data/log_segment_test";
        fs::remove_all(test_dir_);
        fs::create_directories(test_dir_);
    }
    
    void TearDown() override {
        fs::remove_all(test_dir_);
    }
    
    std::string test_dir_;
};

TEST_F(LogSegmentTest, CreateNewSegment) {
    LogSegment segment(test_dir_, 0);
    
    EXPECT_EQ(segment.get_base_offset(), 0);
    EXPECT_EQ(segment.get_next_offset(), 0);
    EXPECT_EQ(segment.size(), 0);
}

TEST_F(LogSegmentTest, AppendSingleRecord) {
    LogSegment segment(test_dir_, 0);
    
    Record record;
    record.key = "key1";
    record.value = {'h', 'e', 'l', 'l', 'o'};
    
    int64_t offset = segment.append(record);
    
    EXPECT_EQ(offset, 0);
    EXPECT_EQ(segment.get_next_offset(), 1);
    EXPECT_EQ(segment.size(), 1);
}

TEST_F(LogSegmentTest, AppendMultipleRecords) {
    LogSegment segment(test_dir_, 0);
    
    for (int i = 0; i < 100; ++i) {
        Record record;
        record.key = "key" + std::to_string(i);
        record.value = std::vector<uint8_t>(100, static_cast<uint8_t>(i));
        
        int64_t offset = segment.append(record);
        EXPECT_EQ(offset, i);
    }
    
    EXPECT_EQ(segment.size(), 100);
}

TEST_F(LogSegmentTest, ReadRecords) {
    LogSegment segment(test_dir_, 0);
    
    // Append records
    for (int i = 0; i < 10; ++i) {
        Record record;
        record.key = "key" + std::to_string(i);
        record.value = {'v', 'a', 'l', static_cast<uint8_t>('0' + i)};
        segment.append(record);
    }
    
    // Read from beginning
    auto records = segment.read(0, 5);
    EXPECT_EQ(records.size(), 5);
    EXPECT_EQ(records[0].key, "key0");
    EXPECT_EQ(records[4].key, "key4");
    
    // Read from middle
    records = segment.read(5, 5);
    EXPECT_EQ(records.size(), 5);
    EXPECT_EQ(records[0].key, "key5");
    EXPECT_EQ(records[0].offset, 5);
}

TEST_F(LogSegmentTest, ReadOutOfRange) {
    LogSegment segment(test_dir_, 0);
    
    Record record;
    record.key = "test";
    record.value = {'t', 'e', 's', 't'};
    segment.append(record);
    
    // Read beyond available records
    auto records = segment.read(100, 10);
    EXPECT_TRUE(records.empty());
}

TEST_F(LogSegmentTest, ContainsOffset) {
    LogSegment segment(test_dir_, 10);
    
    for (int i = 0; i < 5; ++i) {
        Record record;
        record.key = "key";
        record.value = {'v'};
        segment.append(record);
    }
    
    EXPECT_FALSE(segment.contains_offset(5));
    EXPECT_TRUE(segment.contains_offset(10));
    EXPECT_TRUE(segment.contains_offset(12));
    EXPECT_TRUE(segment.contains_offset(14));
    EXPECT_FALSE(segment.contains_offset(15));
}

TEST_F(LogSegmentTest, PersistenceAfterReopen) {
    // Create and write records
    {
        LogSegment segment(test_dir_, 0);
        for (int i = 0; i < 10; ++i) {
            Record record;
            record.key = "persistent_key" + std::to_string(i);
            record.value = {'d', 'a', 't', 'a'};
            segment.append(record);
        }
        segment.flush();
    }
    
    // Reopen and verify
    {
        LogSegment segment(test_dir_, 0);
        EXPECT_EQ(segment.get_next_offset(), 10);
        
        auto records = segment.read(0, 10);
        EXPECT_EQ(records.size(), 10);
        EXPECT_EQ(records[0].key, "persistent_key0");
        EXPECT_EQ(records[9].key, "persistent_key9");
    }
}

TEST_F(LogSegmentTest, LargeRecords) {
    LogSegment segment(test_dir_, 0);
    
    // Create a large record (1MB)
    Record record;
    record.key = "large_key";
    record.value = std::vector<uint8_t>(1024 * 1024, 0xAB);
    
    int64_t offset = segment.append(record);
    EXPECT_EQ(offset, 0);
    
    auto records = segment.read(0, 1);
    EXPECT_EQ(records.size(), 1);
    EXPECT_EQ(records[0].value.size(), 1024 * 1024);
    EXPECT_EQ(records[0].value[0], 0xAB);
}

TEST_F(LogSegmentTest, EmptyKeyAndValue) {
    LogSegment segment(test_dir_, 0);
    
    Record record;
    record.key = "";
    record.value = {};
    
    int64_t offset = segment.append(record);
    EXPECT_EQ(offset, 0);
    
    auto records = segment.read(0, 1);
    EXPECT_EQ(records.size(), 1);
    EXPECT_TRUE(records[0].key.empty());
    EXPECT_TRUE(records[0].value.empty());
}
