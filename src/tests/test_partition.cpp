#include <gtest/gtest.h>
#include "../storage/partition.hpp"
#include <filesystem>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

class PartitionTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = "./test_data/partition_test";
        fs::remove_all(test_dir_);
        fs::create_directories(test_dir_);
    }
    
    void TearDown() override {
        fs::remove_all(test_dir_);
    }
    
    std::string test_dir_;
};

TEST_F(PartitionTest, CreateNewPartition) {
    Partition partition("test-topic", 0, test_dir_);
    
    EXPECT_EQ(partition.get_topic(), "test-topic");
    EXPECT_EQ(partition.get_partition_id(), 0);
    EXPECT_EQ(partition.get_log_start_offset(), 0);
    EXPECT_EQ(partition.get_log_end_offset(), 0);
}

TEST_F(PartitionTest, ProduceSingleMessage) {
    Partition partition("test-topic", 0, test_dir_);
    
    std::vector<uint8_t> value = {'h', 'e', 'l', 'l', 'o'};
    int64_t offset = partition.produce("key1", value);
    
    EXPECT_EQ(offset, 0);
    EXPECT_EQ(partition.get_log_end_offset(), 1);
}

TEST_F(PartitionTest, ProduceMultipleMessages) {
    Partition partition("test-topic", 0, test_dir_);
    
    for (int i = 0; i < 100; ++i) {
        std::vector<uint8_t> value(100, static_cast<uint8_t>(i));
        int64_t offset = partition.produce("key" + std::to_string(i), value);
        EXPECT_EQ(offset, i);
    }
    
    EXPECT_EQ(partition.get_log_end_offset(), 100);
}

TEST_F(PartitionTest, FetchMessages) {
    Partition partition("test-topic", 0, test_dir_);
    
    // Produce messages
    for (int i = 0; i < 10; ++i) {
        std::vector<uint8_t> value = {'m', 's', 'g', static_cast<uint8_t>('0' + i)};
        partition.produce("key" + std::to_string(i), value);
    }
    
    // Fetch from beginning
    auto records = partition.fetch(0, 1024 * 1024);
    EXPECT_GE(records.size(), 1u);
    EXPECT_EQ(records[0].offset, 0);
    
    // Fetch from middle
    records = partition.fetch(5, 1024 * 1024);
    EXPECT_GE(records.size(), 1u);
    EXPECT_EQ(records[0].offset, 5);
}

TEST_F(PartitionTest, FetchOutOfRange) {
    Partition partition("test-topic", 0, test_dir_);
    
    std::vector<uint8_t> value = {'t', 'e', 's', 't'};
    partition.produce("key", value);
    
    // Fetch beyond available
    auto records = partition.fetch(100, 1024);
    EXPECT_TRUE(records.empty());
    
    // Fetch before start (negative conceptually)
    records = partition.fetch(-1, 1024);
    EXPECT_TRUE(records.empty());
}

TEST_F(PartitionTest, PersistenceAfterReopen) {
    // Create and produce
    {
        Partition partition("persistent-topic", 0, test_dir_);
        for (int i = 0; i < 50; ++i) {
            std::vector<uint8_t> value = {'d', 'a', 't', 'a'};
            partition.produce("key" + std::to_string(i), value);
        }
        partition.flush();
    }
    
    // Reopen and verify
    {
        Partition partition("persistent-topic", 0, test_dir_);
        EXPECT_EQ(partition.get_log_end_offset(), 50);
        
        auto records = partition.fetch(0, 1024 * 1024);
        EXPECT_GE(records.size(), 1u);
    }
}

TEST_F(PartitionTest, ConcurrentProduce) {
    Partition partition("concurrent-topic", 0, test_dir_);
    
    const int num_threads = 4;
    const int messages_per_thread = 100;
    
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&partition, t, messages_per_thread]() {
            for (int i = 0; i < messages_per_thread; ++i) {
                std::vector<uint8_t> value = {'v'};
                partition.produce("key-" + std::to_string(t) + "-" + std::to_string(i), value);
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    EXPECT_EQ(partition.get_log_end_offset(), num_threads * messages_per_thread);
}

TEST_F(PartitionTest, MultiplePartitionsSameTopic) {
    Partition partition0("multi-topic", 0, test_dir_);
    Partition partition1("multi-topic", 1, test_dir_);
    Partition partition2("multi-topic", 2, test_dir_);
    
    // Produce to each partition
    partition0.produce("k0", {'0'});
    partition1.produce("k1", {'1'});
    partition2.produce("k2", {'2'});
    
    EXPECT_EQ(partition0.get_log_end_offset(), 1);
    EXPECT_EQ(partition1.get_log_end_offset(), 1);
    EXPECT_EQ(partition2.get_log_end_offset(), 1);
    
    // Verify isolation
    auto records0 = partition0.fetch(0, 1024);
    auto records1 = partition1.fetch(0, 1024);
    
    EXPECT_EQ(records0[0].value[0], '0');
    EXPECT_EQ(records1[0].value[0], '1');
}

TEST_F(PartitionTest, LargeMessages) {
    Partition partition("large-msg-topic", 0, test_dir_);
    
    // 10MB message
    std::vector<uint8_t> large_value(10 * 1024 * 1024, 0xCD);
    int64_t offset = partition.produce("large-key", large_value);
    
    EXPECT_EQ(offset, 0);
    
    auto records = partition.fetch(0, 20 * 1024 * 1024);
    EXPECT_EQ(records.size(), 1u);
    EXPECT_EQ(records[0].value.size(), 10 * 1024 * 1024);
}

// ============================================================================
// Delete Records and Truncate Tests
// ============================================================================

TEST_F(PartitionTest, DeleteRecordsBefore) {
    Partition partition("delete-test", 0, test_dir_);
    
    // Produce 10 messages
    for (int i = 0; i < 10; ++i) {
        partition.produce("key" + std::to_string(i), 
                         {'v', static_cast<uint8_t>('0' + i)});
    }
    
    EXPECT_EQ(partition.get_log_end_offset(), 10);
    
    // Delete records before offset 5
    int64_t new_start = partition.delete_records_before(5);
    EXPECT_GE(new_start, 0);  // Should be at least 0
    
    // Log end offset should remain unchanged
    EXPECT_EQ(partition.get_log_end_offset(), 10);
}

TEST_F(PartitionTest, DeleteRecordsHighWatermark) {
    Partition partition("delete-hw-test", 0, test_dir_);
    
    // Produce messages
    for (int i = 0; i < 5; ++i) {
        partition.produce("key", {'v'});
    }
    
    // Delete with -1 (high watermark) should clear all
    int64_t new_start = partition.delete_records_before(-1);
    EXPECT_GE(new_start, 0);
}

TEST_F(PartitionTest, Truncate) {
    Partition partition("truncate-test", 0, test_dir_);
    
    // Produce 20 messages
    for (int i = 0; i < 20; ++i) {
        partition.produce("key", {'v'});
    }
    
    int64_t end_before = partition.get_log_end_offset();
    EXPECT_EQ(end_before, 20);
    
    // Truncate the partition
    partition.truncate();
    
    // Log start should equal old log end
    int64_t start_after = partition.get_log_start_offset();
    EXPECT_EQ(start_after, 20);
    
    // No messages should be fetchable
    auto records = partition.fetch(0, 10240);
    EXPECT_TRUE(records.empty());
    
    // New messages should start from offset 20
    int64_t new_offset = partition.produce("new", {'n'});
    EXPECT_EQ(new_offset, 20);
}

TEST_F(PartitionTest, TruncatePreservesPartition) {
    Partition partition("truncate-preserve", 0, test_dir_);
    
    partition.produce("key", {'v'});
    partition.truncate();
    
    // Should be able to produce new messages
    int64_t offset = partition.produce("new-key", {'n', 'e', 'w'});
    EXPECT_GE(offset, 1);
    
    // And fetch them
    auto records = partition.fetch(offset, 1024);
    EXPECT_EQ(records.size(), 1u);
    EXPECT_EQ(records[0].key, "new-key");
}
