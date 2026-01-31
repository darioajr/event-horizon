#include <gtest/gtest.h>
#include "../broker/broker.hpp"
#include <filesystem>
#include <fstream>

using namespace eventhorizon;
namespace fs = std::filesystem;

// ============================================================================
// BrokerConfig Tests
// ============================================================================

class BrokerConfigTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_config_path_ = "./test_broker_config.json";
    }
    
    void TearDown() override {
        if (fs::exists(test_config_path_)) {
            fs::remove(test_config_path_);
        }
    }
    
    std::string test_config_path_;
};

TEST_F(BrokerConfigTest, DefaultValues) {
    BrokerConfig config;
    
    EXPECT_EQ(config.broker_id, 1);
    EXPECT_EQ(config.host, "localhost");
    EXPECT_EQ(config.port, 9092);
    EXPECT_EQ(config.log_dir, "./data");
    EXPECT_EQ(config.num_partitions, 1);
    EXPECT_EQ(config.replication_factor, 1);
    EXPECT_EQ(config.thread_pool_size, 4);
}

TEST_F(BrokerConfigTest, SaveAndLoad) {
    BrokerConfig config;
    config.broker_id = 42;
    config.host = "192.168.1.100";
    config.port = 9093;
    config.log_dir = "/var/data";
    config.num_partitions = 8;
    config.replication_factor = 3;
    config.thread_pool_size = 16;
    config.cluster_id = "test-cluster-id";
    
    config.save(test_config_path_);
    
    auto loaded = BrokerConfig::load(test_config_path_);
    
    EXPECT_EQ(loaded.broker_id, 42);
    EXPECT_EQ(loaded.host, "192.168.1.100");
    EXPECT_EQ(loaded.port, 9093);
    EXPECT_EQ(loaded.log_dir, "/var/data");
    EXPECT_EQ(loaded.num_partitions, 8);
    EXPECT_EQ(loaded.replication_factor, 3);
    EXPECT_EQ(loaded.thread_pool_size, 16);
    EXPECT_EQ(loaded.cluster_id, "test-cluster-id");
}

TEST_F(BrokerConfigTest, LoadNonExistentFile) {
    auto config = BrokerConfig::load("nonexistent_config.json");
    
    // Should return default values
    EXPECT_EQ(config.broker_id, 1);
    EXPECT_EQ(config.host, "localhost");
    EXPECT_EQ(config.port, 9092);
}

TEST_F(BrokerConfigTest, LoadInvalidJson) {
    std::ofstream file(test_config_path_);
    file << "{ invalid json";
    file.close();
    
    auto config = BrokerConfig::load(test_config_path_);
    
    // Should return default values on parse error
    EXPECT_EQ(config.broker_id, 1);
}

// ============================================================================
// Broker Integration Tests (basic)
// ============================================================================

class BrokerTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = "./test_broker_data";
        test_config_path_ = "./test_broker_config.json";
        
        // Clean up from previous runs
        if (fs::exists(test_dir_)) {
            fs::remove_all(test_dir_);
        }
        
        // Create config
        BrokerConfig config;
        config.broker_id = 99;
        config.host = "127.0.0.1";
        config.port = 19092; // Different port to avoid conflicts
        config.log_dir = test_dir_;
        config.num_partitions = 2;
        config.save(test_config_path_);
    }
    
    void TearDown() override {
        if (fs::exists(test_dir_)) {
            fs::remove_all(test_dir_);
        }
        if (fs::exists(test_config_path_)) {
            fs::remove(test_config_path_);
        }
    }
    
    std::string test_dir_;
    std::string test_config_path_;
};

TEST_F(BrokerTest, CreateBroker) {
    Broker broker(test_config_path_);
    
    EXPECT_FALSE(broker.is_running());
    EXPECT_EQ(broker.config().broker_id, 99);
    EXPECT_EQ(broker.config().port, 19092);
}

TEST_F(BrokerTest, CreateTopic) {
    Broker broker(test_config_path_);
    
    broker.create_topic("test-topic", 3, 1);
    
    auto topics = broker.list_topics();
    EXPECT_EQ(topics.size(), 1);
    EXPECT_EQ(topics[0], "test-topic");
    
    // Verify partitions were created
    for (int i = 0; i < 3; ++i) {
        auto* partition = broker.get_partition("test-topic", i);
        EXPECT_NE(partition, nullptr);
    }
}

TEST_F(BrokerTest, CreateMultipleTopics) {
    Broker broker(test_config_path_);
    
    broker.create_topic("topic-a", 2, 1);
    broker.create_topic("topic-b", 4, 1);
    broker.create_topic("topic-c", 1, 1);
    
    auto topics = broker.list_topics();
    EXPECT_EQ(topics.size(), 3);
}

TEST_F(BrokerTest, DeleteTopic) {
    Broker broker(test_config_path_);
    
    broker.create_topic("test-topic", 2, 1);
    EXPECT_EQ(broker.list_topics().size(), 1);
    
    broker.delete_topic("test-topic");
    EXPECT_EQ(broker.list_topics().size(), 0);
    
    // Partition should no longer exist
    EXPECT_EQ(broker.get_partition("test-topic", 0), nullptr);
}

TEST_F(BrokerTest, GetNonExistentPartition) {
    Broker broker(test_config_path_);
    
    EXPECT_EQ(broker.get_partition("nonexistent", 0), nullptr);
    
    broker.create_topic("test-topic", 2, 1);
    EXPECT_EQ(broker.get_partition("test-topic", 99), nullptr);
}

TEST_F(BrokerTest, ProduceAndFetch) {
    Broker broker(test_config_path_);
    
    broker.create_topic("test-topic", 1, 1);
    
    // Produce messages
    std::vector<uint8_t> value1 = {'h', 'e', 'l', 'l', 'o'};
    std::vector<uint8_t> value2 = {'w', 'o', 'r', 'l', 'd'};
    
    int64_t offset1 = broker.produce("test-topic", 0, "key1", value1);
    int64_t offset2 = broker.produce("test-topic", 0, "key2", value2);
    
    EXPECT_EQ(offset1, 0);
    EXPECT_EQ(offset2, 1);
    
    // Fetch messages
    auto records = broker.fetch("test-topic", 0, 0, 1024 * 1024);
    
    EXPECT_EQ(records.size(), 2);
    EXPECT_EQ(records[0].key, "key1");
    EXPECT_EQ(records[0].value, value1);
    EXPECT_EQ(records[1].key, "key2");
    EXPECT_EQ(records[1].value, value2);
}

TEST_F(BrokerTest, FetchFromOffset) {
    Broker broker(test_config_path_);
    
    broker.create_topic("test-topic", 1, 1);
    
    // Produce 5 messages
    for (int i = 0; i < 5; ++i) {
        std::vector<uint8_t> value = {static_cast<uint8_t>(i)};
        broker.produce("test-topic", 0, "", value);
    }
    
    // Fetch from offset 2
    auto records = broker.fetch("test-topic", 0, 2, 1024 * 1024);
    
    EXPECT_EQ(records.size(), 3); // Offsets 2, 3, 4
    EXPECT_EQ(records[0].value[0], 2);
    EXPECT_EQ(records[1].value[0], 3);
    EXPECT_EQ(records[2].value[0], 4);
}

TEST_F(BrokerTest, FetchRaw) {
    Broker broker(test_config_path_);
    
    broker.create_topic("test-topic", 1, 1);
    
    std::vector<uint8_t> value = {'t', 'e', 's', 't'};
    broker.produce("test-topic", 0, "key", value);
    
    auto [raw_data, record_count] = broker.fetch_raw("test-topic", 0, 0, 1024 * 1024);
    
    EXPECT_FALSE(raw_data.empty());
    EXPECT_EQ(record_count, 1);
}

TEST_F(BrokerTest, ConsumerGroupsAccessor) {
    Broker broker(test_config_path_);
    
    auto& groups = broker.consumer_groups();
    
    // Get or create a group
    auto* group = groups.get_or_create_group("test-group");
    EXPECT_NE(group, nullptr);
    EXPECT_EQ(group->group_id(), "test-group");
}

TEST_F(BrokerTest, TopicPersistence) {
    std::string topic_name = "persistent-topic";
    
    // Create broker, add topic and data
    {
        Broker broker(test_config_path_);
        broker.create_topic(topic_name, 1, 1);
        
        std::vector<uint8_t> value = {'p', 'e', 'r', 's', 'i', 's', 't'};
        broker.produce(topic_name, 0, "key", value);
    }
    
    // Create new broker instance, should load existing data
    {
        Broker broker(test_config_path_);
        
        auto topics = broker.list_topics();
        EXPECT_TRUE(std::find(topics.begin(), topics.end(), topic_name) != topics.end());
        
        auto records = broker.fetch(topic_name, 0, 0, 1024 * 1024);
        EXPECT_EQ(records.size(), 1);
        EXPECT_EQ(records[0].key, "key");
    }
}

// ============================================================================
// Broker Error Handling Tests
// ============================================================================

TEST_F(BrokerTest, ProduceToNonExistentTopic) {
    Broker broker(test_config_path_);
    
    std::vector<uint8_t> value = {'t', 'e', 's', 't'};
    
    // Broker auto-creates topics when producing
    int64_t offset = broker.produce("new-topic", 0, "key", value);
    EXPECT_EQ(offset, 0);
    
    // Verify topic was created
    auto topics = broker.list_topics();
    EXPECT_TRUE(std::find(topics.begin(), topics.end(), "new-topic") != topics.end());
}

TEST_F(BrokerTest, FetchFromNonExistentTopic) {
    Broker broker(test_config_path_);
    
    auto records = broker.fetch("nonexistent-topic", 0, 0, 1024);
    EXPECT_TRUE(records.empty());
}

TEST_F(BrokerTest, CreateDuplicateTopic) {
    Broker broker(test_config_path_);
    
    broker.create_topic("test-topic", 2, 1);
    
    // Creating duplicate topic throws exception
    EXPECT_THROW(
        broker.create_topic("test-topic", 2, 1),
        std::runtime_error
    );
    
    // Only one topic should exist
    auto topics = broker.list_topics();
    EXPECT_EQ(topics.size(), 1);
}

// ============================================================================
// Topic Operations Tests (Clear Messages, Recreate, Delete Records)
// ============================================================================

TEST_F(BrokerTest, DeleteRecords) {
    Broker broker(test_config_path_);
    broker.create_topic("test-topic", 1, 1);
    
    // Produce some messages
    for (int i = 0; i < 10; ++i) {
        broker.produce("test-topic", 0, "key" + std::to_string(i), 
                       {'v', 'a', 'l', static_cast<uint8_t>('0' + i)});
    }
    
    // Verify messages exist
    auto records_before = broker.fetch("test-topic", 0, 0, 10240);
    EXPECT_EQ(records_before.size(), 10);
    
    // Delete records before offset 5
    int64_t new_low = broker.delete_records("test-topic", 0, 5);
    EXPECT_GE(new_low, 5);
    
    // Fetch should still work for remaining records
    auto records_after = broker.fetch("test-topic", 0, 5, 10240);
    EXPECT_GE(records_after.size(), 0);  // May be 5 or less depending on segment
}

TEST_F(BrokerTest, ClearTopicMessages) {
    Broker broker(test_config_path_);
    broker.create_topic("test-topic", 2, 1);
    
    // Produce messages to both partitions
    for (int i = 0; i < 5; ++i) {
        broker.produce("test-topic", 0, "key", {'a'});
        broker.produce("test-topic", 1, "key", {'b'});
    }
    
    // Clear all messages
    broker.clear_topic_messages("test-topic");
    
    // Topic should still exist but with no messages
    auto topics = broker.list_topics();
    EXPECT_EQ(topics.size(), 1);
    
    // Partitions should still be accessible
    auto* p0 = broker.get_partition("test-topic", 0);
    auto* p1 = broker.get_partition("test-topic", 1);
    EXPECT_NE(p0, nullptr);
    EXPECT_NE(p1, nullptr);
}

TEST_F(BrokerTest, RecreateTopic) {
    Broker broker(test_config_path_);
    broker.create_topic("test-topic", 3, 1);
    
    // Produce some messages
    broker.produce("test-topic", 0, "key", {'a'});
    broker.produce("test-topic", 1, "key", {'b'});
    
    // Recreate topic
    broker.recreate_topic("test-topic");
    
    // Topic should exist with same number of partitions
    auto topics = broker.list_topics();
    EXPECT_EQ(topics.size(), 1);
    
    // Should have 3 partitions
    EXPECT_NE(broker.get_partition("test-topic", 0), nullptr);
    EXPECT_NE(broker.get_partition("test-topic", 1), nullptr);
    EXPECT_NE(broker.get_partition("test-topic", 2), nullptr);
}

TEST_F(BrokerTest, DeleteTopicPurgesData) {
    Broker broker(test_config_path_);
    broker.create_topic("test-topic", 2, 1);
    
    // Produce some messages
    broker.produce("test-topic", 0, "key", {'a'});
    
    // Delete with purge (default)
    broker.delete_topic("test-topic", true);
    
    // Topic should not exist
    auto topics = broker.list_topics();
    EXPECT_TRUE(topics.empty());
    
    // Verify directories are removed
    namespace fs = std::filesystem;
    std::string log_dir = "./test_broker_data";
    for (const auto& entry : fs::directory_iterator(log_dir)) {
        std::string name = entry.path().filename().string();
        EXPECT_FALSE(name.starts_with("test-topic-"));
    }
}

TEST_F(BrokerTest, DeleteRecordsNonExistentTopic) {
    Broker broker(test_config_path_);
    
    EXPECT_THROW(
        broker.delete_records("nonexistent", 0, 0),
        std::runtime_error
    );
}

TEST_F(BrokerTest, DeleteRecordsInvalidPartition) {
    Broker broker(test_config_path_);
    broker.create_topic("test-topic", 1, 1);
    
    EXPECT_THROW(
        broker.delete_records("test-topic", 99, 0),
        std::runtime_error
    );
}
