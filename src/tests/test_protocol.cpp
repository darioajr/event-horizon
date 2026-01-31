#include <gtest/gtest.h>
#include "../protocol/kafka_protocol.hpp"
#include "../consumer/consumer_group.hpp"
#include <vector>
#include <string>

using namespace eventhorizon::protocol;
using namespace eventhorizon;

// ============================================================================
// BufferWriter/Reader Tests
// ============================================================================

class BufferTest : public ::testing::Test {};

TEST_F(BufferTest, WriteAndReadInt8) {
    BufferWriter writer;
    writer.write_int8(127);
    writer.write_int8(-128);
    writer.write_int8(0);
    
    BufferReader reader(writer.data());
    EXPECT_EQ(reader.read_int8(), 127);
    EXPECT_EQ(reader.read_int8(), -128);
    EXPECT_EQ(reader.read_int8(), 0);
}

TEST_F(BufferTest, WriteAndReadInt16) {
    BufferWriter writer;
    writer.write_int16(32767);
    writer.write_int16(-32768);
    writer.write_int16(0);
    
    BufferReader reader(writer.data());
    EXPECT_EQ(reader.read_int16(), 32767);
    EXPECT_EQ(reader.read_int16(), -32768);
    EXPECT_EQ(reader.read_int16(), 0);
}

TEST_F(BufferTest, WriteAndReadInt32) {
    BufferWriter writer;
    writer.write_int32(2147483647);
    writer.write_int32(-2147483648);
    writer.write_int32(0);
    writer.write_int32(123456789);
    
    BufferReader reader(writer.data());
    EXPECT_EQ(reader.read_int32(), 2147483647);
    EXPECT_EQ(reader.read_int32(), -2147483648);
    EXPECT_EQ(reader.read_int32(), 0);
    EXPECT_EQ(reader.read_int32(), 123456789);
}

TEST_F(BufferTest, WriteAndReadInt64) {
    BufferWriter writer;
    writer.write_int64(9223372036854775807LL);
    writer.write_int64(-9223372036854775807LL - 1);
    writer.write_int64(0);
    
    BufferReader reader(writer.data());
    EXPECT_EQ(reader.read_int64(), 9223372036854775807LL);
    EXPECT_EQ(reader.read_int64(), -9223372036854775807LL - 1);
    EXPECT_EQ(reader.read_int64(), 0);
}

TEST_F(BufferTest, WriteAndReadVarint) {
    BufferWriter writer;
    writer.write_varint(0);
    writer.write_varint(1);
    writer.write_varint(-1);
    writer.write_varint(300);
    writer.write_varint(-300);
    writer.write_varint(2147483647);
    writer.write_varint(-2147483648);
    
    BufferReader reader(writer.data());
    EXPECT_EQ(reader.read_varint(), 0);
    EXPECT_EQ(reader.read_varint(), 1);
    EXPECT_EQ(reader.read_varint(), -1);
    EXPECT_EQ(reader.read_varint(), 300);
    EXPECT_EQ(reader.read_varint(), -300);
    EXPECT_EQ(reader.read_varint(), 2147483647);
    EXPECT_EQ(reader.read_varint(), -2147483648);
}

TEST_F(BufferTest, WriteAndReadString) {
    BufferWriter writer;
    writer.write_string("hello");
    writer.write_string("");
    writer.write_string("hello world with special chars: éàü");
    
    BufferReader reader(writer.data());
    EXPECT_EQ(reader.read_string(), "hello");
    EXPECT_EQ(reader.read_string(), "");
    EXPECT_EQ(reader.read_string(), "hello world with special chars: éàü");
}

TEST_F(BufferTest, WriteAndReadNullableString) {
    BufferWriter writer;
    std::string str = "not null";
    writer.write_nullable_string(&str);
    writer.write_nullable_string(nullptr);
    
    BufferReader reader(writer.data());
    EXPECT_EQ(reader.read_nullable_string(), "not null");
    EXPECT_EQ(reader.read_nullable_string(), ""); // null returns empty
}

TEST_F(BufferTest, WriteAndReadBytes) {
    BufferWriter writer;
    std::vector<uint8_t> data = {0x01, 0x02, 0x03, 0xFF, 0x00};
    writer.write_bytes(data);
    
    BufferReader reader(writer.data());
    auto result = reader.read_bytes();
    EXPECT_EQ(result, data);
}

TEST_F(BufferTest, WriteAndReadBool) {
    BufferWriter writer;
    writer.write_bool(true);
    writer.write_bool(false);
    
    BufferReader reader(writer.data());
    EXPECT_TRUE(reader.read_bool());
    EXPECT_FALSE(reader.read_bool());
}

TEST_F(BufferTest, BigEndianByteOrder) {
    BufferWriter writer;
    writer.write_int32(0x12345678);
    
    auto& data = writer.data();
    EXPECT_EQ(data[0], 0x12);
    EXPECT_EQ(data[1], 0x34);
    EXPECT_EQ(data[2], 0x56);
    EXPECT_EQ(data[3], 0x78);
}

TEST_F(BufferTest, ReaderUnderflow) {
    BufferWriter writer;
    writer.write_int8(1);
    
    BufferReader reader(writer.data());
    reader.read_int8(); // OK
    
    EXPECT_THROW(reader.read_int8(), std::runtime_error);
}

TEST_F(BufferTest, ReaderSkip) {
    BufferWriter writer;
    writer.write_int32(1);
    writer.write_int32(2);
    writer.write_int32(3);
    
    BufferReader reader(writer.data());
    reader.skip(4);
    EXPECT_EQ(reader.read_int32(), 2);
}

// ============================================================================
// Request Header Tests
// ============================================================================

TEST_F(BufferTest, ParseRequestHeader) {
    BufferWriter writer;
    writer.write_int16(0);  // api_key (Produce)
    writer.write_int16(8);  // api_version
    writer.write_int32(12345);  // correlation_id
    writer.write_string("test-client");  // client_id
    
    BufferReader reader(writer.data());
    auto header = RequestHeader::parse(reader);
    
    EXPECT_EQ(header.api_key, ApiKey::Produce);
    EXPECT_EQ(header.api_version, 8);
    EXPECT_EQ(header.correlation_id, 12345);
    EXPECT_EQ(header.client_id, "test-client");
}

// ============================================================================
// Protocol Handler Tests
// ============================================================================

class ProtocolHandlerTest : public ::testing::Test {
protected:
    void SetUp() override {
        handler_.set_broker_info(0, "localhost", 9092);
        handler_.set_cluster_id("test-cluster");
        handler_.set_consumer_group_manager(&group_manager_);
    }
    
    ConsumerGroupManager group_manager_;
    KafkaProtocolHandler handler_;
    
    std::vector<uint8_t> make_request(ApiKey api_key, int16_t version, 
                                       int32_t correlation_id,
                                       const std::string& client_id) {
        BufferWriter writer;
        writer.write_int16(static_cast<int16_t>(api_key));
        writer.write_int16(version);
        writer.write_int32(correlation_id);
        writer.write_string(client_id);
        return writer.data();
    }
};

TEST_F(ProtocolHandlerTest, HandleApiVersionsRequest) {
    auto request = make_request(ApiKey::ApiVersions, 0, 1, "test-client");
    auto response = handler_.handle_request(request);
    
    EXPECT_FALSE(response.empty());
    
    BufferReader reader(response);
    int32_t correlation_id = reader.read_int32();
    int16_t error_code = reader.read_int16();
    
    EXPECT_EQ(correlation_id, 1);
    EXPECT_EQ(error_code, 0); // No error
}

TEST_F(ProtocolHandlerTest, HandleMetadataRequest) {
    // Build metadata request (v0)
    BufferWriter request_writer;
    request_writer.write_int16(static_cast<int16_t>(ApiKey::Metadata));
    request_writer.write_int16(0);  // version
    request_writer.write_int32(42); // correlation_id
    request_writer.write_string("test-client");
    request_writer.write_int32(0);  // empty topics array
    
    auto response = handler_.handle_request(request_writer.data());
    
    EXPECT_FALSE(response.empty());
    
    BufferReader reader(response);
    int32_t correlation_id = reader.read_int32();
    EXPECT_EQ(correlation_id, 42);
    
    // Broker count
    int32_t broker_count = reader.read_int32();
    EXPECT_EQ(broker_count, 1);
}

TEST_F(ProtocolHandlerTest, HandleHeartbeatRequest) {
    // First, create a group by joining
    BufferWriter join_writer;
    join_writer.write_int16(static_cast<int16_t>(ApiKey::JoinGroup));
    join_writer.write_int16(0); // version 0
    join_writer.write_int32(99);
    join_writer.write_string("consumer");
    join_writer.write_string("group-1");    // group_id
    join_writer.write_int32(10000);         // session_timeout_ms
    join_writer.write_string("");           // member_id (empty = generate)
    join_writer.write_string("consumer");   // protocol_type
    join_writer.write_int32(1);             // protocols count
    join_writer.write_string("range");      // protocol name
    join_writer.write_bytes({});            // protocol metadata
    
    auto join_response = handler_.handle_request(join_writer.data());
    BufferReader join_reader(join_response);
    join_reader.read_int32(); // correlation_id
    int16_t join_error = join_reader.read_int16();
    int32_t generation_id = join_reader.read_int32();
    std::string protocol_name = join_reader.read_string();
    std::string leader = join_reader.read_string();
    std::string member_id = join_reader.read_string();
    
    EXPECT_EQ(join_error, 0);
    EXPECT_EQ(generation_id, 1);
    EXPECT_FALSE(member_id.empty());
    
    // Now send heartbeat with correct member_id and generation
    BufferWriter request_writer;
    request_writer.write_int16(static_cast<int16_t>(ApiKey::Heartbeat));
    request_writer.write_int16(0);
    request_writer.write_int32(100);
    request_writer.write_string("consumer");
    request_writer.write_string("group-1");
    request_writer.write_int32(generation_id);
    request_writer.write_string(member_id);
    
    auto response = handler_.handle_request(request_writer.data());
    
    BufferReader reader(response);
    int32_t correlation_id = reader.read_int32();
    int16_t error_code = reader.read_int16();
    
    EXPECT_EQ(correlation_id, 100);
    // Error code should be 27 (REBALANCE_IN_PROGRESS) because we haven't synced yet
    // or 0 if the group is stable
    EXPECT_TRUE(error_code == 0 || error_code == 27);
}

TEST_F(ProtocolHandlerTest, CustomHandler) {
    bool handler_called = false;
    
    handler_.register_handler(ApiKey::Produce, 
        [&](const RequestHeader& header, BufferReader& reader, BufferWriter& writer) {
            handler_called = true;
            
            BufferWriter response;
            response.write_int32(header.correlation_id);
            response.write_int16(0); // no error
            return response.data();
        });
    
    auto request = make_request(ApiKey::Produce, 0, 999, "producer");
    auto response = handler_.handle_request(request);
    
    EXPECT_TRUE(handler_called);
    
    BufferReader reader(response);
    EXPECT_EQ(reader.read_int32(), 999);
    EXPECT_EQ(reader.read_int16(), 0);
}

TEST_F(ProtocolHandlerTest, UnsupportedApiVersion) {
    // Request with unknown API key
    BufferWriter request_writer;
    request_writer.write_int16(9999);  // Unknown API
    request_writer.write_int16(0);
    request_writer.write_int32(1);
    request_writer.write_string("client");
    
    auto response = handler_.handle_request(request_writer.data());
    
    // Should return error response
    EXPECT_FALSE(response.empty());
}

// ============================================================================
// Error Codes Test
// ============================================================================

TEST(ErrorCodesTest, ErrorCodeValues) {
    EXPECT_EQ(static_cast<int16_t>(ErrorCode::None), 0);
    EXPECT_EQ(static_cast<int16_t>(ErrorCode::Unknown), -1);
    EXPECT_EQ(static_cast<int16_t>(ErrorCode::OffsetOutOfRange), 1);
    EXPECT_EQ(static_cast<int16_t>(ErrorCode::UnknownTopicOrPartition), 3);
    EXPECT_EQ(static_cast<int16_t>(ErrorCode::LeaderNotAvailable), 5);
    EXPECT_EQ(static_cast<int16_t>(ErrorCode::IllegalGeneration), 22);
    EXPECT_EQ(static_cast<int16_t>(ErrorCode::InconsistentGroupProtocol), 23);
    EXPECT_EQ(static_cast<int16_t>(ErrorCode::InvalidGroupId), 24);
    EXPECT_EQ(static_cast<int16_t>(ErrorCode::UnknownMemberId), 25);
    EXPECT_EQ(static_cast<int16_t>(ErrorCode::RebalanceInProgress), 27);
}

// ============================================================================
// API Key Test
// ============================================================================

TEST(ApiKeyTest, ApiKeyValues) {
    EXPECT_EQ(static_cast<int16_t>(ApiKey::Produce), 0);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::Fetch), 1);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::ListOffsets), 2);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::Metadata), 3);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::OffsetCommit), 8);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::OffsetFetch), 9);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::FindCoordinator), 10);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::JoinGroup), 11);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::Heartbeat), 12);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::LeaveGroup), 13);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::SyncGroup), 14);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::DescribeGroups), 15);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::ListGroups), 16);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::ApiVersions), 18);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::CreateTopics), 19);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::DeleteTopics), 20);
}

// ============================================================================
// Additional Buffer Tests
// ============================================================================

TEST_F(BufferTest, WriteAndReadUnsignedVarint) {
    BufferWriter writer;
    writer.write_unsigned_varint(0);
    writer.write_unsigned_varint(1);
    writer.write_unsigned_varint(127);
    writer.write_unsigned_varint(128);
    writer.write_unsigned_varint(16383);
    writer.write_unsigned_varint(16384);
    writer.write_unsigned_varint(2097151);
    
    BufferReader reader(writer.data());
    EXPECT_EQ(reader.read_unsigned_varint(), 0u);
    EXPECT_EQ(reader.read_unsigned_varint(), 1u);
    EXPECT_EQ(reader.read_unsigned_varint(), 127u);
    EXPECT_EQ(reader.read_unsigned_varint(), 128u);
    EXPECT_EQ(reader.read_unsigned_varint(), 16383u);
    EXPECT_EQ(reader.read_unsigned_varint(), 16384u);
    EXPECT_EQ(reader.read_unsigned_varint(), 2097151u);
}

TEST_F(BufferTest, WriteAndReadCompactString) {
    BufferWriter writer;
    writer.write_compact_string("hello");
    writer.write_compact_string("");
    writer.write_compact_string("longer string with spaces");
    
    BufferReader reader(writer.data());
    EXPECT_EQ(reader.read_compact_string(), "hello");
    EXPECT_EQ(reader.read_compact_string(), "");
    EXPECT_EQ(reader.read_compact_string(), "longer string with spaces");
}

TEST_F(BufferTest, WriteAndReadVarlong) {
    BufferWriter writer;
    writer.write_varlong(0);
    writer.write_varlong(1);
    writer.write_varlong(-1);
    writer.write_varlong(9223372036854775807LL);
    writer.write_varlong(-9223372036854775807LL - 1);
    
    BufferReader reader(writer.data());
    EXPECT_EQ(reader.read_varlong(), 0);
    EXPECT_EQ(reader.read_varlong(), 1);
    EXPECT_EQ(reader.read_varlong(), -1);
    EXPECT_EQ(reader.read_varlong(), 9223372036854775807LL);
    EXPECT_EQ(reader.read_varlong(), -9223372036854775807LL - 1);
}

TEST_F(BufferTest, WriteAndReadUnsignedIntegers) {
    BufferWriter writer;
    writer.write_uint8(255);
    writer.write_uint16(65535);
    writer.write_uint32(4294967295u);
    writer.write_uint64(18446744073709551615ull);
    
    BufferReader reader(writer.data());
    EXPECT_EQ(reader.read_uint8(), 255);
    EXPECT_EQ(reader.read_uint16(), 65535);
    EXPECT_EQ(reader.read_uint32(), 4294967295u);
    EXPECT_EQ(reader.read_uint64(), 18446744073709551615ull);
}

TEST_F(BufferTest, WriteRaw) {
    BufferWriter writer;
    std::vector<uint8_t> raw = {0x01, 0x02, 0x03, 0x04};
    writer.write_raw(raw.data(), raw.size());
    
    EXPECT_EQ(writer.data(), raw);
}

TEST_F(BufferTest, WriteNullableBytes) {
    BufferWriter writer;
    std::vector<uint8_t> data = {0x01, 0x02, 0x03};
    writer.write_nullable_bytes(&data);
    writer.write_nullable_bytes(nullptr);
    
    BufferReader reader(writer.data());
    auto result1 = reader.read_nullable_bytes();
    EXPECT_EQ(result1, data);
    
    auto result2 = reader.read_nullable_bytes();
    EXPECT_TRUE(result2.empty());
}

TEST_F(BufferTest, BufferWriterReserve) {
    BufferWriter writer(1024);
    
    // Write data
    for (int i = 0; i < 100; ++i) {
        writer.write_int32(i);
    }
    
    EXPECT_EQ(writer.size(), 400);
}

TEST_F(BufferTest, BufferWriterClear) {
    BufferWriter writer;
    writer.write_int32(42);
    EXPECT_EQ(writer.size(), 4);
    
    writer.clear();
    EXPECT_EQ(writer.size(), 0);
    EXPECT_TRUE(writer.data().empty());
}

TEST_F(BufferTest, ReaderRemaining) {
    BufferWriter writer;
    writer.write_int32(1);
    writer.write_int32(2);
    
    BufferReader reader(writer.data());
    EXPECT_EQ(reader.remaining(), 8);
    
    reader.read_int32();
    EXPECT_EQ(reader.remaining(), 4);
    
    reader.read_int32();
    EXPECT_EQ(reader.remaining(), 0);
}

TEST_F(BufferTest, ReaderPosition) {
    BufferWriter writer;
    writer.write_int32(1);
    writer.write_int32(2);
    
    BufferReader reader(writer.data());
    EXPECT_EQ(reader.position(), 0);
    
    reader.skip(4);
    EXPECT_EQ(reader.position(), 4);
}

TEST_F(BufferTest, ReadBytesAsString) {
    BufferWriter writer;
    writer.write_raw(reinterpret_cast<const uint8_t*>("hello"), 5);
    
    BufferReader reader(writer.data());
    std::string result = reader.read_bytes_as_string(5);
    EXPECT_EQ(result, "hello");
}

// ============================================================================
// Additional Protocol Handler Tests
// ============================================================================

TEST_F(ProtocolHandlerTest, HandleFindCoordinatorRequest) {
    BufferWriter request_writer;
    request_writer.write_int16(static_cast<int16_t>(ApiKey::FindCoordinator));
    request_writer.write_int16(0);  // version 0
    request_writer.write_int32(200);
    request_writer.write_string("client");
    request_writer.write_string("test-group");  // key (group_id)
    
    auto response = handler_.handle_request(request_writer.data());
    EXPECT_FALSE(response.empty());
    
    BufferReader reader(response);
    EXPECT_EQ(reader.read_int32(), 200); // correlation_id
    int16_t error = reader.read_int16();
    EXPECT_EQ(error, 0); // No error
}

TEST_F(ProtocolHandlerTest, HandleJoinGroupRequest) {
    BufferWriter request_writer;
    request_writer.write_int16(static_cast<int16_t>(ApiKey::JoinGroup));
    request_writer.write_int16(0);  // version 0
    request_writer.write_int32(300);
    request_writer.write_string("consumer");
    request_writer.write_string("test-group");
    request_writer.write_int32(10000);      // session_timeout
    request_writer.write_string("");        // member_id
    request_writer.write_string("consumer");// protocol_type
    request_writer.write_int32(1);          // protocols count
    request_writer.write_string("range");   // protocol name
    request_writer.write_bytes({});         // protocol metadata
    
    auto response = handler_.handle_request(request_writer.data());
    EXPECT_FALSE(response.empty());
    
    BufferReader reader(response);
    EXPECT_EQ(reader.read_int32(), 300); // correlation_id
    int16_t error = reader.read_int16();
    EXPECT_EQ(error, 0); // No error
    
    int32_t generation = reader.read_int32();
    EXPECT_EQ(generation, 1);
    
    std::string protocol = reader.read_string();
    EXPECT_EQ(protocol, "range");
}

TEST_F(ProtocolHandlerTest, HandleSyncGroupRequest) {
    // First join
    BufferWriter join_writer;
    join_writer.write_int16(static_cast<int16_t>(ApiKey::JoinGroup));
    join_writer.write_int16(0);
    join_writer.write_int32(400);
    join_writer.write_string("consumer");
    join_writer.write_string("sync-group");
    join_writer.write_int32(10000);
    join_writer.write_string("");
    join_writer.write_string("consumer");
    join_writer.write_int32(1);
    join_writer.write_string("range");
    join_writer.write_bytes({});
    
    auto join_response = handler_.handle_request(join_writer.data());
    BufferReader join_reader(join_response);
    join_reader.read_int32(); // correlation_id
    join_reader.read_int16(); // error
    int32_t generation = join_reader.read_int32();
    join_reader.read_string(); // protocol
    join_reader.read_string(); // leader
    std::string member_id = join_reader.read_string();
    
    // Now sync
    BufferWriter sync_writer;
    sync_writer.write_int16(static_cast<int16_t>(ApiKey::SyncGroup));
    sync_writer.write_int16(0);
    sync_writer.write_int32(401);
    sync_writer.write_string("consumer");
    sync_writer.write_string("sync-group");
    sync_writer.write_int32(generation);
    sync_writer.write_string(member_id);
    sync_writer.write_int32(1);  // assignments count
    sync_writer.write_string(member_id);
    sync_writer.write_bytes({0x01, 0x02, 0x03});
    
    auto sync_response = handler_.handle_request(sync_writer.data());
    EXPECT_FALSE(sync_response.empty());
    
    BufferReader sync_reader(sync_response);
    EXPECT_EQ(sync_reader.read_int32(), 401);
    int16_t sync_error = sync_reader.read_int16();
    EXPECT_EQ(sync_error, 0);
}

TEST_F(ProtocolHandlerTest, HandleLeaveGroupRequest) {
    // First join
    BufferWriter join_writer;
    join_writer.write_int16(static_cast<int16_t>(ApiKey::JoinGroup));
    join_writer.write_int16(0);
    join_writer.write_int32(500);
    join_writer.write_string("consumer");
    join_writer.write_string("leave-group");
    join_writer.write_int32(10000);
    join_writer.write_string("");
    join_writer.write_string("consumer");
    join_writer.write_int32(1);
    join_writer.write_string("range");
    join_writer.write_bytes({});
    
    auto join_response = handler_.handle_request(join_writer.data());
    BufferReader join_reader(join_response);
    join_reader.read_int32();
    join_reader.read_int16();
    join_reader.read_int32();
    join_reader.read_string();
    join_reader.read_string();
    std::string member_id = join_reader.read_string();
    
    // Leave group
    BufferWriter leave_writer;
    leave_writer.write_int16(static_cast<int16_t>(ApiKey::LeaveGroup));
    leave_writer.write_int16(0);
    leave_writer.write_int32(501);
    leave_writer.write_string("consumer");
    leave_writer.write_string("leave-group");
    leave_writer.write_string(member_id);
    
    auto leave_response = handler_.handle_request(leave_writer.data());
    EXPECT_FALSE(leave_response.empty());
    
    BufferReader leave_reader(leave_response);
    EXPECT_EQ(leave_reader.read_int32(), 501);
    int16_t leave_error = leave_reader.read_int16();
    EXPECT_EQ(leave_error, 0);
}

TEST_F(ProtocolHandlerTest, HandleOffsetCommitRequest) {
    BufferWriter request_writer;
    request_writer.write_int16(static_cast<int16_t>(ApiKey::OffsetCommit));
    request_writer.write_int16(0);  // version 0
    request_writer.write_int32(600);
    request_writer.write_string("consumer");
    request_writer.write_string("commit-group");
    request_writer.write_int32(1);  // topics count
    request_writer.write_string("test-topic");
    request_writer.write_int32(1);  // partitions count
    request_writer.write_int32(0);  // partition
    request_writer.write_int64(100);// offset
    request_writer.write_string(""); // metadata
    
    auto response = handler_.handle_request(request_writer.data());
    EXPECT_FALSE(response.empty());
    
    BufferReader reader(response);
    EXPECT_EQ(reader.read_int32(), 600);
}

TEST_F(ProtocolHandlerTest, HandleOffsetFetchRequest) {
    // First commit an offset
    BufferWriter commit_writer;
    commit_writer.write_int16(static_cast<int16_t>(ApiKey::OffsetCommit));
    commit_writer.write_int16(0);
    commit_writer.write_int32(700);
    commit_writer.write_string("consumer");
    commit_writer.write_string("fetch-group");
    commit_writer.write_int32(1);
    commit_writer.write_string("test-topic");
    commit_writer.write_int32(1);
    commit_writer.write_int32(0);
    commit_writer.write_int64(42);
    commit_writer.write_string("test-metadata");
    
    handler_.handle_request(commit_writer.data());
    
    // Now fetch
    BufferWriter fetch_writer;
    fetch_writer.write_int16(static_cast<int16_t>(ApiKey::OffsetFetch));
    fetch_writer.write_int16(0);  // version 0
    fetch_writer.write_int32(701);
    fetch_writer.write_string("consumer");
    fetch_writer.write_string("fetch-group");
    fetch_writer.write_int32(1);  // topics count
    fetch_writer.write_string("test-topic");
    fetch_writer.write_int32(1);  // partitions count
    fetch_writer.write_int32(0);  // partition
    
    auto response = handler_.handle_request(fetch_writer.data());
    EXPECT_FALSE(response.empty());
    
    BufferReader reader(response);
    EXPECT_EQ(reader.read_int32(), 701);
}

TEST_F(ProtocolHandlerTest, HandleListGroupsRequest) {
    // Create a group first
    BufferWriter join_writer;
    join_writer.write_int16(static_cast<int16_t>(ApiKey::JoinGroup));
    join_writer.write_int16(0);
    join_writer.write_int32(800);
    join_writer.write_string("consumer");
    join_writer.write_string("list-group");
    join_writer.write_int32(10000);
    join_writer.write_string("");
    join_writer.write_string("consumer");
    join_writer.write_int32(1);
    join_writer.write_string("range");
    join_writer.write_bytes({});
    
    handler_.handle_request(join_writer.data());
    
    // List groups (v0 - non-flexible)
    BufferWriter list_writer;
    list_writer.write_int16(static_cast<int16_t>(ApiKey::ListGroups));
    list_writer.write_int16(0);  // version 0
    list_writer.write_int32(801);
    list_writer.write_string("admin");
    
    auto response = handler_.handle_request(list_writer.data());
    EXPECT_FALSE(response.empty());
    
    BufferReader reader(response);
    EXPECT_EQ(reader.read_int32(), 801);
}

TEST_F(ProtocolHandlerTest, HandleDescribeGroupsRequest) {
    // Create a group first
    BufferWriter join_writer;
    join_writer.write_int16(static_cast<int16_t>(ApiKey::JoinGroup));
    join_writer.write_int16(0);
    join_writer.write_int32(900);
    join_writer.write_string("consumer");
    join_writer.write_string("describe-group");
    join_writer.write_int32(10000);
    join_writer.write_string("");
    join_writer.write_string("consumer");
    join_writer.write_int32(1);
    join_writer.write_string("range");
    join_writer.write_bytes({});
    
    handler_.handle_request(join_writer.data());
    
    // Describe groups (v0 - non-flexible)
    BufferWriter describe_writer;
    describe_writer.write_int16(static_cast<int16_t>(ApiKey::DescribeGroups));
    describe_writer.write_int16(0);  // version 0
    describe_writer.write_int32(901);
    describe_writer.write_string("admin");
    describe_writer.write_int32(1);  // groups count
    describe_writer.write_string("describe-group");
    
    auto response = handler_.handle_request(describe_writer.data());
    EXPECT_FALSE(response.empty());
    
    BufferReader reader(response);
    EXPECT_EQ(reader.read_int32(), 901);
}

TEST_F(ProtocolHandlerTest, SetTopicsCallback) {
    std::vector<TopicInfo> topics = {
        {"topic-1", 2, false, {}},
        {"topic-2", 4, false, {}}
    };
    
    handler_.set_topics_callback([&]() {
        return topics;
    });
    
    // Metadata request should use the callback
    BufferWriter request_writer;
    request_writer.write_int16(static_cast<int16_t>(ApiKey::Metadata));
    request_writer.write_int16(0);
    request_writer.write_int32(1000);
    request_writer.write_string("client");
    request_writer.write_int32(0);  // all topics
    
    auto response = handler_.handle_request(request_writer.data());
    EXPECT_FALSE(response.empty());
}

TEST_F(ProtocolHandlerTest, SetGroupsCallback) {
    std::vector<ConsumerGroupInfo> groups = {
        {"group-1", "consumer", "Stable", {}},
        {"group-2", "consumer", "Empty", {}}
    };
    
    handler_.set_groups_callback([&]() {
        return groups;
    });
    
    // ListGroups request should use the callback
    BufferWriter request_writer;
    request_writer.write_int16(static_cast<int16_t>(ApiKey::ListGroups));
    request_writer.write_int16(0);
    request_writer.write_int32(1100);
    request_writer.write_string("admin");
    
    auto response = handler_.handle_request(request_writer.data());
    EXPECT_FALSE(response.empty());
}

TEST_F(ProtocolHandlerTest, BrokerVersionInfo) {
    protocol::BrokerVersion version{"custom-broker", "2.0.0", "abc123"};
    handler_.set_broker_version(version);
    
    // ApiVersions request
    auto request = make_request(ApiKey::ApiVersions, 0, 1200, "client");
    auto response = handler_.handle_request(request);
    EXPECT_FALSE(response.empty());
}

TEST_F(ProtocolHandlerTest, AddTopic) {
    TopicInfo topic;
    topic.name = "new-topic";
    topic.num_partitions = 3;
    topic.is_internal = false;
    
    handler_.add_topic(topic);
    
    // Metadata should show the topic
    BufferWriter request_writer;
    request_writer.write_int16(static_cast<int16_t>(ApiKey::Metadata));
    request_writer.write_int16(0);
    request_writer.write_int32(1300);
    request_writer.write_string("client");
    request_writer.write_int32(1);
    request_writer.write_string("new-topic");
    
    auto response = handler_.handle_request(request_writer.data());
    EXPECT_FALSE(response.empty());
    
    // Verify topic is in stored topics
    auto topics = handler_.get_stored_topics();
    EXPECT_EQ(topics.size(), 1);
    EXPECT_EQ(topics[0].name, "new-topic");
    EXPECT_EQ(topics[0].num_partitions, 3);
}

TEST_F(ProtocolHandlerTest, RemoveTopic) {
    // Add multiple topics
    TopicInfo topic1;
    topic1.name = "topic-1";
    topic1.num_partitions = 2;
    topic1.is_internal = false;
    
    TopicInfo topic2;
    topic2.name = "topic-2";
    topic2.num_partitions = 4;
    topic2.is_internal = false;
    
    TopicInfo topic3;
    topic3.name = "topic-3";
    topic3.num_partitions = 1;
    topic3.is_internal = false;
    
    handler_.add_topic(topic1);
    handler_.add_topic(topic2);
    handler_.add_topic(topic3);
    
    auto topics = handler_.get_stored_topics();
    EXPECT_EQ(topics.size(), 3);
    
    // Remove topic-2
    handler_.remove_topic("topic-2");
    
    topics = handler_.get_stored_topics();
    EXPECT_EQ(topics.size(), 2);
    
    // Verify topic-2 is gone but others remain
    bool found_topic1 = false;
    bool found_topic2 = false;
    bool found_topic3 = false;
    for (const auto& t : topics) {
        if (t.name == "topic-1") found_topic1 = true;
        if (t.name == "topic-2") found_topic2 = true;
        if (t.name == "topic-3") found_topic3 = true;
    }
    EXPECT_TRUE(found_topic1);
    EXPECT_FALSE(found_topic2);
    EXPECT_TRUE(found_topic3);
    
    // Remove non-existent topic (should not crash)
    handler_.remove_topic("nonexistent");
    EXPECT_EQ(handler_.get_stored_topics().size(), 2);
    
    // Remove remaining topics
    handler_.remove_topic("topic-1");
    handler_.remove_topic("topic-3");
    EXPECT_EQ(handler_.get_stored_topics().size(), 0);
}

// ============================================================================
// Response Header Test
// ============================================================================

TEST(ResponseHeaderTest, WriteHeader) {
    ResponseHeader header;
    header.correlation_id = 12345;
    
    BufferWriter writer;
    header.write(writer);
    
    BufferReader reader(writer.data());
    EXPECT_EQ(reader.read_int32(), 12345);
}

// ============================================================================
// ApiVersionInfo Test
// ============================================================================

TEST(ApiVersionInfoTest, Structure) {
    ApiVersionInfo info;
    info.api_key = ApiKey::Produce;
    info.min_version = 0;
    info.max_version = 9;
    
    EXPECT_EQ(info.api_key, ApiKey::Produce);
    EXPECT_EQ(info.min_version, 0);
    EXPECT_EQ(info.max_version, 9);
}

