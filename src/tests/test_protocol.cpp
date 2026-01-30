#include <gtest/gtest.h>
#include "../protocol/kafka_protocol.hpp"
#include <vector>
#include <string>

using namespace eventhorizon::protocol;

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
    }
    
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
    BufferWriter request_writer;
    request_writer.write_int16(static_cast<int16_t>(ApiKey::Heartbeat));
    request_writer.write_int16(0);
    request_writer.write_int32(100);
    request_writer.write_string("consumer");
    request_writer.write_string("group-1");
    request_writer.write_int32(1);
    request_writer.write_string("member-1");
    
    auto response = handler_.handle_request(request_writer.data());
    
    BufferReader reader(response);
    int32_t correlation_id = reader.read_int32();
    int16_t error_code = reader.read_int16();
    
    EXPECT_EQ(correlation_id, 100);
    EXPECT_EQ(error_code, 0);
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
}

// ============================================================================
// API Key Test
// ============================================================================

TEST(ApiKeyTest, ApiKeyValues) {
    EXPECT_EQ(static_cast<int16_t>(ApiKey::Produce), 0);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::Fetch), 1);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::ListOffsets), 2);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::Metadata), 3);
    EXPECT_EQ(static_cast<int16_t>(ApiKey::ApiVersions), 18);
}
