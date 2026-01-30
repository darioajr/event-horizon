#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include <memory>
#include <unordered_map>
#include <functional>
#include <stdexcept>
#include <mutex>

namespace eventhorizon {
namespace protocol {

// ============================================================================
// Kafka API Keys (Protocol v2.0+)
// ============================================================================
enum class ApiKey : int16_t {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    DeleteRecords = 21,
    InitProducerId = 22,
    OffsetForLeaderEpoch = 23,
    AddPartitionsToTxn = 24,
    AddOffsetsToTxn = 25,
    EndTxn = 26,
    WriteTxnMarkers = 27,
    TxnOffsetCommit = 28,
    DescribeAcls = 29,
    CreateAcls = 30,
    DeleteAcls = 31,
    DescribeConfigs = 32,
    AlterConfigs = 33,
    AlterReplicaLogDirs = 34,
    DescribeLogDirs = 35,
    SaslAuthenticate = 36,
    CreatePartitions = 37,
    CreateDelegationToken = 38,
    RenewDelegationToken = 39,
    ExpireDelegationToken = 40,
    DescribeDelegationToken = 41,
    DeleteGroups = 42,
    ElectLeaders = 43,
    IncrementalAlterConfigs = 44,
    AlterPartitionReassignments = 45,
    ListPartitionReassignments = 46,
    OffsetDelete = 47,
    DescribeClientQuotas = 48,
    AlterClientQuotas = 49,
};

// ============================================================================
// Error Codes
// ============================================================================
enum class ErrorCode : int16_t {
    None = 0,
    Unknown = -1,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidFetchSize = 4,
    LeaderNotAvailable = 5,
    NotLeaderForPartition = 6,
    RequestTimedOut = 7,
    BrokerNotAvailable = 8,
    ReplicaNotAvailable = 9,
    MessageTooLarge = 10,
    StaleControllerEpoch = 11,
    OffsetMetadataTooLarge = 12,
    NetworkException = 13,
    CoordinatorLoadInProgress = 14,
    CoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    InvalidTopicException = 17,
    RecordListTooLarge = 18,
    NotEnoughReplicas = 19,
    NotEnoughReplicasAfterAppend = 20,
    InvalidRequiredAcks = 21,
    IllegalGeneration = 22,
    InconsistentGroupProtocol = 23,
    InvalidGroupId = 24,
    UnknownMemberId = 25,
    InvalidSessionTimeout = 26,
    RebalanceInProgress = 27,
    InvalidCommitOffsetSize = 28,
    TopicAuthorizationFailed = 29,
    GroupAuthorizationFailed = 30,
    ClusterAuthorizationFailed = 31,
    InvalidTimestamp = 32,
    UnsupportedSaslMechanism = 33,
    IllegalSaslState = 34,
    UnsupportedVersion = 35,
    TopicAlreadyExists = 36,
    InvalidPartitions = 37,
    InvalidReplicationFactor = 38,
    InvalidReplicaAssignment = 39,
    InvalidConfig = 40,
    NotController = 41,
    InvalidRequest = 42,
    UnsupportedForMessageFormat = 43,
    PolicyViolation = 44,
};

// ============================================================================
// Buffer Reader - Para deserialização
// ============================================================================
class BufferReader {
public:
    BufferReader(const uint8_t* data, size_t size);
    BufferReader(const std::vector<uint8_t>& data);

    int8_t read_int8();
    int16_t read_int16();
    int32_t read_int32();
    int64_t read_int64();
    uint8_t read_uint8();
    uint16_t read_uint16();
    uint32_t read_uint32();
    uint64_t read_uint64();
    
    // Varints (Kafka protocol v2+)
    int32_t read_varint();
    int64_t read_varlong();
    uint32_t read_unsigned_varint();
    
    std::string read_string();           // INT16 length + bytes
    std::string read_nullable_string();  // INT16 length (-1 = null) + bytes
    std::string read_compact_string();   // UNSIGNED_VARINT length + bytes
    std::string read_compact_nullable_string(); // UNSIGNED_VARINT length (0 = null, else len-1)
    
    std::vector<uint8_t> read_bytes();          // INT32 length + bytes
    std::vector<uint8_t> read_nullable_bytes(); // INT32 length (-1 = null) + bytes
    std::vector<uint8_t> read_compact_bytes();  // UNSIGNED_VARINT length + bytes
    std::vector<uint8_t> read_compact_nullable_bytes(); // UNSIGNED_VARINT length (0 = null, else len-1)
    
    std::string read_bytes_as_string(size_t len); // Read len bytes as string
    
    bool read_bool();
    
    // Array helpers
    template<typename T, typename ReadFunc>
    std::vector<T> read_array(ReadFunc func);
    
    size_t remaining() const { return size_ - pos_; }
    size_t position() const { return pos_; }
    void skip(size_t bytes);
    
    const uint8_t* current() const { return data_ + pos_; }

private:
    const uint8_t* data_;
    size_t size_;
    size_t pos_ = 0;
    
    void check_remaining(size_t bytes) const;
};

// ============================================================================
// Buffer Writer - Para serialização
// ============================================================================
class BufferWriter {
public:
    BufferWriter();
    explicit BufferWriter(size_t reserve_size);

    void write_int8(int8_t value);
    void write_int16(int16_t value);
    void write_int32(int32_t value);
    void write_int64(int64_t value);
    void write_uint8(uint8_t value);
    void write_uint16(uint16_t value);
    void write_uint32(uint32_t value);
    void write_uint64(uint64_t value);
    
    // Varints
    void write_varint(int32_t value);
    void write_varlong(int64_t value);
    void write_unsigned_varint(uint32_t value);
    
    void write_string(const std::string& value);
    void write_nullable_string(const std::string* value);
    void write_compact_string(const std::string& value);
    void write_compact_nullable_string(const std::string& value); // 0 = null, else len+1
    
    void write_bytes(const std::vector<uint8_t>& value);
    void write_nullable_bytes(const std::vector<uint8_t>* value);
    void write_compact_bytes(const std::vector<uint8_t>& value);
    void write_compact_nullable_bytes(const std::vector<uint8_t>& value);
    
    void write_bool(bool value);
    void write_raw(const uint8_t* data, size_t size);
    
    // Array helpers
    template<typename T, typename WriteFunc>
    void write_array(const std::vector<T>& items, WriteFunc func);
    
    std::vector<uint8_t>& data() { return buffer_; }
    const std::vector<uint8_t>& data() const { return buffer_; }
    size_t size() const { return buffer_.size(); }
    
    void clear() { buffer_.clear(); }

private:
    std::vector<uint8_t> buffer_;
};

// ============================================================================
// Request Header
// ============================================================================
struct RequestHeader {
    ApiKey api_key;
    int16_t api_version;
    int32_t correlation_id;
    std::string client_id;
    
    static RequestHeader parse(BufferReader& reader);
};

// ============================================================================
// Response Header
// ============================================================================
struct ResponseHeader {
    int32_t correlation_id;
    
    void write(BufferWriter& writer) const;
};

// ============================================================================
// API Version Info
// ============================================================================
struct ApiVersionInfo {
    ApiKey api_key;
    int16_t min_version;
    int16_t max_version;
};

// ============================================================================
// Version Info
// ============================================================================
struct BrokerVersion {
    std::string name = "eventhorizon";
    std::string version = "1.0.0";
    std::string commit_id = "unknown";
};

// Partition detail info for DescribeLogDirs
struct PartitionDetailInfo {
    int32_t partition_id = 0;
    int64_t size_bytes = 0;
    int32_t segment_count = 1;
};

// Topic info for metadata
struct TopicInfo {
    std::string name;
    int32_t num_partitions = 1;
    int16_t replication_factor = 1;
    bool is_internal = false;
    std::vector<PartitionDetailInfo> partition_details;  // Optional, for DescribeLogDirs
    std::unordered_map<std::string, std::string> configs;  // Topic configurations
    
    // Get config with default value (C++23 style)
    [[nodiscard]] auto get_config(std::string_view key, std::string_view default_value = "") const -> std::string {
        if (auto it = configs.find(std::string(key)); it != configs.end()) {
            return it->second;
        }
        return std::string(default_value);
    }
    
    // Check if config exists
    [[nodiscard]] bool has_config(std::string_view key) const {
        return configs.contains(std::string(key));
    }
};

// Consumer group info
struct ConsumerGroupInfo {
    std::string group_id;
    std::string protocol_type;
    std::string state;
    std::vector<std::string> members;
};

// ============================================================================
// Kafka Protocol Handler
// ============================================================================
class KafkaProtocolHandler {
public:
    KafkaProtocolHandler();
    ~KafkaProtocolHandler();

    /**
     * @brief Processa uma requisição Kafka e retorna a resposta
     */
    std::vector<uint8_t> handle_request(const std::vector<uint8_t>& request);

    /**
     * @brief Registra um handler customizado para uma API
     */
    using ApiHandler = std::function<std::vector<uint8_t>(
        const RequestHeader&, BufferReader&, BufferWriter&)>;
    
    void register_handler(ApiKey api_key, ApiHandler handler);

    /**
     * @brief Configura informações do broker
     */
    void set_broker_info(int32_t broker_id, const std::string& host, int32_t port);
    void set_cluster_id(const std::string& cluster_id);
    void set_broker_version(const BrokerVersion& version);
    
    /**
     * @brief Callbacks para obter dados dinâmicos do broker
     */
    using TopicsCallback = std::function<std::vector<TopicInfo>()>;
    using GroupsCallback = std::function<std::vector<ConsumerGroupInfo>()>;
    
    void set_topics_callback(TopicsCallback callback);
    void set_groups_callback(GroupsCallback callback);

private:
    // Handlers built-in
    std::vector<uint8_t> handle_api_versions(const RequestHeader& header, 
                                              BufferReader& reader);
    std::vector<uint8_t> handle_metadata(const RequestHeader& header, 
                                          BufferReader& reader);
    std::vector<uint8_t> handle_produce(const RequestHeader& header, 
                                         BufferReader& reader);
    std::vector<uint8_t> handle_fetch(const RequestHeader& header, 
                                       BufferReader& reader);
    std::vector<uint8_t> handle_list_offsets(const RequestHeader& header, 
                                              BufferReader& reader);
    std::vector<uint8_t> handle_find_coordinator(const RequestHeader& header, 
                                                  BufferReader& reader);
    std::vector<uint8_t> handle_join_group(const RequestHeader& header, 
                                            BufferReader& reader);
    std::vector<uint8_t> handle_sync_group(const RequestHeader& header, 
                                            BufferReader& reader);
    std::vector<uint8_t> handle_heartbeat(const RequestHeader& header, 
                                           BufferReader& reader);
    std::vector<uint8_t> handle_leave_group(const RequestHeader& header, 
                                             BufferReader& reader);
    std::vector<uint8_t> handle_offset_fetch(const RequestHeader& header, 
                                              BufferReader& reader);
    std::vector<uint8_t> handle_offset_commit(const RequestHeader& header, 
                                               BufferReader& reader);
    std::vector<uint8_t> handle_list_groups(const RequestHeader& header, 
                                             BufferReader& reader);
    std::vector<uint8_t> handle_describe_groups(const RequestHeader& header, 
                                                 BufferReader& reader);
    std::vector<uint8_t> handle_describe_configs(const RequestHeader& header, 
                                                  BufferReader& reader);
    std::vector<uint8_t> handle_create_topics(const RequestHeader& header, 
                                               BufferReader& reader);
    std::vector<uint8_t> handle_delete_topics(const RequestHeader& header, 
                                               BufferReader& reader);
    std::vector<uint8_t> handle_init_producer_id(const RequestHeader& header, 
                                                  BufferReader& reader);
    std::vector<uint8_t> handle_create_partitions(const RequestHeader& header, 
                                                   BufferReader& reader);
    std::vector<uint8_t> handle_describe_log_dirs(const RequestHeader& header, 
                                                   BufferReader& reader);
    
    // Resposta de erro genérica
    std::vector<uint8_t> make_error_response(const RequestHeader& header, 
                                              ErrorCode error);

    std::unordered_map<int16_t, ApiHandler> custom_handlers_;
    std::vector<ApiVersionInfo> supported_apis_;
    
    // Broker info
    int32_t broker_id_ = 1;
    std::string broker_host_ = "localhost";
    int32_t broker_port_ = 9092;
    std::string cluster_id_ = "event-horizon-cluster";
    BrokerVersion version_;
    
    // Callbacks
    TopicsCallback topics_callback_;
    GroupsCallback groups_callback_;
    
    // Stored topics (created via CreateTopics API)
    std::vector<TopicInfo> stored_topics_;
    mutable std::mutex topics_mutex_;
    
public:
    // Add a topic to storage
    void add_topic(const TopicInfo& topic);
    // Get all stored topics
    std::vector<TopicInfo> get_stored_topics() const;
};

// ============================================================================
// Template Implementations
// ============================================================================

template<typename T, typename ReadFunc>
std::vector<T> BufferReader::read_array(ReadFunc func) {
    int32_t count = read_int32();
    if (count < 0) {
        return {}; // null array
    }
    
    std::vector<T> result;
    result.reserve(static_cast<size_t>(count));
    
    for (int32_t i = 0; i < count; ++i) {
        result.push_back(func(*this));
    }
    
    return result;
}

template<typename T, typename WriteFunc>
void BufferWriter::write_array(const std::vector<T>& items, WriteFunc func) {
    write_int32(static_cast<int32_t>(items.size()));
    for (const auto& item : items) {
        func(*this, item);
    }
}

} // namespace protocol
} // namespace eventhorizon
