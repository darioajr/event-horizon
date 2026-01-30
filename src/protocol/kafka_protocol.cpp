#include "kafka_protocol.hpp"
#include <stdexcept>
#include <cstring>
#include <chrono>
#include <iostream>
#include <atomic>

namespace eventhorizon {
namespace protocol {

// ============================================================================
// BufferReader Implementation
// ============================================================================

BufferReader::BufferReader(const uint8_t* data, size_t size)
    : data_(data), size_(size) {}

BufferReader::BufferReader(const std::vector<uint8_t>& data)
    : data_(data.data()), size_(data.size()) {}

void BufferReader::check_remaining(size_t bytes) const {
    if (pos_ + bytes > size_) {
        throw std::runtime_error("Buffer underflow: need " + 
            std::to_string(bytes) + " bytes, have " + 
            std::to_string(size_ - pos_));
    }
}

int8_t BufferReader::read_int8() {
    check_remaining(1);
    return static_cast<int8_t>(data_[pos_++]);
}

int16_t BufferReader::read_int16() {
    check_remaining(2);
    int16_t value = (static_cast<int16_t>(data_[pos_]) << 8) |
                    (static_cast<int16_t>(data_[pos_ + 1]));
    pos_ += 2;
    return value;
}

int32_t BufferReader::read_int32() {
    check_remaining(4);
    int32_t value = (static_cast<int32_t>(data_[pos_]) << 24) |
                    (static_cast<int32_t>(data_[pos_ + 1]) << 16) |
                    (static_cast<int32_t>(data_[pos_ + 2]) << 8) |
                    (static_cast<int32_t>(data_[pos_ + 3]));
    pos_ += 4;
    return value;
}

int64_t BufferReader::read_int64() {
    check_remaining(8);
    int64_t value = (static_cast<int64_t>(data_[pos_]) << 56) |
                    (static_cast<int64_t>(data_[pos_ + 1]) << 48) |
                    (static_cast<int64_t>(data_[pos_ + 2]) << 40) |
                    (static_cast<int64_t>(data_[pos_ + 3]) << 32) |
                    (static_cast<int64_t>(data_[pos_ + 4]) << 24) |
                    (static_cast<int64_t>(data_[pos_ + 5]) << 16) |
                    (static_cast<int64_t>(data_[pos_ + 6]) << 8) |
                    (static_cast<int64_t>(data_[pos_ + 7]));
    pos_ += 8;
    return value;
}

uint8_t BufferReader::read_uint8() {
    return static_cast<uint8_t>(read_int8());
}

uint16_t BufferReader::read_uint16() {
    return static_cast<uint16_t>(read_int16());
}

uint32_t BufferReader::read_uint32() {
    return static_cast<uint32_t>(read_int32());
}

uint64_t BufferReader::read_uint64() {
    return static_cast<uint64_t>(read_int64());
}

int32_t BufferReader::read_varint() {
    uint32_t value = read_unsigned_varint();
    return static_cast<int32_t>((value >> 1) ^ -(value & 1));
}

int64_t BufferReader::read_varlong() {
    uint64_t value = 0;
    int shift = 0;
    uint8_t byte;
    
    do {
        check_remaining(1);
        byte = data_[pos_++];
        value |= static_cast<uint64_t>(byte & 0x7F) << shift;
        shift += 7;
    } while ((byte & 0x80) != 0 && shift < 64);
    
    return static_cast<int64_t>((value >> 1) ^ -(value & 1));
}

uint32_t BufferReader::read_unsigned_varint() {
    uint32_t value = 0;
    int shift = 0;
    uint8_t byte;
    
    do {
        check_remaining(1);
        byte = data_[pos_++];
        value |= static_cast<uint32_t>(byte & 0x7F) << shift;
        shift += 7;
    } while ((byte & 0x80) != 0 && shift < 32);
    
    return value;
}

std::string BufferReader::read_string() {
    int16_t length = read_int16();
    if (length < 0) {
        throw std::runtime_error("Negative string length");
    }
    check_remaining(static_cast<size_t>(length));
    std::string result(reinterpret_cast<const char*>(data_ + pos_), length);
    pos_ += length;
    return result;
}

std::string BufferReader::read_nullable_string() {
    int16_t length = read_int16();
    if (length < 0) {
        return ""; // null string
    }
    check_remaining(static_cast<size_t>(length));
    std::string result(reinterpret_cast<const char*>(data_ + pos_), length);
    pos_ += length;
    return result;
}

std::string BufferReader::read_compact_string() {
    uint32_t length = read_unsigned_varint();
    if (length == 0) {
        return "";
    }
    length -= 1; // compact strings use length + 1
    check_remaining(length);
    std::string result(reinterpret_cast<const char*>(data_ + pos_), length);
    pos_ += length;
    return result;
}

std::vector<uint8_t> BufferReader::read_bytes() {
    int32_t length = read_int32();
    if (length < 0) {
        throw std::runtime_error("Negative bytes length");
    }
    check_remaining(static_cast<size_t>(length));
    std::vector<uint8_t> result(data_ + pos_, data_ + pos_ + length);
    pos_ += length;
    return result;
}

std::vector<uint8_t> BufferReader::read_nullable_bytes() {
    int32_t length = read_int32();
    if (length < 0) {
        return {}; // null bytes
    }
    check_remaining(static_cast<size_t>(length));
    std::vector<uint8_t> result(data_ + pos_, data_ + pos_ + length);
    pos_ += length;
    return result;
}

std::vector<uint8_t> BufferReader::read_compact_bytes() {
    uint32_t length = read_unsigned_varint();
    if (length == 0) {
        return {};
    }
    length -= 1;
    check_remaining(length);
    std::vector<uint8_t> result(data_ + pos_, data_ + pos_ + length);
    pos_ += length;
    return result;
}

std::string BufferReader::read_compact_nullable_string() {
    uint32_t length = read_unsigned_varint();
    if (length == 0) {
        return ""; // null string
    }
    length -= 1;
    check_remaining(length);
    std::string result(reinterpret_cast<const char*>(data_ + pos_), length);
    pos_ += length;
    return result;
}

std::vector<uint8_t> BufferReader::read_compact_nullable_bytes() {
    uint32_t length = read_unsigned_varint();
    if (length == 0) {
        return {}; // null bytes
    }
    length -= 1;
    check_remaining(length);
    std::vector<uint8_t> result(data_ + pos_, data_ + pos_ + length);
    pos_ += length;
    return result;
}

bool BufferReader::read_bool() {
    return read_int8() != 0;
}

void BufferReader::skip(size_t bytes) {
    check_remaining(bytes);
    pos_ += bytes;
}

std::string BufferReader::read_bytes_as_string(size_t len) {
    check_remaining(len);
    std::string result(reinterpret_cast<const char*>(data_ + pos_), len);
    pos_ += len;
    return result;
}

// ============================================================================
// BufferWriter Implementation
// ============================================================================

BufferWriter::BufferWriter() = default;

BufferWriter::BufferWriter(size_t reserve_size) {
    buffer_.reserve(reserve_size);
}

void BufferWriter::write_int8(int8_t value) {
    buffer_.push_back(static_cast<uint8_t>(value));
}

void BufferWriter::write_int16(int16_t value) {
    buffer_.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
    buffer_.push_back(static_cast<uint8_t>(value & 0xFF));
}

void BufferWriter::write_int32(int32_t value) {
    buffer_.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
    buffer_.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
    buffer_.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
    buffer_.push_back(static_cast<uint8_t>(value & 0xFF));
}

void BufferWriter::write_int64(int64_t value) {
    buffer_.push_back(static_cast<uint8_t>((value >> 56) & 0xFF));
    buffer_.push_back(static_cast<uint8_t>((value >> 48) & 0xFF));
    buffer_.push_back(static_cast<uint8_t>((value >> 40) & 0xFF));
    buffer_.push_back(static_cast<uint8_t>((value >> 32) & 0xFF));
    buffer_.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
    buffer_.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
    buffer_.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
    buffer_.push_back(static_cast<uint8_t>(value & 0xFF));
}

void BufferWriter::write_uint8(uint8_t value) {
    buffer_.push_back(value);
}

void BufferWriter::write_uint16(uint16_t value) {
    write_int16(static_cast<int16_t>(value));
}

void BufferWriter::write_uint32(uint32_t value) {
    write_int32(static_cast<int32_t>(value));
}

void BufferWriter::write_uint64(uint64_t value) {
    write_int64(static_cast<int64_t>(value));
}

void BufferWriter::write_varint(int32_t value) {
    // Zigzag encoding: (n << 1) ^ (n >> 31)
    // The arithmetic right shift on signed value propagates the sign bit
    uint32_t encoded = (static_cast<uint32_t>(value) << 1) ^ 
                       static_cast<uint32_t>(value >> 31);
    write_unsigned_varint(encoded);
}

void BufferWriter::write_varlong(int64_t value) {
    // Zigzag encoding: (n << 1) ^ (n >> 63)
    // The arithmetic right shift on signed value propagates the sign bit
    uint64_t encoded = (static_cast<uint64_t>(value) << 1) ^ 
                       static_cast<uint64_t>(value >> 63);
    
    while ((encoded & ~0x7FULL) != 0) {
        buffer_.push_back(static_cast<uint8_t>((encoded & 0x7F) | 0x80));
        encoded >>= 7;
    }
    buffer_.push_back(static_cast<uint8_t>(encoded));
}

void BufferWriter::write_unsigned_varint(uint32_t value) {
    while ((value & ~0x7FU) != 0) {
        buffer_.push_back(static_cast<uint8_t>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    buffer_.push_back(static_cast<uint8_t>(value));
}

void BufferWriter::write_string(const std::string& value) {
    write_int16(static_cast<int16_t>(value.size()));
    buffer_.insert(buffer_.end(), value.begin(), value.end());
}

void BufferWriter::write_nullable_string(const std::string* value) {
    if (!value) {
        write_int16(-1);
    } else {
        write_string(*value);
    }
}

void BufferWriter::write_compact_string(const std::string& value) {
    write_unsigned_varint(static_cast<uint32_t>(value.size() + 1));
    buffer_.insert(buffer_.end(), value.begin(), value.end());
}

void BufferWriter::write_bytes(const std::vector<uint8_t>& value) {
    write_int32(static_cast<int32_t>(value.size()));
    buffer_.insert(buffer_.end(), value.begin(), value.end());
}

void BufferWriter::write_nullable_bytes(const std::vector<uint8_t>* value) {
    if (!value) {
        write_int32(-1);
    } else {
        write_bytes(*value);
    }
}

void BufferWriter::write_compact_bytes(const std::vector<uint8_t>& value) {
    write_unsigned_varint(static_cast<uint32_t>(value.size() + 1));
    buffer_.insert(buffer_.end(), value.begin(), value.end());
}

void BufferWriter::write_compact_nullable_string(const std::string& value) {
    // Empty string treated as non-null empty (length = 1), null would be 0
    write_unsigned_varint(static_cast<uint32_t>(value.size() + 1));
    buffer_.insert(buffer_.end(), value.begin(), value.end());
}

void BufferWriter::write_compact_nullable_bytes(const std::vector<uint8_t>& value) {
    // Empty vector treated as non-null empty (length = 1), null would be 0
    write_unsigned_varint(static_cast<uint32_t>(value.size() + 1));
    buffer_.insert(buffer_.end(), value.begin(), value.end());
}

void BufferWriter::write_bool(bool value) {
    write_int8(value ? 1 : 0);
}

void BufferWriter::write_raw(const uint8_t* data, size_t size) {
    buffer_.insert(buffer_.end(), data, data + size);
}

// ============================================================================
// RequestHeader Implementation
// ============================================================================

RequestHeader RequestHeader::parse(BufferReader& reader) {
    RequestHeader header;
    header.api_key = static_cast<ApiKey>(reader.read_int16());
    header.api_version = reader.read_int16();
    header.correlation_id = reader.read_int32();
    
    // Determine if this API version uses flexible format (request header v2)
    // Flexible APIs use COMPACT_NULLABLE_STRING for client_id + TAG_BUFFER
    // Non-flexible APIs use NULLABLE_STRING (INT16 length prefix)
    bool flexible_header = false;
    
    switch (header.api_key) {
        case ApiKey::Produce:
            flexible_header = (header.api_version >= 9);
            break;
        case ApiKey::Fetch:
            flexible_header = (header.api_version >= 12);
            break;
        case ApiKey::ListOffsets:
            flexible_header = (header.api_version >= 6);
            break;
        case ApiKey::Metadata:
            flexible_header = (header.api_version >= 9);
            break;
        case ApiKey::OffsetCommit:
            flexible_header = (header.api_version >= 8);
            break;
        case ApiKey::OffsetFetch:
            flexible_header = (header.api_version >= 6);
            break;
        case ApiKey::FindCoordinator:
            flexible_header = (header.api_version >= 3);
            break;
        case ApiKey::JoinGroup:
            flexible_header = (header.api_version >= 6);
            break;
        case ApiKey::Heartbeat:
            flexible_header = (header.api_version >= 4);
            break;
        case ApiKey::LeaveGroup:
            flexible_header = (header.api_version >= 4);
            break;
        case ApiKey::SyncGroup:
            flexible_header = (header.api_version >= 4);
            break;
        case ApiKey::DescribeGroups:
            flexible_header = (header.api_version >= 5);
            break;
        case ApiKey::ListGroups:
            flexible_header = (header.api_version >= 4);  // Changed: v4+
            break;
        case ApiKey::ApiVersions:
            flexible_header = (header.api_version >= 3);
            break;
        case ApiKey::CreateTopics:
            flexible_header = (header.api_version >= 5);  // Kafka 4.1: v5+ uses Request Header v2
            break;
        case ApiKey::DeleteTopics:
            flexible_header = (header.api_version >= 4);  // Kafka 4.1: v4+ uses Request Header v2
            break;
        case ApiKey::DescribeConfigs:
            flexible_header = (header.api_version >= 4);  // Kafka 4.1: v4+ uses Request Header v2
            break;
        case ApiKey::InitProducerId:
            flexible_header = (header.api_version >= 4);  // Changed: v4+
            break;
        case ApiKey::CreatePartitions:
            flexible_header = (header.api_version >= 2);  // Kafka 4.1: v2+ uses Request Header v2
            break;
        case ApiKey::DescribeLogDirs:
            flexible_header = (header.api_version >= 2);  // Kafka 4.1: v2+ uses Request Header v2
            break;
        default:
            flexible_header = false;
            break;
    }
    
    if (flexible_header) {
        // Header v2 (Kafka 4.1): client_id é NULLABLE_STRING (INT16 length) + TAG_BUFFER
        // IMPORTANTE: Header v2 ainda usa NULLABLE_STRING para client_id, NÃO COMPACT_NULLABLE_STRING!
        header.client_id = reader.read_nullable_string();
        // TAG_BUFFER (tagged fields) - skip
        uint32_t tag_count = reader.read_unsigned_varint();
        for (uint32_t i = 0; i < tag_count; i++) {
            reader.read_unsigned_varint(); // tag
            uint32_t len = reader.read_unsigned_varint();
            reader.skip(len); // skip tagged field data
        }
    } else {
        // Header v1: client_id é NULLABLE_STRING (INT16 length)
        header.client_id = reader.read_nullable_string();
    }
    
    return header;
}

// ============================================================================
// ResponseHeader Implementation
// ============================================================================

void ResponseHeader::write(BufferWriter& writer) const {
    writer.write_int32(correlation_id);
}

// ============================================================================
// KafkaProtocolHandler Implementation
// ============================================================================

KafkaProtocolHandler::KafkaProtocolHandler() {
    // Registrar APIs suportadas - compatível APENAS com Kafka 4.1.x
    // Versões baseadas na documentação oficial: https://kafka.apache.org/41/design/protocol/
    supported_apis_ = {
        {ApiKey::Produce, 9, 11},          // v9+ flexible, v11 max in 4.1
        {ApiKey::Fetch, 12, 12},           // v12 only - v13+ uses topic_id which requires UUID mapping
        {ApiKey::ListOffsets, 6, 9},       // v6+ flexible, v9 max in 4.1
        {ApiKey::Metadata, 9, 12},         // v9+ flexible, limit to v12 for compatibility
        {ApiKey::OffsetCommit, 8, 9},      // v8+ flexible, v9 max in 4.1
        {ApiKey::OffsetFetch, 6, 9},       // v6+ flexible, v9 max in 4.1
        {ApiKey::FindCoordinator, 3, 6},   // v3+ flexible, v6 max in 4.1
        {ApiKey::JoinGroup, 6, 10},        // v6+ flexible, v10 max in 4.1
        {ApiKey::Heartbeat, 4, 5},         // v4+ flexible, v5 max in 4.1
        {ApiKey::LeaveGroup, 4, 6},        // v4+ flexible, v6 max in 4.1
        {ApiKey::SyncGroup, 4, 6},         // v4+ flexible, v6 max in 4.1
        {ApiKey::DescribeGroups, 5, 6},    // v5+ flexible, v6 max in 4.1
        {ApiKey::ListGroups, 4, 5},        // v4+ flexible, v5 max in 4.1
        {ApiKey::ApiVersions, 3, 4},       // v3+ flexible, v4 max in 4.1
        {ApiKey::CreateTopics, 5, 7},      // v5+ flexible, v7 max in 4.1
        {ApiKey::DeleteTopics, 4, 6},      // v4+ flexible, v6 max in 4.1
        {ApiKey::DescribeConfigs, 4, 4},   // v4 flexible, v4 max in 4.1
        {ApiKey::InitProducerId, 4, 5},    // v4+ flexible, v5 max in 4.1
        {ApiKey::CreatePartitions, 2, 3},  // v2+ flexible, v3 max in 4.1
        {ApiKey::DescribeLogDirs, 2, 4},   // v2+ flexible, v4 max in 4.1
    };
    
    // Versão compatível com Kafka 4.1.x
    version_ = {"eventhorizon", "4.1.0", "event-horizon-1.0"};
}

KafkaProtocolHandler::~KafkaProtocolHandler() = default;

void KafkaProtocolHandler::set_broker_info(int32_t broker_id, 
                                            const std::string& host, 
                                            int32_t port) {
    broker_id_ = broker_id;
    broker_host_ = host;
    broker_port_ = port;
}

void KafkaProtocolHandler::set_cluster_id(const std::string& cluster_id) {
    cluster_id_ = cluster_id;
}

void KafkaProtocolHandler::set_broker_version(const BrokerVersion& version) {
    version_ = version;
}

void KafkaProtocolHandler::set_topics_callback(TopicsCallback callback) {
    topics_callback_ = std::move(callback);
}

void KafkaProtocolHandler::set_groups_callback(GroupsCallback callback) {
    groups_callback_ = std::move(callback);
}

void KafkaProtocolHandler::add_topic(const TopicInfo& topic) {
    std::lock_guard<std::mutex> lock(topics_mutex_);
    // Check if topic already exists
    for (auto& t : stored_topics_) {
        if (t.name == topic.name) {
            t = topic; // Update existing
            return;
        }
    }
    stored_topics_.push_back(topic);
}

std::vector<TopicInfo> KafkaProtocolHandler::get_stored_topics() const {
    std::lock_guard<std::mutex> lock(topics_mutex_);
    return stored_topics_;
}

void KafkaProtocolHandler::register_handler(ApiKey api_key, ApiHandler handler) {
    custom_handlers_[static_cast<int16_t>(api_key)] = std::move(handler);
}

std::vector<uint8_t> KafkaProtocolHandler::handle_request(
    const std::vector<uint8_t>& request) {
    
    try {
        BufferReader reader(request);
        RequestHeader header = RequestHeader::parse(reader);
        
        std::cout << "Request: API=" << static_cast<int>(header.api_key)
                  << " v" << header.api_version
                  << " CorrId=" << header.correlation_id
                  << " Client=" << header.client_id << "\n";
        
        // Verificar se há handler customizado
        auto custom_it = custom_handlers_.find(static_cast<int16_t>(header.api_key));
        if (custom_it != custom_handlers_.end()) {
            BufferWriter response_writer;
            return custom_it->second(header, reader, response_writer);
        }
        
        // Handlers built-in
        switch (header.api_key) {
            case ApiKey::ApiVersions:
                return handle_api_versions(header, reader);
            case ApiKey::Metadata:
                return handle_metadata(header, reader);
            case ApiKey::Produce:
                return handle_produce(header, reader);
            case ApiKey::Fetch:
                return handle_fetch(header, reader);
            case ApiKey::ListOffsets:
                return handle_list_offsets(header, reader);
            case ApiKey::FindCoordinator:
                return handle_find_coordinator(header, reader);
            case ApiKey::JoinGroup:
                return handle_join_group(header, reader);
            case ApiKey::SyncGroup:
                return handle_sync_group(header, reader);
            case ApiKey::Heartbeat:
                return handle_heartbeat(header, reader);
            case ApiKey::LeaveGroup:
                return handle_leave_group(header, reader);
            case ApiKey::OffsetFetch:
                return handle_offset_fetch(header, reader);
            case ApiKey::OffsetCommit:
                return handle_offset_commit(header, reader);
            case ApiKey::ListGroups:
                return handle_list_groups(header, reader);
            case ApiKey::DescribeGroups:
                return handle_describe_groups(header, reader);
            case ApiKey::DescribeConfigs:
                return handle_describe_configs(header, reader);
            case ApiKey::CreateTopics:
                return handle_create_topics(header, reader);
            case ApiKey::DeleteTopics:
                return handle_delete_topics(header, reader);
            case ApiKey::InitProducerId:
                return handle_init_producer_id(header, reader);
            case ApiKey::CreatePartitions:
                return handle_create_partitions(header, reader);
            case ApiKey::DescribeLogDirs:
                return handle_describe_log_dirs(header, reader);
            default:
                std::cerr << "Unsupported API: " << static_cast<int>(header.api_key) << "\n";
                return make_error_response(header, ErrorCode::UnsupportedVersion);
        }
    } catch (const std::exception& e) {
        std::cerr << "Error processing request: " << e.what() << "\n";
        return {};
    }
}

std::vector<uint8_t> KafkaProtocolHandler::handle_api_versions(
    const RequestHeader& header, BufferReader& /*reader*/) {
    
    BufferWriter writer;
    
    // Response header - para ApiVersions, SEMPRE usa header v0 (só correlation_id)
    // mesmo para v3+. Isso é uma exceção no protocolo Kafka para bootstrapping.
    writer.write_int32(header.correlation_id);
    
    // Para ApiVersions v3+, o response BODY usa formato "flexible"
    // Mas o header NÃO usa tagged fields!
    
    if (header.api_version >= 3) {
        // Error code
        writer.write_int16(static_cast<int16_t>(ErrorCode::None));
        
        // API versions array (compact array: length + 1)
        writer.write_unsigned_varint(static_cast<uint32_t>(supported_apis_.size() + 1));
        for (const auto& api : supported_apis_) {
            writer.write_int16(static_cast<int16_t>(api.api_key));
            writer.write_int16(api.min_version);
            writer.write_int16(api.max_version);
            // Tagged fields para cada entry
            writer.write_unsigned_varint(0);
        }
        
        // Throttle time
        writer.write_int32(0);
        
        // Tagged fields no final do body
        writer.write_unsigned_varint(0);
    } else {
        // Formato não-flexible para v0-v2
        
        // Error code
        writer.write_int16(static_cast<int16_t>(ErrorCode::None));
        
        // API versions array
        writer.write_int32(static_cast<int32_t>(supported_apis_.size()));
        for (const auto& api : supported_apis_) {
            writer.write_int16(static_cast<int16_t>(api.api_key));
            writer.write_int16(api.min_version);
            writer.write_int16(api.max_version);
        }
        
        // Throttle time (v1+)
        if (header.api_version >= 1) {
            writer.write_int32(0);
        }
    }
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_metadata(
    const RequestHeader& header, BufferReader& reader) {
    
    // Metadata v9+ uses flexible format
    bool flexible = (header.api_version >= 9);
    
    const uint8_t* ptr = reader.current();
    size_t rem = reader.remaining();
    
    // WORKAROUND: Skip leading 0x00 byte for flexible format
    if (flexible && rem >= 2 && ptr[0] == 0x00 && ptr[1] >= 0x01 && ptr[1] <= 0x20) {
        reader.skip(1);
    }
    
    // Parse topics requested (can be null = all topics)
    int32_t topics_requested;
    std::vector<std::string> topic_names;
    bool all_topics = false;
    
    if (flexible) {
        uint32_t arr_len = reader.read_unsigned_varint();
        if (arr_len == 0) {
            all_topics = true;
            topics_requested = -1;
        } else {
            topics_requested = static_cast<int32_t>(arr_len - 1);
        }
    } else {
        topics_requested = reader.read_int32();
        all_topics = (topics_requested < 0);
    }
    
    if (!all_topics && topics_requested > 0) {
        for (int32_t i = 0; i < topics_requested; ++i) {
            if (flexible) {
                // Topic ID (v10+) - UUID, skip 16 bytes if present
                if (header.api_version >= 10) {
                    reader.skip(16);
                }
                uint32_t name_len = reader.read_unsigned_varint();
                if (name_len > 1) {
                    topic_names.push_back(reader.read_bytes_as_string(name_len - 1));
                }
                reader.read_unsigned_varint(); // tagged fields
            } else {
                topic_names.push_back(reader.read_string());
            }
        }
    }
    
    // Skip remaining request fields for flexible
    if (flexible) {
        // allow_auto_topic_creation (v4+)
        if (header.api_version >= 4) {
            reader.read_bool();
        }
        // include_cluster_authorized_operations (v8+)
        if (header.api_version >= 8) {
            reader.read_bool();
        }
        // include_topic_authorized_operations (v8+)
        if (header.api_version >= 8) {
            reader.read_bool();
        }
        // tagged fields
        reader.read_unsigned_varint();
    }
    
    BufferWriter writer;
    
    // Response header
    writer.write_int32(header.correlation_id);
    if (flexible) {
        writer.write_unsigned_varint(0); // header tagged fields
    }
    
    // Throttle time (v3+)
    if (header.api_version >= 3) {
        writer.write_int32(0);
    }
    
    // Brokers array
    if (flexible) {
        writer.write_unsigned_varint(2); // 1 broker + 1 for compact array
    } else {
        writer.write_int32(1); // 1 broker
    }
    
    writer.write_int32(broker_id_);
    if (flexible) {
        writer.write_unsigned_varint(static_cast<uint32_t>(broker_host_.size() + 1));
        for (char c : broker_host_) writer.write_int8(static_cast<int8_t>(c));
    } else {
        writer.write_string(broker_host_);
    }
    writer.write_int32(broker_port_);
    
    if (header.api_version >= 1) {
        if (flexible) {
            writer.write_unsigned_varint(0); // null rack
        } else {
            writer.write_nullable_string(nullptr); // rack
        }
    }
    if (flexible) {
        writer.write_unsigned_varint(0); // broker tagged fields
    }
    
    // Cluster ID (v2+)
    if (header.api_version >= 2) {
        if (flexible) {
            writer.write_unsigned_varint(static_cast<uint32_t>(cluster_id_.size() + 1));
            for (char c : cluster_id_) writer.write_int8(static_cast<int8_t>(c));
        } else {
            writer.write_nullable_string(&cluster_id_);
        }
    }
    
    // Controller ID (v1+)
    if (header.api_version >= 1) {
        writer.write_int32(broker_id_);
    }
    
    // Topics array
    std::vector<TopicInfo> topics;
    if (topics_callback_) {
        topics = topics_callback_();
    }
    
    // Also include stored topics (created via CreateTopics)
    auto stored = get_stored_topics();
    for (const auto& t : stored) {
        bool found = false;
        for (const auto& existing : topics) {
            if (existing.name == t.name) {
                found = true;
                break;
            }
        }
        if (!found) {
            topics.push_back(t);
        }
    }
    
    // Filter if specific topics requested
    if (!all_topics && !topic_names.empty()) {
        std::vector<TopicInfo> filtered;
        for (const auto& topic : topics) {
            if (std::find(topic_names.begin(), topic_names.end(), topic.name) != topic_names.end()) {
                filtered.push_back(topic);
            }
        }
        topics = std::move(filtered);
    }
    
    // Debug: show topics being returned
    if (!topics.empty()) {
        std::cout << "  Metadata returning " << topics.size() << " topics: ";
        for (const auto& t : topics) std::cout << t.name << " ";
        std::cout << "\n";
    }
    
    if (flexible) {
        writer.write_unsigned_varint(static_cast<uint32_t>(topics.size() + 1));
    } else {
        writer.write_int32(static_cast<int32_t>(topics.size()));
    }
    
    for (const auto& topic : topics) {
        // Error code
        writer.write_int16(static_cast<int16_t>(ErrorCode::None));
        
        // Topic name
        if (flexible) {
            writer.write_unsigned_varint(static_cast<uint32_t>(topic.name.size() + 1));
            for (char c : topic.name) writer.write_int8(static_cast<int8_t>(c));
        } else {
            writer.write_string(topic.name);
        }
        
        // Topic ID (v10+) - UUID (16 bytes of zeros)
        if (header.api_version >= 10) {
            for (int j = 0; j < 16; ++j) writer.write_int8(0);
        }
        
        // Is internal (v1+)
        if (header.api_version >= 1) {
            writer.write_bool(topic.is_internal);
        }
        
        // Partitions array
        if (flexible) {
            writer.write_unsigned_varint(static_cast<uint32_t>(topic.num_partitions + 1));
        } else {
            writer.write_int32(topic.num_partitions);
        }
        
        for (int32_t p = 0; p < topic.num_partitions; ++p) {
            // Error code
            writer.write_int16(static_cast<int16_t>(ErrorCode::None));
            // Partition index
            writer.write_int32(p);
            // Leader ID
            writer.write_int32(broker_id_);
            // Leader epoch (v7+)
            if (header.api_version >= 7) {
                writer.write_int32(0);
            }
            // Replica nodes
            if (flexible) {
                writer.write_unsigned_varint(2); // 1 replica + 1
            } else {
                writer.write_int32(1);
            }
            writer.write_int32(broker_id_);
            // ISR nodes
            if (flexible) {
                writer.write_unsigned_varint(2); // 1 isr + 1
            } else {
                writer.write_int32(1);
            }
            writer.write_int32(broker_id_);
            // Offline replicas (v5+)
            if (header.api_version >= 5) {
                if (flexible) {
                    writer.write_unsigned_varint(1); // empty array
                } else {
                    writer.write_int32(0);
                }
            }
            if (flexible) {
                writer.write_unsigned_varint(0); // partition tagged fields
            }
        }
        
        // Topic authorized operations (v8+)
        if (header.api_version >= 8) {
            writer.write_int32(-2147483648); // unknown
        }
        
        if (flexible) {
            writer.write_unsigned_varint(0); // topic tagged fields
        }
    }
    
    // Cluster authorized operations (v8+)
    if (header.api_version >= 8) {
        writer.write_int32(-2147483648); // unknown
    }
    
    if (flexible) {
        writer.write_unsigned_varint(0); // response tagged fields
    }
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_produce(
    const RequestHeader& header, BufferReader& reader) {
    
    // Produce v9+ uses flexible format
    bool is_flexible = (header.api_version >= 9);
    
    // Parse produce request
    if (header.api_version >= 3) {
        if (is_flexible) {
            reader.read_compact_nullable_string(); // transactional_id
        } else {
            reader.read_nullable_string(); // transactional_id
        }
    }
    [[maybe_unused]] int16_t acks = reader.read_int16();
    [[maybe_unused]] int32_t timeout = reader.read_int32();
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    // Response header v1 for flexible versions - TAG_BUFFER
    if (is_flexible) {
        writer.write_unsigned_varint(0);  // empty tagged fields in header
    }
    
    // Skip parsing topics - just return empty response
    // Parse topics count
    [[maybe_unused]] int32_t topic_count;
    if (is_flexible) {
        uint32_t n = reader.read_unsigned_varint();
        topic_count = (n > 0) ? static_cast<int32_t>(n - 1) : 0;
    } else {
        topic_count = reader.read_int32();
    }
    
    // Topics response array - empty for now (não temos tópicos reais ainda)
    if (is_flexible) {
        writer.write_unsigned_varint(1); // empty COMPACT_ARRAY
    } else {
        writer.write_int32(0);
    }
    
    // Throttle time (v1+)
    if (header.api_version >= 1) {
        writer.write_int32(0);
    }
    
    // Response tagged fields for flexible versions
    if (is_flexible) {
        writer.write_unsigned_varint(0);
    }
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_fetch(
    const RequestHeader& header, [[maybe_unused]] BufferReader& reader) {
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    // Throttle time (v1+)
    if (header.api_version >= 1) {
        writer.write_int32(0);
    }
    
    // Error code (v7+)
    if (header.api_version >= 7) {
        writer.write_int16(static_cast<int16_t>(ErrorCode::None));
        writer.write_int32(0); // session_id
    }
    
    // Responses array - empty
    writer.write_int32(0);
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_list_offsets(
    const RequestHeader& header, BufferReader& /*reader*/) {
    
    bool is_flexible = (header.api_version >= 6);
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    // Response header v1 for flexible versions - TAG_BUFFER
    if (is_flexible) {
        writer.write_unsigned_varint(0);  // empty tagged fields in header
    }
    
    // Throttle time (v2+)
    if (header.api_version >= 2) {
        writer.write_int32(0);
    }
    
    // Topics array - empty
    // For flexible versions, use COMPACT_ARRAY (varint with length+1)
    if (is_flexible) {
        writer.write_unsigned_varint(1);  // length+1 = 0+1 = 1 (empty array)
    } else {
        writer.write_int32(0);
    }
    
    // Trailing tagged fields for flexible versions
    if (is_flexible) {
        writer.write_unsigned_varint(0);  // no tagged fields
    }
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_find_coordinator(
    const RequestHeader& header, BufferReader& reader) {
    
    std::string key = reader.read_string();
    int8_t key_type = 0;
    if (header.api_version >= 1) {
        key_type = reader.read_int8();
    }
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    // Throttle time (v1+)
    if (header.api_version >= 1) {
        writer.write_int32(0);
    }
    
    // Error code
    writer.write_int16(static_cast<int16_t>(ErrorCode::None));
    
    // Error message (v1+)
    if (header.api_version >= 1) {
        writer.write_nullable_string(nullptr);
    }
    
    // Coordinator info
    writer.write_int32(broker_id_);
    writer.write_string(broker_host_);
    writer.write_int32(broker_port_);
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_join_group(
    const RequestHeader& header, BufferReader& reader) {
    
    std::string group_id = reader.read_string();
    int32_t session_timeout = reader.read_int32();
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    // Throttle time (v2+)
    if (header.api_version >= 2) {
        writer.write_int32(0);
    }
    
    // Error code
    writer.write_int16(static_cast<int16_t>(ErrorCode::None));
    
    // Generation ID
    writer.write_int32(1);
    
    // Protocol type (v7+)
    if (header.api_version >= 7) {
        writer.write_nullable_string(nullptr);
    }
    
    // Protocol name
    writer.write_string("range");
    
    // Leader
    writer.write_string("member-1");
    
    // Member ID
    writer.write_string("member-1");
    
    // Members array
    writer.write_int32(1);
    writer.write_string("member-1");
    if (header.api_version >= 5) {
        writer.write_nullable_string(nullptr); // group_instance_id
    }
    writer.write_bytes({}); // metadata
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_sync_group(
    const RequestHeader& header, BufferReader& /*reader*/) {
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    // Throttle time (v1+)
    if (header.api_version >= 1) {
        writer.write_int32(0);
    }
    
    // Error code
    writer.write_int16(static_cast<int16_t>(ErrorCode::None));
    
    // Protocol type (v5+)
    if (header.api_version >= 5) {
        writer.write_nullable_string(nullptr);
    }
    
    // Protocol name (v5+)
    if (header.api_version >= 5) {
        writer.write_nullable_string(nullptr);
    }
    
    // Assignment
    writer.write_bytes({});
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_heartbeat(
    const RequestHeader& header, BufferReader& /*reader*/) {
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    // Throttle time (v1+)
    if (header.api_version >= 1) {
        writer.write_int32(0);
    }
    
    // Error code
    writer.write_int16(static_cast<int16_t>(ErrorCode::None));
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_leave_group(
    const RequestHeader& header, BufferReader& /*reader*/) {
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    // Throttle time (v1+)
    if (header.api_version >= 1) {
        writer.write_int32(0);
    }
    
    // Error code
    writer.write_int16(static_cast<int16_t>(ErrorCode::None));
    
    // Members (v3+)
    if (header.api_version >= 3) {
        writer.write_int32(0); // empty members array
    }
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_offset_fetch(
    const RequestHeader& header, BufferReader& /*reader*/) {
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    // Throttle time (v3+)
    if (header.api_version >= 3) {
        writer.write_int32(0);
    }
    
    // Topics array - empty
    writer.write_int32(0);
    
    // Error code (v2+)
    if (header.api_version >= 2) {
        writer.write_int16(static_cast<int16_t>(ErrorCode::None));
    }
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_offset_commit(
    const RequestHeader& header, BufferReader& /*reader*/) {
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    // Throttle time (v3+)
    if (header.api_version >= 3) {
        writer.write_int32(0);
    }
    
    // Topics array - empty
    writer.write_int32(0);
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_list_groups(
    const RequestHeader& header, BufferReader& /*reader*/) {
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    // Throttle time (v1+)
    if (header.api_version >= 1) {
        writer.write_int32(0);
    }
    
    // Error code
    writer.write_int16(static_cast<int16_t>(ErrorCode::None));
    
    // Groups array
    std::vector<ConsumerGroupInfo> groups;
    if (groups_callback_) {
        groups = groups_callback_();
    }
    
    writer.write_int32(static_cast<int32_t>(groups.size()));
    for (const auto& group : groups) {
        writer.write_string(group.group_id);
        writer.write_string(group.protocol_type);
        
        // Group state (v4+)
        if (header.api_version >= 4) {
            writer.write_string(group.state);
        }
    }
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_describe_groups(
    const RequestHeader& header, BufferReader& reader) {
    
    // Read group IDs
    int32_t group_count = reader.read_int32();
    std::vector<std::string> group_ids;
    for (int32_t i = 0; i < group_count; ++i) {
        group_ids.push_back(reader.read_string());
    }
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    // Throttle time (v1+)
    if (header.api_version >= 1) {
        writer.write_int32(0);
    }
    
    // Groups array
    std::vector<ConsumerGroupInfo> all_groups;
    if (groups_callback_) {
        all_groups = groups_callback_();
    }
    
    writer.write_int32(static_cast<int32_t>(group_ids.size()));
    for (const auto& group_id : group_ids) {
        // Find group
        ConsumerGroupInfo* found_group = nullptr;
        for (auto& g : all_groups) {
            if (g.group_id == group_id) {
                found_group = &g;
                break;
            }
        }
        
        if (found_group) {
            writer.write_int16(static_cast<int16_t>(ErrorCode::None));
            writer.write_string(found_group->group_id);
            writer.write_string(found_group->state);
            writer.write_string(found_group->protocol_type);
            writer.write_string(""); // protocol data
            
            // Members
            writer.write_int32(static_cast<int32_t>(found_group->members.size()));
            for (const auto& member : found_group->members) {
                writer.write_string(member); // member_id
                if (header.api_version >= 4) {
                    writer.write_nullable_string(nullptr); // group_instance_id
                }
                writer.write_string(""); // client_id
                writer.write_string(broker_host_); // client_host
                writer.write_bytes({}); // member_metadata
                writer.write_bytes({}); // member_assignment
            }
            
            // Authorized operations (v3+)
            if (header.api_version >= 3) {
                writer.write_int32(-2147483648);
            }
        } else {
            writer.write_int16(static_cast<int16_t>(ErrorCode::InvalidGroupId));
            writer.write_string(group_id);
            writer.write_string("Dead");
            writer.write_string("");
            writer.write_string("");
            writer.write_int32(0); // empty members
            if (header.api_version >= 3) {
                writer.write_int32(-2147483648);
            }
        }
    }
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_describe_configs(
    const RequestHeader& header, BufferReader& reader) {
    
    // DescribeConfigs v4+ uses flexible format
    bool flexible = (header.api_version >= 4);
    
    std::cout << "  DescribeConfigs v" << header.api_version << " (flexible=" << flexible << ")\n";
    
    // Parse the resources from the request
    struct ConfigResource {
        int8_t resource_type;  // 2=TOPIC, 4=BROKER, 8=BROKER_LOGGER
        std::string resource_name;
    };
    std::vector<ConfigResource> resources;
    
    try {
        // Read resources array
        int32_t resource_count;
        if (flexible) {
            resource_count = static_cast<int32_t>(reader.read_unsigned_varint()) - 1;
        } else {
            resource_count = reader.read_int32();
        }
        
        std::cout << "    resource_count=" << resource_count << "\n";
        
        for (int32_t i = 0; i < resource_count && i < 100; ++i) {
            ConfigResource resource;
            resource.resource_type = reader.read_int8();
            if (flexible) {
                resource.resource_name = reader.read_compact_string();
            } else {
                resource.resource_name = reader.read_string();
            }
            
            std::cout << "    resource: type=" << (int)resource.resource_type 
                      << " name=" << resource.resource_name << "\n";
            
            // Skip configuration_keys array (nullable)
            int32_t key_count;
            if (flexible) {
                key_count = static_cast<int32_t>(reader.read_unsigned_varint()) - 1;
            } else {
                key_count = reader.read_int32();
            }
            if (key_count >= 0) {
                for (int32_t k = 0; k < key_count && k < 1000; ++k) {
                    if (flexible) {
                        reader.read_compact_string();
                    } else {
                        reader.read_string();
                    }
                }
            }
            
            if (flexible) {
                reader.read_unsigned_varint(); // tagged fields for resource
            }
            
            resources.push_back(resource);
        }
        
        // include_synonyms (v1+)
        if (header.api_version >= 1) {
            reader.read_int8();
        }
        
        // include_documentation (v3+)
        if (header.api_version >= 3) {
            reader.read_int8();
        }
        
        if (flexible) {
            reader.read_unsigned_varint(); // request tagged fields
        }
    } catch (...) {
        // Ignore parse errors, just return empty results for what we have
    }
    
    BufferWriter writer;
    
    // Response header
    writer.write_int32(header.correlation_id);
    if (flexible) {
        writer.write_unsigned_varint(0); // header tagged fields
    }
    
    // Throttle time
    writer.write_int32(0);
    
    // Results array - one entry for each requested resource
    if (flexible) {
        writer.write_unsigned_varint(static_cast<uint32_t>(resources.size() + 1));
    } else {
        writer.write_int32(static_cast<int32_t>(resources.size()));
    }
    
    for (const auto& resource : resources) {
        // error_code
        writer.write_int16(0); // NONE
        
        // error_message (nullable)
        if (flexible) {
            writer.write_unsigned_varint(0); // null compact string
        } else {
            writer.write_int16(-1); // null string
        }
        
        // resource_type
        writer.write_int8(resource.resource_type);
        
        // resource_name
        if (flexible) {
            writer.write_compact_string(resource.resource_name);
        } else {
            writer.write_string(resource.resource_name);
        }
        
        // configs array - return common topic configs for TOPIC resource type (2)
        std::vector<std::pair<std::string, std::string>> configs;
        if (resource.resource_type == 2) { // TOPIC
            configs = {
                {"cleanup.policy", "delete"},
                {"compression.type", "producer"},
                {"delete.retention.ms", "86400000"},
                {"file.delete.delay.ms", "60000"},
                {"flush.messages", "9223372036854775807"},
                {"flush.ms", "9223372036854775807"},
                {"index.interval.bytes", "4096"},
                {"max.compaction.lag.ms", "9223372036854775807"},
                {"max.message.bytes", "1048588"},
                {"message.timestamp.type", "CreateTime"},
                {"min.cleanable.dirty.ratio", "0.5"},
                {"min.compaction.lag.ms", "0"},
                {"min.insync.replicas", "1"},
                {"retention.bytes", "-1"},
                {"retention.ms", "604800000"},
                {"segment.bytes", "1073741824"},
                {"segment.index.bytes", "10485760"},
                {"segment.jitter.ms", "0"},
                {"segment.ms", "604800000"},
            };
        }
        
        if (flexible) {
            writer.write_unsigned_varint(static_cast<uint32_t>(configs.size() + 1));
        } else {
            writer.write_int32(static_cast<int32_t>(configs.size()));
        }
        
        for (const auto& [name, value] : configs) {
            // config_name
            if (flexible) {
                writer.write_compact_string(name);
            } else {
                writer.write_string(name);
            }
            
            // config_value (nullable)
            if (flexible) {
                writer.write_compact_nullable_string(value);
            } else {
                writer.write_string(value);
            }
            
            // read_only
            writer.write_bool(false);
            
            // config_source (v1+): 5 = DEFAULT_CONFIG
            if (header.api_version >= 1) {
                writer.write_int8(5);
            }
            
            // is_sensitive
            writer.write_bool(false);
            
            // synonyms (v1+) - empty array
            if (header.api_version >= 1) {
                if (flexible) {
                    writer.write_unsigned_varint(1); // 0 + 1
                } else {
                    writer.write_int32(0);
                }
            }
            
            // config_type (v3+): 2 = STRING
            if (header.api_version >= 3) {
                writer.write_int8(2);
            }
            
            // documentation (v3+) - nullable
            if (header.api_version >= 3) {
                if (flexible) {
                    writer.write_unsigned_varint(0); // null
                } else {
                    writer.write_int16(-1);
                }
            }
            
            // tagged fields for config (flexible)
            if (flexible) {
                writer.write_unsigned_varint(0);
            }
        }
        
        if (flexible) {
            writer.write_unsigned_varint(0); // tagged fields for result
        }
    }
    
    // Response-level tagged fields (flexible)
    if (flexible) {
        writer.write_unsigned_varint(0);
    }
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_create_topics(
    const RequestHeader& header, BufferReader& reader) {
    
    // CreateTopics v5+ uses flexible format
    bool flexible = (header.api_version >= 5);
    
    // Simplified parsing: only read topic name, num_partitions, replication_factor
    // Skip everything else to avoid parsing errors
    
    struct CreateTopicInfo {
        std::string name;
        int32_t num_partitions;
        int16_t replication_factor;
    };
    std::vector<CreateTopicInfo> topics;
    
    try {
        // Read topics array
        int32_t topic_count;
        if (flexible) {
            topic_count = static_cast<int32_t>(reader.read_unsigned_varint()) - 1;
        } else {
            topic_count = reader.read_int32();
        }
        
        for (int32_t i = 0; i < topic_count && i < 100; ++i) {
            CreateTopicInfo topic;
            
            // Topic name
            if (flexible) {
                topic.name = reader.read_compact_string();
            } else {
                topic.name = reader.read_string();
            }
            
            topic.num_partitions = reader.read_int32();
            topic.replication_factor = reader.read_int16();
            
            // Skip replica assignments
            int32_t assignment_count;
            if (flexible) {
                assignment_count = static_cast<int32_t>(reader.read_unsigned_varint()) - 1;
            } else {
                assignment_count = reader.read_int32();
            }
            for (int32_t a = 0; a < assignment_count && a < 1000; ++a) {
                reader.read_int32(); // partition
                int32_t replica_count;
                if (flexible) {
                    replica_count = static_cast<int32_t>(reader.read_unsigned_varint()) - 1;
                } else {
                    replica_count = reader.read_int32();
                }
                for (int32_t r = 0; r < replica_count && r < 100; ++r) {
                    reader.read_int32(); // broker_id
                }
                if (flexible) {
                    reader.read_unsigned_varint(); // tagged fields for assignment
                }
            }
            
            // Skip configs
            int32_t config_count;
            if (flexible) {
                config_count = static_cast<int32_t>(reader.read_unsigned_varint()) - 1;
            } else {
                config_count = reader.read_int32();
            }
            for (int32_t c = 0; c < config_count && c < 1000; ++c) {
                if (flexible) {
                    reader.read_compact_string(); // key
                    reader.read_compact_nullable_string(); // value
                } else {
                    reader.read_string(); // key
                    reader.read_nullable_string(); // value
                }
                if (flexible) {
                    reader.read_unsigned_varint(); // tagged fields for config
                }
            }
            
            if (flexible) {
                reader.read_unsigned_varint(); // tagged fields for topic
            }
            
            topics.push_back(topic);
            
            // Store the topic for future metadata requests
            TopicInfo topic_info;
            topic_info.name = topic.name;
            topic_info.num_partitions = topic.num_partitions > 0 ? topic.num_partitions : 1;
            topic_info.replication_factor = topic.replication_factor > 0 ? topic.replication_factor : 1;
            topic_info.is_internal = false;
            add_topic(topic_info);
            
            std::cout << "  Created topic: " << topic_info.name 
                      << " partitions=" << topic_info.num_partitions << "\n";
        }
    } catch (const std::exception& e) {
        std::cerr << "CreateTopics parse warning (continuing): " << e.what() << "\n";
        // Continue with whatever topics we managed to parse
    }
    
    BufferWriter writer;
    
    // Response header
    writer.write_int32(header.correlation_id);
    if (flexible) {
        writer.write_unsigned_varint(0); // header tagged fields
    }
    
    // Throttle time (v2+)
    if (header.api_version >= 2) {
        writer.write_int32(0);
    }
    
    // Topics response array
    if (flexible) {
        writer.write_unsigned_varint(static_cast<uint32_t>(topics.size() + 1));
    } else {
        writer.write_int32(static_cast<int32_t>(topics.size()));
    }
    
    for (const auto& topic : topics) {
        // Topic name
        if (flexible) {
            writer.write_compact_string(topic.name);
        } else {
            writer.write_string(topic.name);
        }
        
        // Topic ID (v7+) - UUID (16 bytes of zeros for now)
        if (header.api_version >= 7) {
            for (int j = 0; j < 16; ++j) writer.write_int8(0);
        }
        
        // Error code
        writer.write_int16(static_cast<int16_t>(ErrorCode::None));
        
        // Error message (v1+)
        if (header.api_version >= 1) {
            if (flexible) {
                writer.write_unsigned_varint(0); // null compact string
            } else {
                writer.write_nullable_string(nullptr);
            }
        }
        
        // Topic config info (v5+)
        if (header.api_version >= 5) {
            writer.write_int32(topic.num_partitions);
            writer.write_int16(topic.replication_factor);
            if (flexible) {
                writer.write_unsigned_varint(1); // empty configs array
            } else {
                writer.write_int32(0); // configs count
            }
        }
        
        if (flexible) {
            writer.write_unsigned_varint(0); // tagged fields for topic response
        }
    }
    
    if (flexible) {
        writer.write_unsigned_varint(0); // response-level tagged fields
    }
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_delete_topics(
    const RequestHeader& header, BufferReader& reader) {
    
    // v4+ uses flexible format
    bool flexible = (header.api_version >= 4);
    
    std::vector<std::string> topic_names;
    
    if (flexible) {
        // Skip leading 0x00 byte if present (workaround)
        const uint8_t* ptr = reader.current();
        size_t rem = reader.remaining();
        if (rem >= 2 && ptr[0] == 0x00 && ptr[1] >= 0x01 && ptr[1] <= 0x20) {
            reader.skip(1);
        }
        
        // COMPACT_ARRAY of topics
        uint32_t topic_count = reader.read_unsigned_varint();
        if (topic_count > 0) {
            topic_count--; // compact array uses length + 1
            for (uint32_t i = 0; i < topic_count; ++i) {
                // Topic name (compact string)
                std::string topic_name = reader.read_compact_string();
                topic_names.push_back(topic_name);
                
                // Tagged fields for each topic entry
                reader.read_unsigned_varint();
            }
        }
        
        // timeout_ms
        reader.read_int32();
        
        // Tagged fields at end of request
        reader.read_unsigned_varint();
    } else {
        // Non-flexible format
        int32_t topic_count = reader.read_int32();
        for (int32_t i = 0; i < topic_count; ++i) {
            topic_names.push_back(reader.read_string());
        }
        
        // timeout
        reader.read_int32();
    }
    
    // Remove topics from stored topics
    {
        std::lock_guard<std::mutex> lock(topics_mutex_);
        for (const auto& name : topic_names) {
            stored_topics_.erase(
                std::remove_if(stored_topics_.begin(), stored_topics_.end(),
                    [&name](const TopicInfo& t) { return t.name == name; }),
                stored_topics_.end());
        }
    }
    
    std::cout << "  Deleted topics: ";
    for (const auto& n : topic_names) std::cout << n << " ";
    std::cout << "\n";
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    if (flexible) {
        // Header tagged fields
        writer.write_unsigned_varint(0);
    }
    
    // Throttle time (v1+)
    if (header.api_version >= 1) {
        writer.write_int32(0);
    }
    
    // Topics response array
    if (flexible) {
        writer.write_unsigned_varint(static_cast<uint32_t>(topic_names.size() + 1));
    } else {
        writer.write_int32(static_cast<int32_t>(topic_names.size()));
    }
    
    for (const auto& topic_name : topic_names) {
        if (flexible) {
            // Topic name (compact string)
            writer.write_unsigned_varint(static_cast<uint32_t>(topic_name.size() + 1));
            for (char c : topic_name) writer.write_int8(static_cast<int8_t>(c));
        } else {
            writer.write_string(topic_name);
        }
        
        // Topic ID (v6+) - 16 bytes UUID
        if (header.api_version >= 6) {
            for (int i = 0; i < 16; i++) writer.write_int8(0);
        }
        
        // Error code
        writer.write_int16(static_cast<int16_t>(ErrorCode::None));
        
        // Error message (v5+, nullable compact string)
        if (header.api_version >= 5) {
            if (flexible) {
                writer.write_unsigned_varint(0); // null
            } else {
                writer.write_int16(-1); // null
            }
        }
        
        // Tagged fields for each topic (flexible)
        if (flexible) {
            writer.write_unsigned_varint(0);
        }
    }
    
    // Tagged fields at end (flexible)
    if (flexible) {
        writer.write_unsigned_varint(0);
    }
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_init_producer_id(
    const RequestHeader& header, BufferReader& reader) {
    
    // InitProducerId v2+ uses flexible format
    bool flexible = (header.api_version >= 2);
    
    // Parse request (we just need to read it, not use it for now)
    // transactional_id (nullable string)
    // transaction_timeout_ms
    // producer_id (v3+)
    // producer_epoch (v3+)
    
    if (flexible) {
        // Skip the request body - we'll generate a new producer ID
        // In a real implementation, we would parse and validate
    }
    
    // Generate a unique producer ID (simple incrementing counter)
    static std::atomic<int64_t> next_producer_id{1000};
    int64_t producer_id = next_producer_id++;
    int16_t producer_epoch = 0;
    
    std::cout << "  InitProducerId: assigned producer_id=" << producer_id << "\n";
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    // Response header v1 for flexible versions - TAG_BUFFER
    if (flexible) {
        writer.write_unsigned_varint(0);  // empty tagged fields in header
    }
    
    // Throttle time (all versions)
    writer.write_int32(0);
    
    // Error code
    writer.write_int16(static_cast<int16_t>(ErrorCode::None));
    
    // Producer ID
    writer.write_int64(producer_id);
    
    // Producer epoch
    writer.write_int16(producer_epoch);
    
    // Tagged fields for flexible versions
    if (flexible) {
        writer.write_unsigned_varint(0);
    }
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_create_partitions(
    const RequestHeader& header, BufferReader& /*reader*/) {
    
    // CreatePartitions v2+ uses flexible format
    bool flexible = (header.api_version >= 2);
    
    // Skip parsing the request body - just return success for all topics
    // In a real implementation, we would parse the topics and create partitions
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    // Response header v1 for flexible versions - TAG_BUFFER
    if (flexible) {
        writer.write_unsigned_varint(0);  // empty tagged fields in header
    }
    
    // Throttle time
    writer.write_int32(0);
    
    // Results array - empty (no errors to report)
    if (flexible) {
        writer.write_unsigned_varint(1);  // COMPACT_ARRAY length 0 + 1
    } else {
        writer.write_int32(0);
    }
    
    // Tagged fields for flexible versions
    if (flexible) {
        writer.write_unsigned_varint(0);
    }
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::handle_describe_log_dirs(
    const RequestHeader& header, BufferReader& /*reader*/) {
    
    // DescribeLogDirs v2+ uses flexible format
    bool flexible = (header.api_version >= 2);
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    // Response header v1 for flexible versions - TAG_BUFFER
    if (flexible) {
        writer.write_unsigned_varint(0);  // empty tagged fields in header
    }
    
    // Throttle time (v0+)
    writer.write_int32(0);
    
    // Error code (v3+)
    if (header.api_version >= 3) {
        writer.write_int16(0);  // NONE
    }
    
    // Get topics
    std::vector<TopicInfo> topics;
    if (topics_callback_) {
        topics = topics_callback_();
    } else {
        std::lock_guard<std::mutex> lock(topics_mutex_);
        topics = stored_topics_;
    }
    
    // Calculate total size from partition details if available
    int64_t total_bytes = 0;
    for (const auto& topic : topics) {
        if (!topic.partition_details.empty()) {
            for (const auto& pd : topic.partition_details) {
                total_bytes += pd.size_bytes;
            }
        } else {
            total_bytes += topic.num_partitions * 1024;  // 1KB fallback per partition
        }
    }
    
    // Results array - one log directory
    if (flexible) {
        writer.write_unsigned_varint(2);  // 1 element + 1
    } else {
        writer.write_int32(1);
    }
    
    // Log directory result
    // error_code
    writer.write_int16(0);  // NONE
    
    // log_dir (path)
    std::string log_dir = "./data";
    if (flexible) {
        writer.write_compact_string(log_dir);
    } else {
        writer.write_string(log_dir);
    }
    
    // Topics array
    if (flexible) {
        writer.write_unsigned_varint(static_cast<uint32_t>(topics.size()) + 1);
    } else {
        writer.write_int32(static_cast<int32_t>(topics.size()));
    }
    
    for (const auto& topic : topics) {
        // topic_name
        if (flexible) {
            writer.write_compact_string(topic.name);
        } else {
            writer.write_string(topic.name);
        }
        
        // partitions array - use partition_details if available
        int32_t partition_count = topic.partition_details.empty() 
            ? topic.num_partitions 
            : static_cast<int32_t>(topic.partition_details.size());
            
        if (flexible) {
            writer.write_unsigned_varint(static_cast<uint32_t>(partition_count) + 1);
        } else {
            writer.write_int32(partition_count);
        }
        
        for (int32_t p = 0; p < partition_count; ++p) {
            // partition_index
            writer.write_int32(p);
            
            // partition_size - use real size if available
            int64_t partition_size = 1024;  // fallback
            if (p < static_cast<int32_t>(topic.partition_details.size())) {
                partition_size = topic.partition_details[p].size_bytes;
            }
            writer.write_int64(partition_size);
            
            // offset_lag
            writer.write_int64(0);  // No lag for local storage
            
            // is_future_key
            writer.write_bool(false);
            
            // Tagged fields for partition (flexible)
            if (flexible) {
                writer.write_unsigned_varint(0);
            }
        }
        
        // Tagged fields for topic (flexible)
        if (flexible) {
            writer.write_unsigned_varint(0);
        }
    }
    
    // total_bytes (v4+)
    if (header.api_version >= 4) {
        writer.write_int64(total_bytes);
        
        // usable_bytes (v4+)
        writer.write_int64(total_bytes * 10);  // Assume 10x available space
    }
    
    // Tagged fields for log_dir (flexible)
    if (flexible) {
        writer.write_unsigned_varint(0);
    }
    
    // Tagged fields for response (flexible)
    if (flexible) {
        writer.write_unsigned_varint(0);
    }
    
    return writer.data();
}

std::vector<uint8_t> KafkaProtocolHandler::make_error_response(
    const RequestHeader& header, ErrorCode error) {
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    writer.write_int16(static_cast<int16_t>(error));
    return writer.data();
}

} // namespace protocol
} // namespace eventhorizon
