# API Reference

Referência das APIs internas do Event Horizon para desenvolvedores.

## Broker

### Classe `Broker`

```cpp
#include "broker/broker.hpp"

class Broker {
public:
    explicit Broker(const BrokerConfig& config);
    
    // Topic Management
    bool create_topic(const std::string& name, int num_partitions);
    bool delete_topic(const std::string& name, bool purge_data = true);
    bool topic_exists(const std::string& name) const;
    std::vector<std::string> list_topics() const;
    bool recreate_topic(const std::string& name, int num_partitions);
    
    // Partition Access
    Partition* get_partition(const std::string& topic, int partition);
    int get_partition_count(const std::string& topic) const;
    
    // Records Management
    int16_t delete_records(const std::string& topic, int partition, int64_t before_offset);
    bool clear_topic_messages(const std::string& topic);
    
    // Consumer Groups
    ConsumerGroup* get_or_create_group(const std::string& group_id);
    ConsumerGroup* get_group(const std::string& group_id);
};
```

### Exemplo

```cpp
BrokerConfig config;
config.data_dir = "./data";
config.broker_id = 1;

Broker broker(config);

// Criar tópico
broker.create_topic("my-topic", 3);

// Obter partição
auto* partition = broker.get_partition("my-topic", 0);

// Produzir mensagem
partition->append(message);
```

---

## Partition

### Classe `Partition`

```cpp
#include "storage/partition.hpp"

class Partition {
public:
    Partition(const std::string& topic, int id, const std::string& data_dir);
    
    // Write Operations
    int64_t append(const Message& message);
    int64_t append_batch(const std::vector<Message>& messages);
    
    // Read Operations
    std::vector<Message> read(int64_t start_offset, int32_t max_bytes);
    std::optional<Message> read_one(int64_t offset);
    
    // Offset Management
    int64_t log_start_offset() const;
    int64_t log_end_offset() const;
    int64_t high_watermark() const;
    
    // Maintenance
    int64_t delete_records_before(int64_t offset);
    void truncate();
    void flush();
};
```

### Exemplo

```cpp
Partition partition("my-topic", 0, "./data");

// Append
Message msg;
msg.key = "key1";
msg.value = "value1";
int64_t offset = partition.append(msg);

// Read
auto messages = partition.read(0, 1024 * 1024);

// Truncate
partition.truncate();
```

---

## Log Segment

### Classe `LogSegment`

```cpp
#include "storage/log_segment.hpp"

class LogSegment {
public:
    LogSegment(const std::filesystem::path& path, int64_t base_offset);
    
    // Write
    int64_t append(const Message& message);
    
    // Read
    std::vector<Message> read(int64_t start_offset, int32_t max_bytes);
    
    // Properties
    int64_t base_offset() const;
    int64_t next_offset() const;
    size_t size_bytes() const;
    
    // Maintenance
    void flush();
    void close();
};
```

---

## Consumer Group

### Classe `ConsumerGroup`

```cpp
#include "consumer/consumer_group.hpp"

struct JoinResult {
    int16_t error_code;
    int32_t generation_id;
    std::string protocol_name;
    std::string leader_id;
    std::string member_id;
    std::vector<MemberInfo> members;  // Only for leader
};

struct SyncResult {
    int16_t error_code;
    std::vector<uint8_t> assignment;
};

class ConsumerGroup {
public:
    explicit ConsumerGroup(const std::string& group_id);
    
    // Membership
    JoinResult join(
        const std::string& member_id,
        std::optional<std::string> group_instance_id,
        const std::string& client_id,
        const std::string& client_host,
        int32_t session_timeout_ms,
        int32_t rebalance_timeout_ms,
        const std::string& protocol_type,
        const std::vector<Protocol>& protocols
    );
    
    SyncResult sync(
        const std::string& member_id,
        int32_t generation_id,
        const std::vector<Assignment>& assignments
    );
    
    int16_t heartbeat(const std::string& member_id, int32_t generation_id);
    int16_t leave(const std::string& member_id);
    
    // State
    std::string_view group_id() const;
    std::string_view protocol_type() const;
    std::string_view protocol_name() const;
    int32_t generation_id() const;
    GroupState state() const;
};
```

---

## Kafka Protocol

### Classe `KafkaProtocolHandler`

```cpp
#include "protocol/kafka_protocol.hpp"

class KafkaProtocolHandler {
public:
    explicit KafkaProtocolHandler(Broker& broker);
    
    std::vector<uint8_t> handle_request(const std::vector<uint8_t>& request);
    
private:
    std::vector<uint8_t> handle_api_versions(const RequestHeader& header);
    std::vector<uint8_t> handle_metadata(const RequestHeader& header, BufferReader& reader);
    std::vector<uint8_t> handle_produce(const RequestHeader& header, BufferReader& reader);
    std::vector<uint8_t> handle_fetch(const RequestHeader& header, BufferReader& reader);
    std::vector<uint8_t> handle_create_topics(const RequestHeader& header, BufferReader& reader);
    std::vector<uint8_t> handle_delete_topics(const RequestHeader& header, BufferReader& reader);
    std::vector<uint8_t> handle_delete_records(const RequestHeader& header, BufferReader& reader);
    // ... mais handlers
};
```

### Buffer Utilities

```cpp
class BufferReader {
public:
    explicit BufferReader(const std::vector<uint8_t>& data);
    
    int8_t read_int8();
    int16_t read_int16();
    int32_t read_int32();
    int64_t read_int64();
    std::string read_string();
    std::string read_compact_string();
    std::vector<uint8_t> read_bytes();
    
    template<typename T>
    std::vector<T> read_array(std::function<T()> reader);
};

class BufferWriter {
public:
    void write_int8(int8_t value);
    void write_int16(int16_t value);
    void write_int32(int32_t value);
    void write_int64(int64_t value);
    void write_string(const std::string& value);
    void write_compact_string(const std::string& value);
    void write_bytes(const std::vector<uint8_t>& data);
    
    std::vector<uint8_t> data() const;
};
```

---

## Error Codes

```cpp
namespace ErrorCode {
    constexpr int16_t NONE = 0;
    constexpr int16_t OFFSET_OUT_OF_RANGE = 1;
    constexpr int16_t UNKNOWN_TOPIC_OR_PARTITION = 3;
    constexpr int16_t NOT_LEADER_OR_FOLLOWER = 6;
    constexpr int16_t GROUP_COORDINATOR_NOT_AVAILABLE = 15;
    constexpr int16_t UNKNOWN_MEMBER_ID = 25;
    constexpr int16_t REBALANCE_IN_PROGRESS = 27;
    constexpr int16_t TOPIC_ALREADY_EXISTS = 36;
    constexpr int16_t MEMBER_ID_REQUIRED = 79;
}
```
