# Arquitetura do Event Horizon

## Visão Geral

O Event Horizon é um broker de mensagens compatível com o protocolo Kafka, implementado em C++23 para alta performance.

```
┌─────────────────────────────────────────────────────────────────┐
│                         Event Horizon                            │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   Network   │  │  Protocol   │  │        Broker           │  │
│  │   Server    │──│   Handler   │──│  ┌───────────────────┐  │  │
│  │  (TCP/IP)   │  │   (Kafka)   │  │  │  Topic Manager    │  │  │
│  └─────────────┘  └─────────────┘  │  └───────────────────┘  │  │
│                                     │  ┌───────────────────┐  │  │
│                                     │  │ Consumer Groups   │  │  │
│                                     │  └───────────────────┘  │  │
│                                     └─────────────────────────┘  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                      Storage Layer                          ││
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    ││
│  │  │Partition │  │Partition │  │Partition │  │Partition │    ││
│  │  │  0       │  │  1       │  │  2       │  │  N       │    ││
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘    ││
│  │       │             │             │             │          ││
│  │  ┌────▼─────┐  ┌────▼─────┐  ┌────▼─────┐  ┌────▼─────┐    ││
│  │  │   Log    │  │   Log    │  │   Log    │  │   Log    │    ││
│  │  │ Segments │  │ Segments │  │ Segments │  │ Segments │    ││
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘    ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │   File System   │
                    │   (data/*.log)  │
                    └─────────────────┘
```

## Componentes

### Network Server

- **Tecnologia**: Boost.Asio (assíncrono)
- **Porta padrão**: 9092
- **Protocolo**: TCP binário (Kafka wire protocol)

```cpp
class Server {
    boost::asio::io_context io_context_;
    tcp::acceptor acceptor_;
    
    void accept_connections();
    void handle_client(tcp::socket socket);
};
```

### Protocol Handler

Implementa o protocolo binário Kafka:

| API Key | Nome | Versões | Status |
|---------|------|---------|--------|
| 0 | Produce | 0-9 | ✅ |
| 1 | Fetch | 0-16 | ✅ |
| 3 | Metadata | 0-12 | ✅ |
| 10 | FindCoordinator | 0-5 | ✅ |
| 11 | JoinGroup | 0-9 | ✅ |
| 12 | Heartbeat | 0-4 | ✅ |
| 13 | LeaveGroup | 0-5 | ✅ |
| 14 | SyncGroup | 0-5 | ✅ |
| 18 | ApiVersions | 0-4 | ✅ |
| 19 | CreateTopics | 0-7 | ✅ |
| 20 | DeleteTopics | 0-6 | ✅ |
| 21 | DeleteRecords | 0-2 | ✅ |

### Broker

Gerencia tópicos e coordena operações:

```cpp
class Broker {
    std::unordered_map<std::string, std::vector<Partition>> topics_;
    ConsumerGroupManager group_manager_;
    
    bool create_topic(const std::string& name, int partitions);
    bool delete_topic(const std::string& name, bool purge = true);
    Partition* get_partition(const std::string& topic, int partition);
};
```

### Storage Layer

#### Partition

Representa uma partição de um tópico:

```cpp
class Partition {
    std::vector<std::unique_ptr<LogSegment>> segments_;
    int64_t log_start_offset_;
    int64_t log_end_offset_;
    
    int64_t append(const Message& msg);
    std::vector<Message> read(int64_t offset, int max_bytes);
    void truncate();
};
```

#### Log Segment

Arquivo físico no disco:

```
data/
├── topic-0/
│   ├── 00000000000000000000.log    # Dados
│   └── 00000000000000000000.index  # Índice
├── topic-1/
│   └── ...
```

Formato do log:
```
┌────────────────────────────────────────┐
│ Offset (8 bytes)                       │
├────────────────────────────────────────┤
│ Size (4 bytes)                         │
├────────────────────────────────────────┤
│ CRC (4 bytes)                          │
├────────────────────────────────────────┤
│ Magic (1 byte)                         │
├────────────────────────────────────────┤
│ Attributes (1 byte)                    │
├────────────────────────────────────────┤
│ Timestamp (8 bytes)                    │
├────────────────────────────────────────┤
│ Key Length (4 bytes)                   │
├────────────────────────────────────────┤
│ Key (variable)                         │
├────────────────────────────────────────┤
│ Value Length (4 bytes)                 │
├────────────────────────────────────────┤
│ Value (variable)                       │
└────────────────────────────────────────┘
```

### Consumer Groups

Gerencia grupos de consumidores com rebalanceamento:

```cpp
class ConsumerGroup {
    std::string group_id_;
    std::string protocol_type_;
    int32_t generation_id_;
    std::unordered_map<std::string, MemberInfo> members_;
    
    JoinResult join(const std::string& member_id, ...);
    SyncResult sync(const std::string& member_id, ...);
    void heartbeat(const std::string& member_id);
};
```

## Fluxo de Dados

### Produce

```
Client → Server → Protocol Handler → Broker → Partition → LogSegment → Disk
```

1. Cliente envia ProduceRequest
2. Server recebe bytes via TCP
3. Protocol Handler decodifica a requisição
4. Broker roteia para a partição correta
5. Partition append a mensagem
6. LogSegment escreve no disco
7. Response com offset é retornada

### Fetch

```
Client ← Server ← Protocol Handler ← Broker ← Partition ← LogSegment ← Disk
```

1. Cliente envia FetchRequest com offset
2. Partition localiza mensagens a partir do offset
3. LogSegment lê do disco
4. Mensagens são codificadas e enviadas

## Thread Safety

- **Broker**: Thread-safe com mutex por operação
- **Partition**: Thread-safe com shared_mutex (readers-writer lock)
- **LogSegment**: Thread-safe para append (mutex)
- **ConsumerGroup**: Thread-safe com mutex

## Performance Considerations

- Zero-copy onde possível
- Memory-mapped files para leitura (planejado)
- Batch writes para melhor throughput
- Index para busca rápida por offset
