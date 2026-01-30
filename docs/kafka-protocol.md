# Protocolo Kafka

O Event Horizon implementa o protocolo binário Kafka para compatibilidade com clientes existentes.

## Visão Geral

O protocolo Kafka usa um formato binário sobre TCP:

```
┌─────────────────────────────────────────┐
│ Request/Response Size (4 bytes, int32)  │
├─────────────────────────────────────────┤
│ Request Header                          │
├─────────────────────────────────────────┤
│ Request Body                            │
└─────────────────────────────────────────┘
```

## Request Header

```
┌────────────────────────────────────┐
│ API Key (2 bytes, int16)           │
├────────────────────────────────────┤
│ API Version (2 bytes, int16)       │
├────────────────────────────────────┤
│ Correlation ID (4 bytes, int32)    │
├────────────────────────────────────┤
│ Client ID (string)                 │
└────────────────────────────────────┘
```

## APIs Suportadas

### ApiVersions (Key 18)

Retorna as versões de API suportadas pelo broker.

**Versões**: 0-4

```
Request: (vazio)
Response: 
  - error_code: int16
  - api_versions: [api_key, min_version, max_version]
```

### Metadata (Key 3)

Retorna informações sobre tópicos e partições.

**Versões**: 0-12

```
Request:
  - topics: [string] ou null (todos)
  
Response:
  - brokers: [node_id, host, port]
  - topics: [name, partitions: [id, leader, replicas, isr]]
```

### Produce (Key 0)

Produz mensagens para tópicos.

**Versões**: 0-9

```
Request:
  - acks: int16 (-1, 0, 1)
  - timeout_ms: int32
  - topics: [name, partitions: [id, records]]
  
Response:
  - topics: [name, partitions: [id, error_code, base_offset]]
```

### Fetch (Key 1)

Consome mensagens de partições.

**Versões**: 0-16

```
Request:
  - max_wait_ms: int32
  - min_bytes: int32
  - topics: [name, partitions: [id, fetch_offset, max_bytes]]
  
Response:
  - topics: [name, partitions: [id, error_code, hw, records]]
```

### CreateTopics (Key 19)

Cria novos tópicos.

**Versões**: 0-7

```
Request:
  - topics: [name, num_partitions, replication_factor]
  - timeout_ms: int32
  
Response:
  - topics: [name, error_code]
```

### DeleteTopics (Key 20)

Remove tópicos existentes.

**Versões**: 0-6

```
Request:
  - topics: [name]
  - timeout_ms: int32
  
Response:
  - topics: [name, error_code]
```

### DeleteRecords (Key 21)

Limpa mensagens antes de um offset específico.

**Versões**: 0-2

```
Request:
  - topics: [name, partitions: [id, offset]]
  - timeout_ms: int32
  
Response:
  - topics: [name, partitions: [id, error_code, low_watermark]]
```

### FindCoordinator (Key 10)

Encontra o coordenador para um grupo.

**Versões**: 0-5

```
Request:
  - key: string (group_id)
  - key_type: int8 (0=group, 1=transaction)
  
Response:
  - error_code: int16
  - node_id, host, port
```

### JoinGroup (Key 11)

Junta um membro a um consumer group.

**Versões**: 0-9

```
Request:
  - group_id: string
  - session_timeout_ms: int32
  - member_id: string
  - protocol_type: string
  - protocols: [name, metadata]
  
Response:
  - error_code: int16
  - generation_id: int32
  - protocol_name: string
  - leader: string
  - member_id: string
  - members: [member_id, metadata]
```

### SyncGroup (Key 14)

Sincroniza estado do grupo após rebalance.

**Versões**: 0-5

```
Request:
  - group_id: string
  - generation_id: int32
  - member_id: string
  - assignments: [member_id, assignment]
  
Response:
  - error_code: int16
  - assignment: bytes
```

### Heartbeat (Key 12)

Mantém membro ativo no grupo.

**Versões**: 0-4

```
Request:
  - group_id: string
  - generation_id: int32
  - member_id: string
  
Response:
  - error_code: int16
```

### LeaveGroup (Key 13)

Remove membro do grupo.

**Versões**: 0-5

```
Request:
  - group_id: string
  - member_id: string (v0-2)
  - members: [member_id, reason] (v3+)
  
Response:
  - error_code: int16
```

## Códigos de Erro

| Código | Nome | Descrição |
|--------|------|-----------|
| 0 | NONE | Sucesso |
| 1 | OFFSET_OUT_OF_RANGE | Offset inválido |
| 3 | UNKNOWN_TOPIC_OR_PARTITION | Tópico/partição não existe |
| 6 | NOT_LEADER_OR_FOLLOWER | Não é o líder |
| 15 | GROUP_COORDINATOR_NOT_AVAILABLE | Coordenador indisponível |
| 25 | UNKNOWN_MEMBER_ID | Membro desconhecido |
| 27 | REBALANCE_IN_PROGRESS | Rebalance em andamento |
| 36 | TOPIC_ALREADY_EXISTS | Tópico já existe |
| 79 | MEMBER_ID_REQUIRED | Member ID obrigatório |

## Flexible Format (v2+)

Versões mais recentes usam "flexible format":

- Strings: COMPACT_STRING (varint length)
- Arrays: COMPACT_ARRAY (varint length)
- Tagged fields no final

```cpp
// Varint encoding
void write_varint(int32_t value);
void write_compact_string(const std::string& s);
void write_compact_array<T>(const std::vector<T>& arr);
```

## Compatibilidade

O Event Horizon é testado com:

- Kafka clients 3.x e 4.x
- Kafka UI
- librdkafka
- kafka-python
- franz-go
