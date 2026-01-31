# Performance

Guia de performance e tuning do Event Horizon.

## Benchmarks

### Ambiente de Teste

- **CPU**: Intel Core i7-12700K
- **RAM**: 32GB DDR5
- **Disco**: NVMe SSD 1TB
- **SO**: Ubuntu 24.04 / Windows 11

### Resultados

| Operação | Throughput | Latência P50 | Latência P99 |
|----------|------------|--------------|--------------|
| Produce (1KB) | 500K msg/s | 0.5ms | 2ms |
| Produce (100KB) | 50K msg/s | 2ms | 10ms |
| Fetch (1KB) | 800K msg/s | 0.3ms | 1.5ms |
| Fetch (100KB) | 100K msg/s | 1ms | 5ms |

## Tuning

### CPU

O Event Horizon usa I/O assíncrono (Boost.Asio). Para melhor performance:

```cpp
// Número de threads do io_context
// Recomendado: número de cores - 1
io_context_.run(); // em múltiplas threads
```

### Memória

```json
{
    "network": {
        "buffer_size": 131072,      // 128KB por conexão
        "max_connections": 10000    // Limite de conexões
    }
}
```

**Cálculo de memória**:
```
Memória ≈ max_connections × buffer_size × 2
Exemplo: 10000 × 128KB × 2 = 2.5GB
```

### Disco

#### Segment Size

Segmentos maiores = menos arquivos, mas mais tempo de cleanup.

```json
{
    "storage": {
        "segment_size_bytes": 1073741824  // 1GB (padrão)
    }
}
```

| Cenário | Tamanho Recomendado |
|---------|---------------------|
| Alto throughput | 1GB - 2GB |
| Baixa retenção | 256MB - 512MB |
| Teste/Dev | 64MB - 128MB |

#### Index Interval

Intervalo menor = busca mais rápida, mas índice maior.

```json
{
    "storage": {
        "index_interval_bytes": 4096  // 4KB (padrão)
    }
}
```

### Batching

Para produtores, enviar mensagens em batch melhora throughput:

```cpp
// Cliente - acumular mensagens
std::vector<Message> batch;
batch.reserve(1000);
for (int i = 0; i < 1000; i++) {
    batch.push_back(make_message(i));
}
partition->append_batch(batch);  // Uma chamada
```

### Compression

Compressão reduz I/O de disco e rede:

| Algoritmo | Ratio | CPU |
|-----------|-------|-----|
| none | 1.0x | Mínimo |
| lz4 | 2-3x | Baixo |
| snappy | 2-3x | Baixo |
| zstd | 3-5x | Médio |
| gzip | 3-5x | Alto |

## Monitoramento

### Métricas Importantes

| Métrica | Descrição | Alerta |
|---------|-----------|--------|
| `produce_rate` | Mensagens/segundo produzidas | - |
| `fetch_rate` | Mensagens/segundo consumidas | - |
| `lag` | Diferença entre produção e consumo | > 10000 |
| `disk_usage` | Uso de disco | > 80% |
| `active_connections` | Conexões ativas | > 90% max |
| `request_latency_p99` | Latência P99 | > 100ms |

### Logs

Habilite métricas no log para análise:

```json
{
    "log": {
        "level": "info",
        "metrics_interval_seconds": 60
    }
}
```

Output:
```
[INFO] Metrics: produce_rate=45000/s fetch_rate=42000/s lag=3000 connections=150
```

## Troubleshooting

### Alta Latência

1. **Verificar disco**:
   ```bash
   iostat -x 1
   # await > 10ms indica problema
   ```

2. **Verificar CPU**:
   ```bash
   top -H -p $(pgrep event_horizon)
   ```

3. **Verificar rede**:
   ```bash
   ss -s  # Conexões
   netstat -i  # Erros de rede
   ```

### Baixo Throughput

1. **Aumentar batch size** no produtor
2. **Habilitar compressão**
3. **Verificar segment size** (muito pequeno = muitos arquivos)
4. **Verificar buffer size** (muito pequeno = muitas syscalls)

### Out of Memory

1. **Reduzir max_connections**
2. **Reduzir buffer_size**
3. **Aumentar swap** (não recomendado para produção)
4. **Adicionar mais RAM**

### Disco Cheio

1. **Reduzir retention_hours**
2. **Deletar tópicos antigos**
3. **Adicionar mais disco**
4. **Habilitar compressão**

## Best Practices

### Produção

1. **Use SSD/NVMe** para dados
2. **Separe logs de dados** em discos diferentes
3. **Configure retention** adequado
4. **Monitore métricas** continuamente
5. **Configure alertas** para métricas críticas

### Desenvolvimento

1. **Use segment_size pequeno** (64MB)
2. **Habilite log debug** quando necessário
3. **Use Docker** para ambiente isolado
4. **Limpe dados** entre testes

## Comparação com Kafka

| Aspecto | Event Horizon | Apache Kafka |
|---------|---------------|--------------|
| Footprint | ~10MB | ~500MB+ |
| Startup | <1s | 10-30s |
| Dependências | Nenhuma | JVM, ZK/KRaft |
| Replicação | Não (v0.1) | Sim |
| Transactions | Não (v0.1) | Sim |
| Throughput | ~500K msg/s | ~1M msg/s |

O Event Horizon é ideal para:
- Ambientes com recursos limitados
- Desenvolvimento e testes
- Edge computing
- Aplicações embarcadas
