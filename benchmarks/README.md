# Benchmark de Performance: Event Horizon vs Apache Kafka

Este diretório contém ferramentas para medir e comparar performance de produção
entre Event Horizon e Apache Kafka.

## Opções de Benchmark

### 1. OpenMessaging Benchmark (Recomendado)

Usa a ferramenta oficial da Confluent ([OpenMessaging Benchmark](https://github.com/confluentinc/openmessaging-benchmark))
via Docker. Mesma metodologia usada nos benchmarks oficiais do Kafka.

```powershell
# Comparar Event Horizon vs Kafka
.\run-benchmark.ps1 -Target compare

# Apenas Kafka
.\run-benchmark.ps1 -Target kafka

# Apenas Event Horizon (deve estar rodando em localhost:9092)
.\run-benchmark.ps1 -Target eventhorizon

# Teste de latência
.\run-benchmark.ps1 -Target compare -Workload latency
```

### 2. Python Script (Testes Rápidos)

Script Python simples usando `confluent-kafka` e `testcontainers`.

```powershell
# Instalar dependências
pip install -r requirements.txt

# Comparar (Event Horizon deve estar rodando)
python produce_benchmark.py --compare --eh-host localhost --eh-port 9092

# Apenas Kafka (usa testcontainer)
python produce_benchmark.py --kafka-only

# Apenas Event Horizon
python produce_benchmark.py --eventhorizon-only --eh-host localhost --eh-port 9092

# Customizar
python produce_benchmark.py --compare --messages 1000000 --message-size 1024
```

## Workloads

| Arquivo | Descrição |
|---------|-----------|
| `throughput-1kb.yaml` | Throughput máximo com mensagens de 1KB |
| `latency-1kb.yaml` | Latência em carga fixa (200K msg/s) |

## Métricas Coletadas

- **Throughput**: mensagens/segundo, MB/segundo
- **Latência**: P50, P90, P95, P99, P99.9, Max

## Configuração Baseada no Benchmark Confluent

Referência: https://developer.confluent.io/learn/kafka-performance/

| Parâmetro | Throughput | Latency |
|-----------|------------|---------|
| batch.size | 1MB | 1MB |
| linger.ms | 10 | 1 |
| acks | all | all |
| partitions | 8 | 8 |
| message size | 1KB | 1KB |

## Resultados

Os resultados JSON são salvos em `./results/`

## Pré-requisitos

1. Docker Desktop rodando
2. Event Horizon compilado e rodando (para testes do EH)
3. Python 3.10+ (para script Python)
