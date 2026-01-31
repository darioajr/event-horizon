# Configuração

Este documento descreve as opções de configuração do Event Horizon.

## Arquivo de Configuração

O Event Horizon usa um arquivo `config.json` na raiz do projeto:

```json
{
    "broker": {
        "id": 1,
        "host": "localhost",
        "port": 9092
    },
    "storage": {
        "data_dir": "./data",
        "segment_size_bytes": 1073741824,
        "retention_hours": 168
    },
    "network": {
        "max_connections": 1000,
        "request_timeout_ms": 30000
    },
    "log": {
        "level": "info",
        "file": "./logs/eventhorizon.log"
    }
}
```

## Opções

### Broker

| Opção | Tipo | Padrão | Descrição |
|-------|------|--------|-----------|
| `broker.id` | int | 1 | ID único do broker |
| `broker.host` | string | "localhost" | Host para bind |
| `broker.port` | int | 9092 | Porta TCP |

### Storage

| Opção | Tipo | Padrão | Descrição |
|-------|------|--------|-----------|
| `storage.data_dir` | string | "./data" | Diretório de dados |
| `storage.segment_size_bytes` | int | 1GB | Tamanho máximo do segmento |
| `storage.retention_hours` | int | 168 (7 dias) | Retenção de dados |
| `storage.index_interval_bytes` | int | 4096 | Intervalo do índice |

### Network

| Opção | Tipo | Padrão | Descrição |
|-------|------|--------|-----------|
| `network.max_connections` | int | 1000 | Conexões simultâneas |
| `network.request_timeout_ms` | int | 30000 | Timeout de requisição |
| `network.buffer_size` | int | 65536 | Tamanho do buffer |

### Consumer Groups

| Opção | Tipo | Padrão | Descrição |
|-------|------|--------|-----------|
| `group.session_timeout_ms` | int | 30000 | Timeout de sessão |
| `group.heartbeat_interval_ms` | int | 3000 | Intervalo de heartbeat |
| `group.max_poll_interval_ms` | int | 300000 | Intervalo máximo de poll |

### Logging

| Opção | Tipo | Padrão | Descrição |
|-------|------|--------|-----------|
| `log.level` | string | "info" | Nível: debug, info, warn, error |
| `log.file` | string | "" | Arquivo de log (vazio = stdout) |
| `log.max_size_mb` | int | 100 | Tamanho máximo do log |
| `log.max_files` | int | 5 | Número de arquivos de backup |

## Variáveis de Ambiente

Configurações podem ser sobrescritas via variáveis de ambiente:

| Variável | Configuração |
|----------|--------------|
| `EVENT_HORIZON_PORT` | `broker.port` |
| `EVENT_HORIZON_DATA_DIR` | `storage.data_dir` |
| `EVENT_HORIZON_LOG_LEVEL` | `log.level` |

Exemplo:
```bash
EVENT_HORIZON_PORT=9093 ./event_horizon
```

## Linha de Comando

```bash
./event_horizon [opções]

Opções:
  -c, --config <arquivo>    Arquivo de configuração (padrão: config.json)
  -p, --port <porta>        Porta TCP (sobrescreve config)
  -d, --data-dir <dir>      Diretório de dados
  -v, --verbose             Modo verbose (debug)
  -h, --help                Mostra ajuda
  --version                 Mostra versão
```

## Configuração de Produção

Exemplo para ambiente de produção:

```json
{
    "broker": {
        "id": 1,
        "host": "0.0.0.0",
        "port": 9092
    },
    "storage": {
        "data_dir": "/var/lib/eventhorizon",
        "segment_size_bytes": 1073741824,
        "retention_hours": 720
    },
    "network": {
        "max_connections": 10000,
        "request_timeout_ms": 30000
    },
    "log": {
        "level": "warn",
        "file": "/var/log/eventhorizon/server.log",
        "max_size_mb": 500,
        "max_files": 10
    }
}
```

### Recomendações

1. **Data Directory**: Use SSD para melhor performance
2. **Segment Size**: 1GB é um bom padrão; aumente para alto throughput
3. **Max Connections**: Ajuste conforme número de clientes esperados
4. **Log Level**: Use "warn" ou "error" em produção
5. **Retention**: Configure conforme requisitos de negócio
