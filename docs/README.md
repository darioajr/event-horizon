# Documentação do Event Horizon

Bem-vindo à documentação técnica do Event Horizon.

## Índice

1. [Arquitetura](architecture.md) - Visão geral da arquitetura do sistema
2. [Protocolo Kafka](kafka-protocol.md) - APIs Kafka suportadas
3. [Configuração](configuration.md) - Opções de configuração
4. [API Reference](api-reference.md) - Referência das APIs internas
5. [Performance](performance.md) - Guia de performance e tuning

## Início Rápido

```bash
# Compilar
cmake --preset windows-release
cmake --build build/windows-release --config Release

# Executar
./build/windows-release/Release/event_horizon.exe
```

O servidor inicia na porta 9092 (padrão Kafka).

## Links Úteis

- [README](../README.md) - Introdução ao projeto
- [CONTRIBUTING](../CONTRIBUTING.md) - Como contribuir
- [CHANGELOG](../CHANGELOG.md) - Histórico de versões
- [QA](../QA.md) - Guia de testes
