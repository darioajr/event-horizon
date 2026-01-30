# Changelog

Todas as mudanças notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/),
e este projeto adere ao [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [Unreleased]

### Added
- Suporte completo ao protocolo Kafka 4.1.x
- Consumer Groups com rebalanceamento automático
- API DeleteRecords para limpeza de mensagens
- Persistência de dados em disco
- Compatibilidade com Kafka UI
- CI/CD com GitHub Actions para Linux e Windows
- Documentação QA e Contributing

### Changed
- Migração para C++23

### Fixed
- Correção de encoding UTF-8 no MSVC

---

## [0.1.0] - 2026-01-30

### Added
- Implementação inicial do broker
- Suporte a tópicos e partições
- Protocolo Kafka básico (Produce, Fetch, Metadata)
- Armazenamento em log segments
- Servidor TCP assíncrono com Boost.Asio
- Testes unitários com Google Test (127+ testes)

[Unreleased]: https://github.com/seu-usuario/eventhorizon/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/seu-usuario/eventhorizon/releases/tag/v0.1.0
