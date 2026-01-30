# Contribuindo para o Event Horizon

Obrigado pelo seu interesse em contribuir com o Event Horizon! Este documento fornece diretrizes e informações para contribuidores.

## Índice

1. [Código de Conduta](#código-de-conduta)
2. [Como Contribuir](#como-contribuir)
3. [Configuração do Ambiente](#configuração-do-ambiente)
4. [Padrões de Código](#padrões-de-código)
5. [Commits e Pull Requests](#commits-e-pull-requests)
6. [Testes](#testes)
7. [Documentação](#documentação)

---

## Código de Conduta

- Seja respeitoso e inclusivo
- Aceite críticas construtivas
- Foque no que é melhor para a comunidade
- Mostre empatia com outros membros

---

## Como Contribuir

### Reportando Bugs

1. Verifique se o bug já não foi reportado nas [Issues](../../issues)
2. Crie uma nova issue com:
   - Título claro e descritivo
   - Passos para reproduzir
   - Comportamento esperado vs. atual
   - Versão do Event Horizon
   - Sistema operacional e versão

### Sugerindo Melhorias

1. Abra uma issue com a tag `enhancement`
2. Descreva a melhoria proposta
3. Explique por que seria útil
4. Forneça exemplos se possível

### Contribuindo com Código

1. Fork o repositório
2. Crie uma branch para sua feature (`git checkout -b feature/minha-feature`)
3. Faça suas alterações
4. Adicione testes para novas funcionalidades
5. Certifique-se de que todos os testes passam
6. Commit suas mudanças (veja [Commits](#commits-e-pull-requests))
7. Push para sua branch (`git push origin feature/minha-feature`)
8. Abra um Pull Request

---

## Configuração do Ambiente

### Pré-requisitos

#### Windows
- Visual Studio 2022 ou Build Tools com suporte a C++23
- CMake 3.20+
- Git
- Docker Desktop (opcional, para testes de integração)

#### Linux
- GCC 14+ ou Clang 17+
- CMake 3.20+
- Ninja
- Docker (opcional)

### Build

```bash
# Clonar
git clone https://github.com/seu-usuario/eventhorizon.git
cd eventhorizon

# Windows
cmake --preset windows-debug
cmake --build build/windows-debug --config Debug

# Linux
cmake --preset linux-release
cmake --build build/linux-release
```

### Executar Testes

```bash
# Windows
cd build/windows-debug
ctest --build-config Debug --output-on-failure

# Linux
cd build/linux-release
ctest --output-on-failure
```

---

## Padrões de Código

### C++23

O projeto usa C++23. Utilize features modernas quando apropriado:

```cpp
// ✅ Bom - usar std::ranges
auto it = std::ranges::find(container, value);

// ✅ Bom - usar std::string::starts_with/ends_with
if (name.starts_with("test-")) { ... }

// ✅ Bom - usar CTAD (Class Template Argument Deduction)
std::lock_guard lock(mutex_);

// ✅ Bom - usar std::optional para valores opcionais
std::optional<int64_t> get_offset();
```

### Estilo de Código

- **Indentação**: 4 espaços (não tabs)
- **Largura máxima**: 100 caracteres por linha
- **Chaves**: Estilo Allman para funções, K&R para blocos

```cpp
// Funções - Allman
void my_function()
{
    // código
}

// Blocos - K&R
if (condition) {
    // código
} else {
    // código
}
```

### Nomenclatura

| Tipo | Convenção | Exemplo |
|------|-----------|---------|
| Classes | PascalCase | `ConsumerGroup` |
| Métodos | snake_case | `get_partition()` |
| Variáveis | snake_case | `partition_count` |
| Constantes | UPPER_SNAKE | `MAX_BATCH_SIZE` |
| Membros privados | snake_case_ | `partitions_` |
| Namespaces | snake_case | `event_horizon` |

### Headers

```cpp
#pragma once

#include <system_headers>

#include "project_headers.hpp"

namespace event_horizon {

class MyClass {
public:
    // Métodos públicos

private:
    // Membros privados
};

} // namespace event_horizon
```

### Tratamento de Erros

- Use exceções para erros excepcionais
- Use `std::optional` para valores que podem não existir
- Use códigos de erro do Kafka para respostas do protocolo

```cpp
// ✅ Bom
std::optional<Message> read_message(int64_t offset);

// ✅ Bom - códigos de erro Kafka
if (!topic_exists) {
    return UNKNOWN_TOPIC_OR_PARTITION; // 3
}
```

---

## Commits e Pull Requests

### Mensagens de Commit

Usamos [Conventional Commits](https://www.conventionalcommits.org/):

```
<tipo>(<escopo>): <descrição>

[corpo opcional]

[rodapé opcional]
```

#### Tipos

| Tipo | Descrição |
|------|-----------|
| `feat` | Nova funcionalidade |
| `fix` | Correção de bug |
| `docs` | Apenas documentação |
| `style` | Formatação, sem mudança de código |
| `refactor` | Refatoração de código |
| `perf` | Melhoria de performance |
| `test` | Adição ou correção de testes |
| `chore` | Tarefas de manutenção |
| `ci` | Mudanças no CI/CD |

#### Exemplos

```
feat(broker): add DeleteRecords API support

Implement Kafka DeleteRecords API (key 21) for clearing topic messages.
- Add handle_delete_records() in protocol handler
- Add delete_records_before() in Partition
- Add truncate() for full partition cleanup

Closes #42
```

```
fix(consumer): resolve rebalance timeout issue

The consumer group was timing out during rebalance when
members exceeded 30 seconds to rejoin.

Fixes #123
```

### Pull Requests

1. **Título**: Use o formato de commit convencional
2. **Descrição**: Explique o que foi feito e por quê
3. **Checklist**:
   - [ ] Testes adicionados/atualizados
   - [ ] Documentação atualizada
   - [ ] Código segue os padrões do projeto
   - [ ] Todos os testes passam
   - [ ] Sem warnings de compilação

---

## Testes

### Estrutura de Testes

Os testes ficam em `src/tests/` e usam Google Test:

```cpp
#include <gtest/gtest.h>
#include "broker/broker.hpp"

class BrokerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup
    }

    void TearDown() override {
        // Cleanup
    }
};

TEST_F(BrokerTest, CreateTopic) {
    // Arrange
    Broker broker(config_);

    // Act
    bool result = broker.create_topic("test-topic", 3);

    // Assert
    EXPECT_TRUE(result);
    EXPECT_TRUE(broker.topic_exists("test-topic"));
}
```

### Executar Testes Específicos

```bash
# Por suite
./eventhorizon_tests --gtest_filter="BrokerTest.*"

# Por nome
./eventhorizon_tests --gtest_filter="*DeleteRecords*"

# Verbose
./eventhorizon_tests --gtest_filter="PartitionTest.*" --gtest_print_time=1
```

### Cobertura Mínima

- Novas funcionalidades devem ter testes
- Correções de bugs devem incluir teste que reproduz o bug
- Objetivo: manter cobertura acima de 80%

---

## Documentação

### Código

Use comentários Doxygen para APIs públicas:

```cpp
/**
 * @brief Deletes records before the specified offset.
 * 
 * @param offset The offset before which records will be deleted.
 *               Use -1 to delete up to the high watermark.
 * @return The new low watermark after deletion.
 * 
 * @throws std::runtime_error if the partition is not initialized.
 */
int64_t delete_records_before(int64_t offset);
```

### README e Docs

- Mantenha o README.md atualizado
- Documente novas features
- Adicione exemplos de uso

---

## Arquitetura

### Estrutura do Projeto

```
eventhorizon/
├── src/
│   ├── main.cpp              # Entry point
│   ├── broker/               # Broker e gerenciamento de tópicos
│   ├── consumer/             # Consumer groups
│   ├── network/              # Servidor TCP
│   ├── protocol/             # Protocolo Kafka
│   ├── storage/              # Persistência (partições, segmentos)
│   └── tests/                # Testes unitários
├── build/                    # Build artifacts
├── data/                     # Dados persistidos
└── .github/workflows/        # CI/CD
```

### Componentes Principais

| Componente | Responsabilidade |
|------------|------------------|
| `Broker` | Gerenciamento de tópicos e partições |
| `Partition` | Armazenamento de mensagens |
| `LogSegment` | Segmentos de log no disco |
| `ConsumerGroup` | Gerenciamento de consumers |
| `KafkaProtocol` | Parser/builder do protocolo Kafka |
| `Server` | Servidor TCP assíncrono |

---

## Dúvidas?

- Abra uma [Discussion](../../discussions) para perguntas gerais
- Abra uma [Issue](../../issues) para bugs ou features
- Entre em contato com os maintainers

**Obrigado por contribuir!** 🚀
