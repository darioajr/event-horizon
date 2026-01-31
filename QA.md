# QA - Guia de Testes do Event Horizon

Este documento descreve como configurar o ambiente de testes e executar os testes do Event Horizon.

## Índice

1. [Pré-requisitos](#pré-requisitos)
2. [Configuração do Ambiente](#configuração-do-ambiente)
3. [Testes Unitários](#testes-unitários)
4. [Testes de Integração com Kafka UI](#testes-de-integração-com-kafka-ui)
5. [Cenários de Teste](#cenários-de-teste)
6. [Troubleshooting](#troubleshooting)

---

## Pré-requisitos

### Windows

- **Visual Studio 2022** (ou Build Tools) com suporte a C++23
- **CMake** 3.20+
- **Git**
- **Docker Desktop** (para testes com Kafka UI)
- **PowerShell** 5.1+

### Linux

- **GCC 14+** ou **Clang 17+**
- **CMake** 3.20+
- **Ninja** (recomendado)
- **Docker** e **Docker Compose**

---

## Configuração do Ambiente

### 1. Clonar o Repositório

```bash
git clone https://github.com/seu-usuario/eventhorizon.git
cd eventhorizon
```

### 2. Configurar e Compilar

#### Windows (PowerShell)

```powershell
# Configurar (Debug)
cmake --preset windows-debug

# Compilar
cmake --build build/windows-debug --config Debug

# Ou para Release
cmake --preset windows-release
cmake --build build/windows-release --config Release
```

#### Linux

```bash
# Configurar
cmake --preset linux-release

# Compilar
cmake --build build/linux-release --config Release
```

---

## Testes Unitários

### Executar Todos os Testes

#### Windows

```powershell
# Via CTest
cd build/windows-debug
ctest --build-config Debug --output-on-failure --verbose

# Ou executar diretamente
.\Debug\eventhorizon_tests.exe
```

#### Linux

```bash
cd build/linux-release
ctest --build-config Release --output-on-failure --verbose

# Ou executar diretamente
./eventhorizon_tests
```

### Executar Testes Específicos

```powershell
# Filtrar por nome do teste
.\Debug\eventhorizon_tests.exe --gtest_filter="PartitionTest.*"
.\Debug\eventhorizon_tests.exe --gtest_filter="BrokerTest.*"
.\Debug\eventhorizon_tests.exe --gtest_filter="ConsumerGroupTest.*"
.\Debug\eventhorizon_tests.exe --gtest_filter="ProtocolHandlerTest.*"
```

### Suites de Teste Disponíveis

| Suite | Descrição | Quantidade |
|-------|-----------|------------|
| `PartitionTest` | Testes de partições e segmentos | 13 |
| `BrokerTest` | Testes do broker e gerenciamento de tópicos | 20 |
| `ConsumerGroupTest` | Testes de consumer groups | 24 |
| `ProtocolHandlerTest` | Testes do protocolo Kafka | 18 |
| `LogSegmentTest` | Testes de segmentos de log | ~15 |
| `BufferTest` | Testes de buffer/serialização | ~10 |
| **Total** | | **127+** |

---

## Testes de Integração com Kafka UI

### 1. Iniciar o Event Horizon

```powershell
# Windows
.\build\windows-debug\Debug\event_horizon.exe

# Linux
./build/linux-release/event_horizon
```

O servidor inicia na porta **9092** (padrão Kafka).

### 2. Iniciar o Kafka UI

```powershell
docker-compose up -d
```

Acesse: **http://localhost:8080**

### 3. Configurar Conexão no Kafka UI

No Kafka UI, o cluster já está pré-configurado para conectar em `host.docker.internal:9092`.

Se precisar configurar manualmente:
- **Cluster Name**: Event Horizon
- **Bootstrap Servers**: `host.docker.internal:9092` (Windows/Mac) ou `172.17.0.1:9092` (Linux)

---

## Cenários de Teste

### CT-001: Criar Tópico

**Passos:**
1. Abrir Kafka UI (http://localhost:8080)
2. Ir em "Topics" → "Add a Topic"
3. Preencher:
   - Name: `teste-topico`
   - Number of partitions: `3`
   - Replication factor: `1`
4. Clicar em "Create topic"

**Resultado Esperado:**
- Tópico criado com sucesso
- Aparece na lista de tópicos

---

### CT-002: Produzir Mensagens

**Passos:**
1. Selecionar o tópico criado
2. Ir em "Messages" → "Produce Message"
3. Preencher:
   - Key: `key-1`
   - Value: `{"nome": "teste", "valor": 123}`
4. Clicar em "Produce Message"

**Resultado Esperado:**
- Mensagem produzida com sucesso
- Offset incrementado

---

### CT-003: Consumir Mensagens

**Passos:**
1. Selecionar o tópico
2. Ir em "Messages"
3. Configurar filtros se necessário
4. Clicar em "Fetch"

**Resultado Esperado:**
- Mensagens exibidas corretamente
- Key e Value visíveis

---

### CT-004: Limpar Mensagens do Tópico (Clear Messages)

**Passos:**
1. Selecionar o tópico
2. Ir em "..." (menu) → "Clear messages"
3. Confirmar a operação

**Resultado Esperado:**
- Todas as mensagens removidas
- Offset resetado ou mantido (dependendo da implementação)

---

### CT-005: Excluir Tópico

**Passos:**
1. Selecionar o tópico
2. Ir em "..." (menu) → "Remove topic"
3. Confirmar a exclusão

**Resultado Esperado:**
- Tópico removido da lista
- Dados físicos removidos

---

### CT-006: Consumer Groups

**Passos:**
1. Criar um consumer group via cliente Kafka ou ferramenta
2. No Kafka UI, ir em "Consumer Groups"
3. Verificar o grupo criado

**Resultado Esperado:**
- Grupo visível na lista
- Membros e partições atribuídas corretamente

---

### CT-007: Recriar Tópico

**Passos:**
1. Excluir um tópico existente (CT-005)
2. Criar o tópico novamente com o mesmo nome (CT-001)
3. Produzir novas mensagens (CT-002)

**Resultado Esperado:**
- Tópico recriado sem erros
- Novas mensagens persistidas corretamente

---

### CT-008: Teste de Persistência

**Passos:**
1. Produzir mensagens em um tópico
2. Parar o Event Horizon (Ctrl+C)
3. Reiniciar o Event Horizon
4. Consumir mensagens do tópico

**Resultado Esperado:**
- Mensagens persistidas corretamente
- Dados recuperados após reinício

---

### CT-009: Múltiplas Partições

**Passos:**
1. Criar tópico com 5 partições
2. Produzir 100 mensagens com keys diferentes
3. Verificar distribuição entre partições

**Resultado Esperado:**
- Mensagens distribuídas entre partições baseado na key
- Cada partição com seu próprio offset

---

### CT-010: Teste de Carga

**Passos:**
1. Criar tópico de teste
2. Usar ferramenta de benchmark (kafka-producer-perf-test ou similar)
3. Produzir 10.000 mensagens
4. Medir throughput e latência

**Resultado Esperado:**
- Todas as mensagens persistidas
- Throughput dentro do esperado

---

## Teste de Compatibilidade Binária com Kafka

O projeto inclui um script para testar compatibilidade binária:

```powershell
.\test-kafka-compatibility.ps1
```

Este script:
1. Descobre tópicos e partições na pasta `data/`
2. Inicia um Kafka real via Docker
3. Importa os dados do Event Horizon para o Kafka
4. Valida a compatibilidade do formato binário

---

## Troubleshooting

### Erro: "Connection refused" no Kafka UI

1. Verificar se o Event Horizon está rodando na porta 9092
2. Verificar firewall do Windows
3. Testar conexão: `Test-NetConnection localhost -Port 9092`

### Erro: Testes falham com "file not found"

1. Verificar se a pasta `data/` existe
2. Limpar e recompilar:
   ```powershell
   Remove-Item -Recurse build/windows-debug
   cmake --preset windows-debug
   cmake --build build/windows-debug --config Debug
   ```

### Erro: Docker não conecta ao host

No Linux, usar o IP da bridge do Docker:
```bash
# Descobrir IP
docker network inspect bridge | grep Gateway
# Usar esse IP no Kafka UI
```

### Erro: Mensagens não aparecem no Kafka UI

1. Verificar se a mensagem foi produzida (offset incrementou)
2. Verificar filtros de busca no UI
3. Testar com offset 0: "Oldest"

---

## Relatório de Testes

Após executar os testes, preencher:

| Cenário | Status | Observações |
|---------|--------|-------------|
| CT-001 | ⬜ | |
| CT-002 | ⬜ | |
| CT-003 | ⬜ | |
| CT-004 | ⬜ | |
| CT-005 | ⬜ | |
| CT-006 | ⬜ | |
| CT-007 | ⬜ | |
| CT-008 | ⬜ | |
| CT-009 | ⬜ | |
| CT-010 | ⬜ | |

**Legenda:** ✅ Passou | ❌ Falhou | ⬜ Não executado

---

## Contato

Em caso de dúvidas ou problemas, abrir uma issue no repositório.
