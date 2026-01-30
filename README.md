# Event Horizon - Kafka-Compatible Event Streaming Platform

Um serviço de armazenamento de eventos compatível com o protocolo Apache Kafka, inspirado no MapR Event Horizon, implementado em C++20.

## Características

- ✅ Protocolo Kafka binário compatível
- ✅ Armazenamento persistente em disco
- ✅ Particionamento de tópicos
- ✅ Servidor TCP multi-threaded
- ✅ APIs: Produce, Fetch, Metadata, ApiVersions, Consumer Groups
- ✅ Serialização big-endian (padrão Kafka)
- ✅ Build cross-platform (Windows e Linux)

## Requisitos

- CMake 3.16+
- C++20 compiler (GCC 10+, Clang 12+, MSVC 2019+)
- vcpkg (gerenciador de pacotes)
- Ninja (opcional, recomendado para Linux)

## Instalação do vcpkg

### Windows

```powershell
git clone https://github.com/microsoft/vcpkg.git C:\vcpkg
cd C:\vcpkg
.\bootstrap-vcpkg.bat

# Configurar variável de ambiente (PowerShell)
$env:VCPKG_ROOT = "C:\vcpkg"

# Ou adicionar permanentemente
[Environment]::SetEnvironmentVariable("VCPKG_ROOT", "C:\vcpkg", "User")
```

### Linux

```bash
git clone https://github.com/microsoft/vcpkg.git ~/vcpkg
cd ~/vcpkg
./bootstrap-vcpkg.sh

# Adicionar ao ~/.bashrc ou ~/.zshrc
export VCPKG_ROOT="$HOME/vcpkg"
```

## Compilação

As dependências (Boost, nlohmann-json) serão baixadas automaticamente pelo vcpkg.

### Windows (PowerShell) - Recomendado

```powershell
# Build Release
.\build.ps1 -BuildType Release

# Build Debug com testes
.\build.ps1 -BuildType Debug -Test

# Build com Visual Studio 2022
.\build.ps1 -Generator vs2022

# Build com Ninja
.\build.ps1 -Generator ninja

# Limpar e recompilar
.\build.ps1 -Clean -BuildType Release
```

### Windows (Batch)

```batch
REM Build Release
build.bat Release

REM Build Release e rodar testes
build.bat Release --test
```

### Windows (CMake Presets)

```powershell
# Configurar
cmake --preset windows-release

# Compilar
cmake --build build/windows-release --config Release

# Testes
ctest --preset windows
```

### Linux

```bash
# Dar permissão ao script
chmod +x build.sh

# Build Release
./build.sh Release

# Build Debug com testes
./build.sh Debug --test
```

### Linux (CMake Presets)

```bash
# Configurar
cmake --preset linux-release

# Compilar
cmake --build build/linux-release

# Testes
ctest --preset linux
```

### CMake Manual (Qualquer Plataforma)

```bash
mkdir build && cd build

# Com vcpkg toolchain
cmake .. -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake

# Compilar
cmake --build . --config Release

# Testes
ctest --output-on-failure
```

## CMake Presets Disponíveis

| Preset | Plataforma | Descrição |
|--------|------------|-----------|
| `linux-release` | Linux | Release com Ninja |
| `linux-debug` | Linux | Debug com Ninja |
| `windows-release` | Windows | Release com Visual Studio 2022 |
| `windows-debug` | Windows | Debug com Visual Studio 2022 |
| `windows-ninja` | Windows | Release com Ninja |
| `windows-static` | Windows | Release com linking estático |

## Execução

```bash
# Iniciar o broker
./event_horizon -p 9092 -d ./data

# Com arquivo de configuração
./event_horizon -c config.json
```

## Kafka UI (Docker)

Para visualizar e gerenciar o Event Horizon via interface web:

```bash
# Subir o Kafka UI
docker-compose up -d

# Acessar no navegador
# http://localhost:8080

# Parar
docker-compose down
```

**Nota:** O Event Horizon deve estar rodando antes de iniciar o Kafka UI.

## Configuração

Edite o arquivo `config.json`:

```json
{
    "broker_id": 0,
    "host": "localhost",
    "port": 9092,
    "log_dir": "./data",
    "num_partitions": 3,
    "replication_factor": 1,
    "thread_pool_size": 4,
    "cluster_id": "event-horizon-cluster"
}
```

## Testando com Cliente Kafka

O Event Horizon é compatível com clientes Kafka padrão:

### Python (kafka-python)

```python
from kafka import KafkaProducer, KafkaConsumer

# Producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('my-topic', b'Hello Event Horizon!')
producer.flush()

# Consumer
consumer = KafkaConsumer('my-topic', bootstrap_servers='localhost:9092')
for message in consumer:
    print(message.value)
```

### Java (kafka-clients)

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.send(new ProducerRecord<>("my-topic", "key", "value"));
```

### CLI (kafkacat/kcat)

```bash
# Produzir mensagem
echo "Hello" | kcat -P -b localhost:9092 -t my-topic

# Consumir mensagens
kcat -C -b localhost:9092 -t my-topic
```

## Estrutura do Projeto

```
mapC/
├── CMakeList.txt           # Configuração do CMake
├── CMakePresets.json       # Presets para Windows/Linux
├── vcpkg.json              # Dependências vcpkg
├── config.json             # Configuração exemplo
├── build.ps1               # Script de build (Windows PowerShell)
├── build.bat               # Script de build (Windows Batch)
├── build.sh                # Script de build (Linux)
├── README.md               # Este arquivo
├── .gitignore              # Arquivos ignorados pelo Git
├── .github/
│   └── workflows/
│       └── build.yml       # CI/CD GitHub Actions
└── src/
    ├── main.cpp            # Ponto de entrada
    ├── broker/
    │   ├── broker.hpp      # Classe Broker
    │   └── broker.cpp
    ├── storage/
    │   ├── log_segment.hpp # Segmento de log (disco)
    │   ├── log_segment.cpp
    │   ├── partition.hpp   # Partição de tópico
    │   └── partition.cpp
    ├── protocol/
    │   ├── kafka_protocol.hpp  # Protocolo Kafka
    │   └── kafka_protocol.cpp
    ├── network/
    │   ├── server.hpp      # Servidor TCP
    │   └── server.cpp
    └── tests/
        ├── test_log_segment.cpp
        ├── test_partition.cpp
        └── test_protocol.cpp
```

## APIs Kafka Suportadas

| API | Status | Descrição |
|-----|--------|-----------|
| Produce (0) | ✅ | Publicar mensagens |
| Fetch (1) | ✅ | Consumir mensagens |
| ListOffsets (2) | ✅ | Listar offsets |
| Metadata (3) | ✅ | Informações do cluster |
| OffsetCommit (8) | ✅ | Commit de offset |
| OffsetFetch (9) | ✅ | Buscar offset |
| FindCoordinator (10) | ✅ | Encontrar coordinator |
| JoinGroup (11) | ✅ | Entrar em grupo |
| Heartbeat (12) | ✅ | Heartbeat do consumidor |
| LeaveGroup (13) | ✅ | Sair do grupo |
| SyncGroup (14) | ✅ | Sincronizar grupo |
| ApiVersions (18) | ✅ | Negociação de versões |

## Arquitetura

```
┌─────────────────────────────────────────────────────────────┐
│                         Broker                               │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────────────────────────────┐   │
│  │   Network   │  │         Protocol Handler            │   │
│  │   Server    │◄─┤  (Kafka Binary Protocol Parser)     │   │
│  │  (Boost.    │  └─────────────────────────────────────┘   │
│  │   Asio)     │                                            │
│  └─────────────┘                                            │
├─────────────────────────────────────────────────────────────┤
│                      Topic Manager                           │
│  ┌─────────────────────────────────────────────────────────┐│
│  │ Topic A    [Partition 0] [Partition 1] [Partition 2]    ││
│  │ Topic B    [Partition 0] [Partition 1]                  ││
│  └─────────────────────────────────────────────────────────┘│
├─────────────────────────────────────────────────────────────┤
│                      Storage Engine                          │
│  ┌─────────────────────────────────────────────────────────┐│
│  │ LogSegment: [.log file] + [.index file]                 ││
│  │ - Append-only writes                                    ││
│  │ - Offset-indexed reads                                  ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

## Testes

### Windows
```powershell
# Via script
.\build.ps1 -Test

# Via ctest
cd build/windows-release
ctest --build-config Release --output-on-failure
```

### Linux
```bash
# Via script
./build.sh Release --test

# Via ctest
cd build/linux-release
ctest --output-on-failure
```

## Binários Gerados

Após a compilação, os binários estarão em:

| Plataforma | Caminho |
|------------|---------|
| Windows Release | `build/windows-release/Release/event_horizon.exe` |
| Windows Debug | `build/windows-debug/Debug/event_horizon.exe` |
| Linux Release | `build/linux-release/event_horizon` |
| Linux Debug | `build/linux-debug/event_horizon` |

## TODO / Roadmap

- [ ] Replicação entre brokers
- [ ] Compressão (gzip, snappy, lz4)
- [ ] Log compaction
- [ ] Transactions
- [ ] SASL/SSL authentication
- [ ] Quotas
- [ ] Admin API

## Licença

MIT License

## Referências

- [Apache Kafka Protocol Guide](https://kafka.apache.org/protocol)
- [MapR Event Horizon Documentation](https://docs.datafabric.hpe.com/62/MapR_Streams/MapR_Streams.html)
