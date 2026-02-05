# Event Horizon - Kafka-Compatible Event Streaming Platform

A Kafka protocol-compatible event storage service, inspired by MapR Event Horizon, implemented in C++23.

## Features

- ✅ Binary Kafka protocol compatible
- ✅ Persistent disk storage
- ✅ Topic partitioning
- ✅ Multi-threaded TCP server
- ✅ APIs: Produce, Fetch, Metadata, ApiVersions, Consumer Groups
- ✅ Big-endian serialization (Kafka standard)
- ✅ Cross-platform build (Windows, Linux, macOS)

## Requirements

- CMake 3.16+
- C++23 compiler (GCC 13+, Clang 17+, MSVC 2026+)
- vcpkg (package manager)
- Ninja (optional, recommended for Linux/macOS)

### Windows Build Tools (sem Visual Studio IDE)

No Windows, você pode usar apenas o **Build Tools** em vez do Visual Studio completo:

```powershell
# Instalar CMake
winget install Kitware.CMake

# Instalar Visual Studio Build Tools 2026 (apenas compilador, ~3-5GB)
winget install Microsoft.VisualStudio.BuildTools --override "--add Microsoft.VisualStudio.Workload.VCTools --add Microsoft.VisualStudio.Component.VC.CMake.Project --passive"
```

Isso instala:
- CMake (ferramenta de build)
- Compilador MSVC (cl.exe)
- MSBuild e ferramentas de build

> **Nota:** Não é necessário instalar o Visual Studio IDE completo (~15-20GB).

## Supported Platforms

| Platform | x86-64 | ARM64 |
|----------|--------|-------|
| Windows  | ✅     | ❌    |
| Linux    | ✅     | ✅    |
| macOS    | ✅     | ✅    |

## vcpkg Installation

### Windows

```powershell
git clone https://github.com/microsoft/vcpkg.git C:\vcpkg
cd C:\vcpkg
.\bootstrap-vcpkg.bat

# Set environment variable (PowerShell)
$env:VCPKG_ROOT = "C:\vcpkg"

# Or add permanently
[Environment]::SetEnvironmentVariable("VCPKG_ROOT", "C:\vcpkg", "User")
```

### Linux / macOS

```bash
git clone https://github.com/microsoft/vcpkg.git ~/vcpkg
cd ~/vcpkg
./bootstrap-vcpkg.sh

# Add to ~/.bashrc or ~/.zshrc
export VCPKG_ROOT="$HOME/vcpkg"
```

## Building

Dependencies (Boost, nlohmann-json) are automatically downloaded by vcpkg.

### Windows (PowerShell) - Recommended

```powershell
# Release build
.\build.ps1 -BuildType Release

# Debug build with tests
.\build.ps1 -BuildType Debug -Test

# Build with Visual Studio 2022
.\build.ps1 -Generator vs2022

# Build with Ninja
.\build.ps1 -Generator ninja

# Clean and rebuild
.\build.ps1 -Clean -BuildType Release
```

### Windows (Batch)

```batch
REM Release build
build.bat Release

REM Release build and run tests
build.bat Release --test
```

### Windows (CMake Presets)

```powershell
# Configure
cmake --preset windows-release

# Build
cmake --build build/windows-release --config Release

# Tests
ctest --preset windows
```

### Linux / macOS

```bash
# Give execute permission
chmod +x build.sh

# Release build
./build.sh Release

# Debug build with tests
./build.sh Debug --test
```

### Linux / macOS (CMake Presets)

```bash
# Configure
cmake --preset linux-release   # or macos-release / macos-arm64-release

# Build
cmake --build build/linux-release

# Tests
ctest --preset linux
```

### Manual CMake (Any Platform)

```bash
mkdir build && cd build

# With vcpkg toolchain
cmake .. -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake

# Build
cmake --build . --config Release

# Tests
ctest --output-on-failure
```

## Available CMake Presets

| Preset | Platform | Description |
|--------|----------|-------------|
| `linux-release` | Linux x64 | Release with Ninja |
| `linux-debug` | Linux x64 | Debug with Ninja |
| `linux-arm64-release` | Linux ARM64 | Release with Ninja |
| `linux-arm64-debug` | Linux ARM64 | Debug with Ninja |
| `macos-release` | macOS x64 | Release with Ninja |
| `macos-debug` | macOS x64 | Debug with Ninja |
| `macos-arm64-release` | macOS ARM64 | Release with Ninja |
| `macos-arm64-debug` | macOS ARM64 | Debug with Ninja |
| `windows-release` | Windows x64 | Release with Visual Studio |
| `windows-debug` | Windows x64 | Debug with Visual Studio |
| `windows-ninja` | Windows x64 | Release with Ninja |
| `windows-static` | Windows x64 | Release with static linking |

## Running

```bash
# Start the broker
./event_horizon -p 9092 -d ./data

# With configuration file
./event_horizon -c config.json
```

## Kafka UI (Docker)

To visualize and manage Event Horizon via web interface:

```bash
# Start Kafka UI
docker-compose up -d

# Access in browser
# http://localhost:8080

# Stop
docker-compose down
```

**Note:** Event Horizon must be running before starting Kafka UI.

## Docker

Event Horizon can be run as a Docker container, using the same base image as Confluent Kafka (Ubuntu 22.04).

### Building the Docker Image

```bash
# Build for local architecture (linux/amd64)
docker build -t eventhorizon:latest .

# Build with specific version tag
docker build -t eventhorizon:1.0.0 .

# Multi-architecture build (x64 and ARM64)
docker buildx create --use
docker buildx build --platform linux/amd64,linux/arm64 -t eventhorizon:latest --push .
```

### Publishing to DockerHub

```bash
# Login no DockerHub
docker login

# Build e tag com seu usuário DockerHub
docker build -t <seu-usuario>/eventhorizon:latest .
docker build -t <seu-usuario>/eventhorizon:1.0.0 .

# Push para DockerHub
docker push <seu-usuario>/eventhorizon:latest
docker push <seu-usuario>/eventhorizon:1.0.0

# Multi-architecture build direto para DockerHub (x64 + ARM64)
docker buildx create --name multiarch --use
docker buildx build \
    --platform linux/amd64,linux/arm64 \
    -t <seu-usuario>/eventhorizon:latest \
    -t <seu-usuario>/eventhorizon:1.0.0 \
    --push .
```

> **Nota:** Substitua `<seu-usuario>` pelo seu username no DockerHub.
```

### Running with Docker

```bash
# Basic run
docker run -d \
    --name eventhorizon \
    -p 9092:9092 \
    eventhorizon:latest

# With persistent data volume
docker run -d \
    --name eventhorizon \
    -p 9092:9092 \
    -v eventhorizon-data:/var/lib/eventhorizon \
    eventhorizon:latest

# With custom configuration
docker run -d \
    --name eventhorizon \
    -p 9092:9092 \
    -v eventhorizon-data:/var/lib/eventhorizon \
    -v $(pwd)/config.json:/etc/eventhorizon/config.json:ro \
    -e LOG_LEVEL=debug \
    eventhorizon:latest

# Run with all options
docker run -d \
    --name eventhorizon \
    -p 9092:9092 \
    -v eventhorizon-data:/var/lib/eventhorizon \
    -v eventhorizon-logs:/var/log/eventhorizon \
    -e BROKER_ID=0 \
    -e BROKER_HOST=0.0.0.0 \
    -e BROKER_PORT=9092 \
    -e LOG_LEVEL=info \
    --restart unless-stopped \
    eventhorizon:latest
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BROKER_ID` | `0` | Unique broker identifier |
| `BROKER_HOST` | `0.0.0.0` | Host address to bind |
| `BROKER_PORT` | `9092` | Port to listen on |
| `LOG_LEVEL` | `info` | Log level: trace, debug, info, warn, error |

### Docker Volumes

| Path | Description |
|------|-------------|
| `/var/lib/eventhorizon` | Data directory (topics, partitions, logs) |
| `/var/log/eventhorizon` | Application logs |
| `/etc/eventhorizon` | Configuration files |

### Docker Compose

Add Event Horizon to your `docker-compose.yml`:

```yaml
version: '3.8'

services:
  eventhorizon:
    build: .
    # Or use pre-built image:
    # image: eventhorizon:latest
    container_name: eventhorizon
    ports:
      - "9092:9092"
    environment:
      BROKER_ID: 0
      BROKER_HOST: 0.0.0.0
      BROKER_PORT: 9092
      LOG_LEVEL: info
    volumes:
      - eventhorizon-data:/var/lib/eventhorizon
      - eventhorizon-logs:/var/log/eventhorizon
      - ./config.json:/etc/eventhorizon/config.json:ro
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "9092"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 5s
    restart: unless-stopped

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: event-horizon
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: eventhorizon:9092
    depends_on:
      eventhorizon:
        condition: service_healthy
    restart: unless-stopped

volumes:
  eventhorizon-data:
  eventhorizon-logs:
```

Start the stack:

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f eventhorizon

# Stop all services
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

### Docker Commands Reference

```bash
# View container logs
docker logs -f eventhorizon

# Execute command inside container
docker exec -it eventhorizon /bin/bash

# Check container health
docker inspect --format='{{.State.Health.Status}}' eventhorizon

# Stop container
docker stop eventhorizon

# Remove container
docker rm eventhorizon

# Remove image
docker rmi eventhorizon:latest
```

### Production Recommendations

1. **Use named volumes** for data persistence:
   ```bash
   docker volume create eventhorizon-data
   ```

2. **Set resource limits**:
   ```yaml
   deploy:
     resources:
       limits:
         cpus: '2'
         memory: 4G
       reservations:
         cpus: '1'
         memory: 2G
   ```

3. **Use health checks** to ensure the broker is ready before dependent services start.

4. **Configure logging driver** for production:
   ```yaml
   logging:
     driver: "json-file"
     options:
       max-size: "100m"
       max-file: "5"
   ```

5. **Network security**: In production, consider using Docker networks to isolate services:
   ```yaml
   networks:
     eventhorizon-net:
       driver: bridge
   ```

## Configuration

Edit the `config.json` file:

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

## Logging

Event Horizon uses [spdlog](https://github.com/gabime/spdlog) for high-performance structured logging.

### Log Levels

| Level | Description |
|-------|-------------|
| `trace` | Detailed protocol-level information (very verbose) |
| `debug` | Debug information for development |
| `info` | General operational messages (default) |
| `warn` | Warning conditions |
| `error` | Error conditions |
| `critical` | Critical failures |

### Command Line Options

```bash
# Run with default log level (info)
./event_horizon -c config.json

# Run with debug logging
./event_horizon -c config.json --log-level debug

# Run with trace logging (very verbose)
./event_horizon -c config.json --log-level trace

# Minimal logging (warnings and errors only)
./event_horizon -c config.json --log-level warn
```

### Log Output

Logs are written to both:

- **Console**: Colored output for easy reading
- **File**: Rotating log files in `logs/` directory
  - Maximum file size: 10 MB
  - Maximum files: 5 (automatic rotation)
  - Location: `logs/eventhorizon.log`

### Log Format

```
[2026-01-31 14:30:45.123] [info] [thread 12345] Server started on port 9092
[2026-01-31 14:30:46.456] [debug] [thread 12346] Received Produce request for topic 'my-topic'
```

### Programmatic Usage

The logging system can be used in code via macros:

```cpp
#include "logging/logger.hpp"

LOG_INFO("Server started on port {}", port);
LOG_DEBUG("Processing request from client {}", client_id);
LOG_ERROR("Failed to write to partition: {}", error_message);
LOG_TRACE("Raw bytes received: {} bytes", data.size());
```

## Testing with Kafka Clients

Event Horizon is compatible with standard Kafka clients:

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
# Produce message
echo "Hello" | kcat -P -b localhost:9092 -t my-topic

# Consume messages
kcat -C -b localhost:9092 -t my-topic
```

## Project Structure

```
eventhorizon/
├── CMakeLists.txt          # CMake configuration
├── CMakePresets.json       # Presets for Windows/Linux/macOS
├── vcpkg.json              # vcpkg dependencies
├── config.json             # Example configuration
├── build.ps1               # Build script (Windows PowerShell)
├── build.bat               # Build script (Windows Batch)
├── build.sh                # Build script (Linux/macOS)
├── README.md               # This file
├── .gitignore              # Git ignored files
├── .vscode/                # VS Code configuration
│   ├── launch.json         # Debug configurations
│   ├── tasks.json          # Build tasks
│   ├── settings.json       # CMake settings
│   └── c_cpp_properties.json # IntelliSense configuration
├── .github/
│   └── workflows/
│       └── build.yml       # CI/CD GitHub Actions
└── src/
    ├── main.cpp            # Entry point
    ├── broker/
    │   ├── broker.hpp      # Broker class
    │   └── broker.cpp
    ├── storage/
    │   ├── log_segment.hpp # Log segment (disk)
    │   ├── log_segment.cpp
    │   ├── partition.hpp   # Topic partition
    │   └── partition.cpp
    ├── protocol/
    │   ├── kafka_protocol.hpp  # Kafka protocol
    │   └── kafka_protocol.cpp
    ├── network/
    │   ├── server.hpp      # TCP server
    │   └── server.cpp
    └── tests/
        ├── test_log_segment.cpp
        ├── test_partition.cpp
        └── test_protocol.cpp
```

## Supported Kafka APIs

| API | Status | Description |
|-----|--------|-------------|
| Produce (0) | ✅ | Publish messages |
| Fetch (1) | ✅ | Consume messages |
| ListOffsets (2) | ✅ | List offsets |
| Metadata (3) | ✅ | Cluster information |
| OffsetCommit (8) | ✅ | Commit offset |
| OffsetFetch (9) | ✅ | Fetch offset |
| FindCoordinator (10) | ✅ | Find coordinator |
| JoinGroup (11) | ✅ | Join consumer group |
| Heartbeat (12) | ✅ | Consumer heartbeat |
| LeaveGroup (13) | ✅ | Leave group |
| SyncGroup (14) | ✅ | Sync group |
| ApiVersions (18) | ✅ | Version negotiation |

## Architecture

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

## Tests

### Windows
```powershell
# Via script
.\build.ps1 -Test

# Via ctest
cd build/windows-release
ctest --build-config Release --output-on-failure
```

### Linux / macOS
```bash
# Via script
./build.sh Release --test

# Via ctest
cd build/linux-release
ctest --output-on-failure
```

## Generated Binaries

After building, binaries will be located at:

| Platform | Path |
|----------|------|
| Windows Release | `build/windows-release/Release/event_horizon.exe` |
| Windows Debug | `build/windows-debug/Debug/event_horizon.exe` |
| Linux Release | `build/linux-release/event_horizon` |
| Linux Debug | `build/linux-debug/event_horizon` |
| macOS Release | `build/macos-release/event_horizon` |
| macOS ARM64 Release | `build/macos-arm64-release/event_horizon` |

## TODO / Roadmap

- [ ] Broker replication
- [ ] Compression (gzip, snappy, lz4)
- [ ] Log compaction
- [ ] Transactions
- [ ] SASL/SSL authentication
- [ ] Quotas
- [ ] Admin API

## License

Apache 2.0 License

## References

- [Apache Kafka Protocol Guide](https://kafka.apache.org/protocol)
- [MapR Event Horizon Documentation](https://docs.datafabric.hpe.com/62/MapR_Streams/MapR_Streams.html)
