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
- C++23 compiler (GCC 13+, Clang 17+, MSVC 2022+)
- vcpkg (package manager)
- Ninja (optional, recommended for Linux/macOS)

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

MIT License

## References

- [Apache Kafka Protocol Guide](https://kafka.apache.org/protocol)
- [MapR Event Horizon Documentation](https://docs.datafabric.hpe.com/62/MapR_Streams/MapR_Streams.html)
