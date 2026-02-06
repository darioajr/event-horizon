cmake_minimum_required(VERSION 3.16)

# Silenciar warning do FindBoost (removido no CMake 3.30+)
if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()

# ============================================================================
# Versionamento
# ============================================================================

# Ler versão base do arquivo version.txt
file(READ "${CMAKE_CURRENT_SOURCE_DIR}/version.txt" VERSION_CONTENT)
string(STRIP "${VERSION_CONTENT}" VERSION_BASE)

# Variáveis de versão (podem ser sobrescritas via -D)
set(VERSION_SUFFIX "" CACHE STRING "Version suffix (e.g., -dev, -feature)")
set(BUILD_NUMBER "0" CACHE STRING "Build number from CI")

# Compor versão completa
if(VERSION_SUFFIX)
    set(VERSION_FULL "${VERSION_BASE}${VERSION_SUFFIX}+${BUILD_NUMBER}")
else()
    set(VERSION_FULL "${VERSION_BASE}+${BUILD_NUMBER}")
endif()

project(event_horizon VERSION ${VERSION_BASE} LANGUAGES CXX)

# ============================================================================
# Verificar arquitetura suportada (x64 ou ARM64)
# ============================================================================
if(NOT CMAKE_SIZEOF_VOID_P EQUAL 8)
    message(FATAL_ERROR "Event Horizon only supports 64-bit architectures (x86-64 or ARM64).")
endif()

# Verificar plataforma e arquitetura
if(WIN32)
    if(NOT CMAKE_SYSTEM_PROCESSOR MATCHES "AMD64|x86_64")
        message(FATAL_ERROR "Windows builds only support x86-64 architecture.")
    endif()
endif()

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Opções de compilação
option(BUILD_TESTS "Build test executables" ON)
option(CODE_COVERAGE "Enable code coverage (GCC/Clang only)" OFF)

# ============================================================================
# Code Coverage (gcov/llvm-cov)
# ============================================================================
if(CODE_COVERAGE AND NOT MSVC)
    message(STATUS "Code coverage enabled")
    set(COVERAGE_FLAGS "--coverage -fprofile-arcs -ftest-coverage")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COVERAGE_FLAGS}")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${COVERAGE_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${COVERAGE_FLAGS}")
endif()

# Configurações de warning
if(MSVC)
    add_compile_options(/W4 /WX- /utf-8)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# ============================================================================
# Dependências (via vcpkg)
# ============================================================================

# Boost (via vcpkg)
find_package(Boost REQUIRED COMPONENTS system iostreams)

# nlohmann_json (via vcpkg)
find_package(nlohmann_json CONFIG REQUIRED)

# spdlog (via vcpkg) - high-performance logging
find_package(spdlog CONFIG REQUIRED)

# ============================================================================
# Biblioteca principal
# ============================================================================

add_library(eventhorizon_lib STATIC
    src/logging/logger.cpp
    src/storage/log_segment.cpp
    src/storage/partition.cpp
    src/protocol/kafka_protocol.cpp
    src/network/server.cpp
    src/broker/broker.cpp
    src/consumer/consumer_group.cpp
)

# Definir versão como macros de compilação
target_compile_definitions(eventhorizon_lib PUBLIC
    EVENT_HORIZON_VERSION="${VERSION_FULL}"
    EVENT_HORIZON_VERSION_BASE="${VERSION_BASE}"
    EVENT_HORIZON_VERSION_SUFFIX="${VERSION_SUFFIX}"
    EVENT_HORIZON_BUILD_NUMBER="${BUILD_NUMBER}"
)

target_include_directories(eventhorizon_lib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src/includes
)

target_link_libraries(eventhorizon_lib PUBLIC
    Boost::system
    Boost::iostreams
    nlohmann_json::nlohmann_json
    spdlog::spdlog
)

# Para Windows - link Ws2_32
if(WIN32)
    target_link_libraries(eventhorizon_lib PUBLIC ws2_32 wsock32)
endif()

# ============================================================================
# Executável principal
# ============================================================================

add_executable(event_horizon
    src/main.cpp
)

target_link_libraries(event_horizon PRIVATE
    eventhorizon_lib
)

# ============================================================================
# Testes
# ============================================================================

if(BUILD_TESTS)
    enable_testing()
    
    # Include FetchContent module
    include(FetchContent)
    
    # Google Test
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.14.0
    )
    FetchContent_MakeAvailable(googletest)
    
    add_executable(eventhorizon_tests
        src/tests/test_log_segment.cpp
        src/tests/test_partition.cpp
        src/tests/test_protocol.cpp
        src/tests/test_consumer_group.cpp
        src/tests/test_broker.cpp
    )
    
    target_link_libraries(eventhorizon_tests PRIVATE
        eventhorizon_lib
        GTest::gtest_main
    )
    
    include(GoogleTest)
    gtest_discover_tests(eventhorizon_tests)
endif()

# ============================================================================
# Instalação
# ============================================================================

install(TARGETS event_horizon
    RUNTIME DESTINATION bin
)

install(DIRECTORY src/
    DESTINATION include/eventhorizon
    FILES_MATCHING PATTERN "*.hpp"
)

# ============================================================================
# Mensagens de configuração
# ============================================================================

message(STATUS "")
message(STATUS "Event Horizon Configuration:")
message(STATUS "  Version:        ${VERSION_FULL}")
message(STATUS "  Version Base:   ${VERSION_BASE}")
message(STATUS "  Version Suffix: ${VERSION_SUFFIX}")
message(STATUS "  Build Number:   ${BUILD_NUMBER}")
message(STATUS "  C++ Standard:   ${CMAKE_CXX_STANDARD}")
message(STATUS "  Build Type:     ${CMAKE_BUILD_TYPE}")
message(STATUS "  Build Tests:    ${BUILD_TESTS}")
message(STATUS "  Code Coverage:  ${CODE_COVERAGE}")
message(STATUS "")