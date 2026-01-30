cmake_minimum_required(VERSION 3.16)
project(event_horizon VERSION 1.0.0 LANGUAGES CXX)

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

# Configurações de warning
if(MSVC)
    add_compile_options(/W4 /WX-)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# ============================================================================
# Dependências (via vcpkg)
# ============================================================================

# Boost (via vcpkg)
find_package(Boost REQUIRED COMPONENTS system)

# nlohmann_json (via vcpkg)
find_package(nlohmann_json CONFIG REQUIRED)

# ============================================================================
# Biblioteca principal
# ============================================================================

add_library(eventhorizon_lib STATIC
    src/storage/log_segment.cpp
    src/storage/partition.cpp
    src/protocol/kafka_protocol.cpp
    src/network/server.cpp
    src/broker/broker.cpp
)

target_include_directories(eventhorizon_lib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src/includes
)

target_link_libraries(eventhorizon_lib PUBLIC
    Boost::system
    nlohmann_json::nlohmann_json
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
message(STATUS "  Version:        ${PROJECT_VERSION}")
message(STATUS "  C++ Standard:   ${CMAKE_CXX_STANDARD}")
message(STATUS "  Build Type:     ${CMAKE_BUILD_TYPE}")
message(STATUS "  Build Tests:    ${BUILD_TESTS}")
message(STATUS "")