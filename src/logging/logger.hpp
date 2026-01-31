#pragma once

#include <spdlog/spdlog.h>
#include <spdlog/fmt/ostr.h>
#include <memory>
#include <string>
#include <string_view>

namespace eventhorizon {

/**
 * @brief Níveis de log suportados
 */
enum class LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Critical,
    Off
};

/**
 * @brief Configuração do sistema de logging
 */
struct LogConfig {
    std::string log_file = "eventhorizon.log";  // Arquivo de log
    std::string log_dir = "./logs";              // Diretório de logs
    LogLevel console_level = LogLevel::Info;     // Nível para console
    LogLevel file_level = LogLevel::Debug;       // Nível para arquivo
    size_t max_file_size = 10 * 1024 * 1024;     // 10 MB por arquivo
    size_t max_files = 5;                        // Manter 5 arquivos rotacionados
    bool async_mode = true;                      // Logging assíncrono
    size_t async_queue_size = 8192;              // Tamanho da fila async
    bool colored_console = true;                 // Console colorido
};

/**
 * @brief Sistema de logging centralizado para Event Horizon
 * 
 * Baseado em spdlog para alta performance.
 * Suporta:
 * - Console colorido
 * - Arquivo com rotação automática
 * - Modo assíncrono (non-blocking)
 * - Múltiplos níveis de log
 */
class Logger {
public:
    /**
     * @brief Inicializa o sistema de logging
     * @param config Configuração do logging
     */
    static void init(const LogConfig& config = LogConfig{});
    
    /**
     * @brief Inicializa com configuração simples
     * @param log_file Nome do arquivo de log
     * @param level Nível mínimo de log
     */
    static void init(std::string_view log_file, LogLevel level = LogLevel::Info);
    
    /**
     * @brief Finaliza o sistema de logging (flush e cleanup)
     */
    static void shutdown();
    
    /**
     * @brief Força escrita de todos os logs pendentes
     */
    static void flush();
    
    /**
     * @brief Define o nível de log global
     */
    static void set_level(LogLevel level);
    
    /**
     * @brief Retorna o logger principal
     */
    static std::shared_ptr<spdlog::logger>& get();
    
    /**
     * @brief Verifica se o logger foi inicializado
     */
    static bool is_initialized();

private:
    static std::shared_ptr<spdlog::logger> logger_;
    static bool initialized_;
};

// ============================================================================
// Macros de conveniência para logging
// ============================================================================

// Logging com source location automático
#define LOG_TRACE(...)    if (eventhorizon::Logger::is_initialized()) SPDLOG_LOGGER_TRACE(eventhorizon::Logger::get(), __VA_ARGS__)
#define LOG_DEBUG(...)    if (eventhorizon::Logger::is_initialized()) SPDLOG_LOGGER_DEBUG(eventhorizon::Logger::get(), __VA_ARGS__)
#define LOG_INFO(...)     if (eventhorizon::Logger::is_initialized()) SPDLOG_LOGGER_INFO(eventhorizon::Logger::get(), __VA_ARGS__)
#define LOG_WARN(...)     if (eventhorizon::Logger::is_initialized()) SPDLOG_LOGGER_WARN(eventhorizon::Logger::get(), __VA_ARGS__)
#define LOG_ERROR(...)    if (eventhorizon::Logger::is_initialized()) SPDLOG_LOGGER_ERROR(eventhorizon::Logger::get(), __VA_ARGS__)
#define LOG_CRITICAL(...) if (eventhorizon::Logger::is_initialized()) SPDLOG_LOGGER_CRITICAL(eventhorizon::Logger::get(), __VA_ARGS__)

} // namespace eventhorizon
