#include "logger.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/async.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace eventhorizon {

// Definição dos membros estáticos
std::shared_ptr<spdlog::logger> Logger::logger_ = nullptr;
bool Logger::initialized_ = false;

namespace {

spdlog::level::level_enum to_spdlog_level(LogLevel level) {
    switch (level) {
        case LogLevel::Trace:    return spdlog::level::trace;
        case LogLevel::Debug:    return spdlog::level::debug;
        case LogLevel::Info:     return spdlog::level::info;
        case LogLevel::Warn:     return spdlog::level::warn;
        case LogLevel::Error:    return spdlog::level::err;
        case LogLevel::Critical: return spdlog::level::critical;
        case LogLevel::Off:      return spdlog::level::off;
    }
    return spdlog::level::info;
}

} // anonymous namespace

void Logger::init(const LogConfig& config) {
    if (initialized_) {
        return;
    }
    
    try {
        // Criar diretório de logs se não existir
        if (!fs::exists(config.log_dir)) {
            fs::create_directories(config.log_dir);
        }
        
        std::vector<spdlog::sink_ptr> sinks;
        
        // Console sink (colorido)
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        console_sink->set_level(to_spdlog_level(config.console_level));
        if (config.colored_console) {
            // Cores padrão já estão configuradas
        }
        console_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%s:%#] %v");
        sinks.push_back(console_sink);
        
        // File sink (rotating)
        std::string log_path = config.log_dir + "/" + config.log_file;
        auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            log_path,
            config.max_file_size,
            config.max_files
        );
        file_sink->set_level(to_spdlog_level(config.file_level));
        file_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] [%s:%#] [thread %t] %v");
        sinks.push_back(file_sink);
        
        // Criar logger
        if (config.async_mode) {
            // Inicializar thread pool para async logging
            spdlog::init_thread_pool(config.async_queue_size, 1);
            logger_ = std::make_shared<spdlog::async_logger>(
                "eventhorizon",
                sinks.begin(),
                sinks.end(),
                spdlog::thread_pool(),
                spdlog::async_overflow_policy::block
            );
        } else {
            logger_ = std::make_shared<spdlog::logger>(
                "eventhorizon",
                sinks.begin(),
                sinks.end()
            );
        }
        
        // Configurar nível mínimo geral
        auto min_level = std::min(
            to_spdlog_level(config.console_level),
            to_spdlog_level(config.file_level)
        );
        logger_->set_level(min_level);
        
        // Flush automático em warnings e acima
        logger_->flush_on(spdlog::level::warn);
        
        // Registrar como logger padrão
        spdlog::set_default_logger(logger_);
        
        initialized_ = true;
        
        LOG_INFO("Event Horizon logging initialized");
        LOG_DEBUG("Log file: {}", log_path);
        LOG_DEBUG("Async mode: {}", config.async_mode ? "enabled" : "disabled");
        
    } catch (const spdlog::spdlog_ex& ex) {
        // Fallback para console simples se houver erro
        logger_ = spdlog::stdout_color_mt("eventhorizon");
        logger_->error("Failed to initialize file logging: {}", ex.what());
        initialized_ = true;
    }
}

void Logger::init(std::string_view log_file, LogLevel level) {
    LogConfig config;
    config.log_file = std::string(log_file);
    config.console_level = level;
    config.file_level = LogLevel::Debug; // Arquivo sempre mais verbose
    init(config);
}

void Logger::shutdown() {
    if (!initialized_) {
        return;
    }
    
    LOG_INFO("Shutting down logging system");
    
    // Flush todos os logs pendentes
    if (logger_) {
        logger_->flush();
    }
    
    // Limpar
    spdlog::shutdown();
    logger_.reset();
    initialized_ = false;
}

void Logger::flush() {
    if (logger_) {
        logger_->flush();
    }
}

void Logger::set_level(LogLevel level) {
    if (logger_) {
        logger_->set_level(to_spdlog_level(level));
    }
}

std::shared_ptr<spdlog::logger>& Logger::get() {
    return logger_;
}

bool Logger::is_initialized() {
    return initialized_;
}

} // namespace eventhorizon
