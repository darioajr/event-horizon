#include <iostream>
#include <csignal>
#include <thread>
#include <atomic>

#include "broker/broker.hpp"
#include "logging/logger.hpp"

// ============================================================================
// Global state for signal handling
// ============================================================================
std::atomic<bool> g_running{true};
eventhorizon::Broker* g_broker = nullptr;

void signal_handler(int signal) {
    LOG_INFO("Received signal {}, shutting down...", signal);
    g_running = false;
    if (g_broker) {
        g_broker->stop();
    }
}

// Version from CMake
#ifndef EVENT_HORIZON_VERSION
#define EVENT_HORIZON_VERSION "1.0.0+0"
#endif

void print_banner() {
    std::cout << R"(
 _____                _     _   _            _                
|  ___|              | |   | | | |          (_)               
| |____   _____ _ __ | |_  | |_| | ___  _ __ _ _______  _ __  
|  __\ \ / / _ \ '_ \| __| |  _  |/ _ \| '__| |_  / _ \| '_ \ 
| |___\ V /  __/ | | | |_  | | | | (_) | |  | |/ / (_) | | | |
\____/ \_/ \___|_| |_|\__| \_| |_/\___/|_|  |_/___\___/|_| |_|
)" << std::endl;
    std::cout << "Kafka-Compatible Event Streaming Platform - v" << EVENT_HORIZON_VERSION << "\n" << std::endl;
}

void print_usage(const char* program) {
    std::cout << "Usage: " << program << " [options]" << std::endl;
    std::cout << "\nOptions:" << std::endl;
    std::cout << "  -c, --config <path>   Path to config file (default: config.json)" << std::endl;
    std::cout << "  -p, --port <port>     Port to listen on (default: 9092)" << std::endl;
    std::cout << "  -d, --data <dir>      Data directory (default: ./data)" << std::endl;
    std::cout << "  -l, --log-level <lvl> Log level: trace, debug, info, warn, error (default: info)" << std::endl;
    std::cout << "  -h, --help            Show this help message" << std::endl;
    std::cout << "\nExample:" << std::endl;
    std::cout << "  " << program << " -p 9092 -d /var/lib/eventhorizon\n" << std::endl;
}

eventhorizon::LogLevel parse_log_level(const std::string& level) {
    if (level == "trace") return eventhorizon::LogLevel::Trace;
    if (level == "debug") return eventhorizon::LogLevel::Debug;
    if (level == "info") return eventhorizon::LogLevel::Info;
    if (level == "warn" || level == "warning") return eventhorizon::LogLevel::Warn;
    if (level == "error") return eventhorizon::LogLevel::Error;
    if (level == "critical") return eventhorizon::LogLevel::Critical;
    return eventhorizon::LogLevel::Info;  // default
}

int main(int argc, char* argv[]) {
    print_banner();
    
    // Parse argumentos
    std::string config_path = "config.json";
    uint16_t port = 9092;
    std::string data_dir = "./data";
    std::string log_level_str = "info";
    
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        
        if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else if ((arg == "-c" || arg == "--config") && i + 1 < argc) {
            config_path = argv[++i];
        } else if ((arg == "-p" || arg == "--port") && i + 1 < argc) {
            port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if ((arg == "-d" || arg == "--data") && i + 1 < argc) {
            data_dir = argv[++i];
        } else if ((arg == "-l" || arg == "--log-level") && i + 1 < argc) {
            log_level_str = argv[++i];
        }
    }
    
    // Inicializar sistema de logging
    eventhorizon::LogConfig log_config;
    log_config.log_dir = data_dir + "/logs";
    log_config.log_file = "eventhorizon.log";
    log_config.console_level = parse_log_level(log_level_str);
    log_config.file_level = eventhorizon::LogLevel::Debug;  // Arquivo sempre mais verbose
    eventhorizon::Logger::init(log_config);
    
    // Configurar signal handlers
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
    
    try {
        // Criar configuração padrão se não existir
        eventhorizon::BrokerConfig config;
        try {
            config = eventhorizon::BrokerConfig::load(config_path);
        } catch (...) {
            // Usar defaults
        }
        
        // Override com argumentos de linha de comando
        config.port = port;
        config.log_dir = data_dir;
        
        // Salvar configuração
        config.save(config_path);
        
        LOG_INFO("Configuration:");
        LOG_INFO("  Broker ID:    {}", config.broker_id);
        LOG_INFO("  Host:         {}", config.host);
        LOG_INFO("  Port:         {}", config.port);
        LOG_INFO("  Data Dir:     {}", config.log_dir);
        LOG_INFO("  Thread Pool:  {}", config.thread_pool_size);
        LOG_INFO("  Log Level:    {}", log_level_str);
        
        // Criar e iniciar broker
        eventhorizon::Broker broker(config_path);
        g_broker = &broker;
        
        broker.start();
        
        LOG_INFO("Event Horizon is ready to accept connections");
        LOG_INFO("Press Ctrl+C to shutdown");
        
        // Loop principal - aguardar shutdown
        while (g_running) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        
        g_broker = nullptr;
        
    } catch (const std::exception& e) {
        LOG_CRITICAL("Fatal error: {}", e.what());
        eventhorizon::Logger::shutdown();
        return 1;
    }
    
    LOG_INFO("Goodbye!");
    eventhorizon::Logger::shutdown();
    return 0;
}
