#include <iostream>
#include <print>
#include <csignal>
#include <thread>
#include <atomic>

#include "broker/broker.hpp"

// ============================================================================
// Global state for signal handling
// ============================================================================
std::atomic<bool> g_running{true};
eventhorizon::Broker* g_broker = nullptr;

void signal_handler(int signal) {
    std::println("\nReceived signal {}, shutting down...", signal);
    g_running = false;
    if (g_broker) {
        g_broker->stop();
    }
}

void print_banner() {
    std::println(R"(
 _____                _     _   _            _                
|  ___|              | |   | | | |          (_)               
| |____   _____ _ __ | |_  | |_| | ___  _ __ _ _______  _ __  
|  __\ \ / / _ \ '_ \| __| |  _  |/ _ \| '__| |_  / _ \| '_ \ 
| |___\ V /  __/ | | | |_  | | | | (_) | |  | |/ / (_) | | | |
\____/ \_/ \___|_| |_|\__| \_| |_/\___/|_|  |_/___\___/|_| |_|

Kafka-Compatible Event Streaming Platform - v1.0.0
)");
}

void print_usage(const char* program) {
    std::println("Usage: {} [options]", program);
    std::println("\nOptions:");
    std::println("  -c, --config <path>   Path to config file (default: config.json)");
    std::println("  -p, --port <port>     Port to listen on (default: 9092)");
    std::println("  -d, --data <dir>      Data directory (default: ./data)");
    std::println("  -h, --help            Show this help message");
    std::println("\nExample:");
    std::println("  {} -p 9092 -d /var/lib/eventhorizon\n", program);
}

int main(int argc, char* argv[]) {
    print_banner();
    
    // Parse argumentos
    std::string config_path = "config.json";
    uint16_t port = 9092;
    std::string data_dir = "./data";
    
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
        }
    }
    
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
        
        std::println("Configuration:");
        std::println("  Broker ID:    {}", config.broker_id);
        std::println("  Host:         {}", config.host);
        std::println("  Port:         {}", config.port);
        std::println("  Data Dir:     {}", config.log_dir);
        std::println("  Thread Pool:  {}\n", config.thread_pool_size);
        
        // Criar e iniciar broker
        eventhorizon::Broker broker(config_path);
        g_broker = &broker;
        
        broker.start();
        
        std::println("\nEvent Horizon is ready to accept connections.");
        std::println("Press Ctrl+C to shutdown.\n");
        
        // Loop principal - aguardar shutdown
        while (g_running) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        
        g_broker = nullptr;
        
    } catch (const std::exception& e) {
        std::println(stderr, "Fatal error: {}", e.what());
        return 1;
    }
    
    std::println("Goodbye!");
    return 0;
}
