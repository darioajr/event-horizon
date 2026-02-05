#pragma once

#include "../storage/partition.hpp"
#include "../protocol/kafka_protocol.hpp"
#include "../consumer/consumer_group.hpp"
#include <unordered_map>
#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <shared_mutex>
#include <thread>

namespace eventhorizon {

// Forward declarations
namespace network { class Server; }

/**
 * @brief Configuração do broker
 */
struct BrokerConfig {
    int32_t broker_id = 1;
    std::string host = "localhost";
    uint16_t port = 9092;
    std::string log_dir = "./data";
    int32_t num_partitions = 1;
    int16_t replication_factor = 1;
    size_t thread_pool_size = 4;
    std::string cluster_id = "event-horizon-cluster";
    
    static BrokerConfig load(const std::string& config_path);
    void save(const std::string& config_path) const;
};

/**
 * @brief Broker principal do Event Horizon
 * 
 * Gerencia tópicos, partições e conexões de clientes.
 * Implementa o protocolo Kafka para compatibilidade.
 */
class Broker {
public:
    explicit Broker(const std::string& config_path);
    ~Broker();
    
    // Não permitir cópia
    Broker(const Broker&) = delete;
    Broker& operator=(const Broker&) = delete;
    
    /**
     * @brief Inicia o broker e começa a aceitar conexões
     */
    void start();
    
    /**
     * @brief Para o broker graciosamente
     */
    void stop();
    
    /**
     * @brief Verifica se o broker está rodando
     */
    bool is_running() const { return running_; }
    
    // ========================================================================
    // Topic/Partition Management
    // ========================================================================
    
    /**
     * @brief Cria um novo tópico
     */
    void create_topic(const std::string& name, int32_t num_partitions, 
                      int16_t replication_factor);
    
    /**
     * @brief Remove um tópico
     * @param purge_data Se true, remove também os arquivos físicos
     */
    void delete_topic(const std::string& name, bool purge_data = true);
    
    /**
     * @brief Recria um tópico (delete + create com mesmas configurações)
     * Usado pelo Kafka UI "Recreate Topic"
     */
    void recreate_topic(const std::string& name);
    
    /**
     * @brief Remove records antes de um offset em uma partição
     * Usado pelo Kafka UI "Clear Messages" via DeleteRecords API
     * @param offset Use -1 para high watermark (limpar tudo)
     * @return O novo low_watermark após a operação
     */
    int64_t delete_records(const std::string& topic, int32_t partition_id, int64_t offset);
    
    /**
     * @brief Trunca todas as partições de um tópico (limpa todas as mensagens)
     * Usado pelo Kafka UI "Clear Messages"
     */
    void clear_topic_messages(const std::string& name);
    
    /**
     * @brief Lista todos os tópicos
     */
    std::vector<std::string> list_topics() const;
    
    /**
     * @brief Obtém uma partição específica
     */
    Partition* get_partition(const std::string& topic, int32_t partition_id);
    
    // ========================================================================
    // Message Operations
    // ========================================================================
    
    /**
     * @brief Produz uma mensagem em um tópico/partição
     */
    int64_t produce(const std::string& topic, int32_t partition_id,
                    const std::string& key, const std::vector<uint8_t>& value,
                    int64_t timestamp = 0);
    
    /**
     * @brief Produz um RecordBatch bruto em um tópico/partição (mantém CRC original)
     */
    int64_t produce_raw_batch(const std::string& topic, int32_t partition_id,
                              const std::vector<uint8_t>& batch_data, int32_t record_count);
    
    /**
     * @brief Busca mensagens de um tópico/partição
     */
    std::vector<Record> fetch(const std::string& topic, int32_t partition_id,
                               int64_t offset, size_t max_bytes);
    
    /**
     * @brief Busca bytes brutos do log (formato Kafka nativo)
     * @return Par de (bytes do log, número de records)
     */
    std::pair<std::vector<uint8_t>, int32_t> fetch_raw(const std::string& topic, 
                                                        int32_t partition_id,
                                                        int64_t offset, size_t max_bytes);
    
    /**
     * @brief Retorna a configuração do broker
     */
    const BrokerConfig& config() const { return config_; }
    
    /**
     * @brief Retorna o gerenciador de consumer groups
     */
    ConsumerGroupManager& consumer_groups() { return consumer_group_manager_; }

private:
    void load_topics();
    void register_protocol_handlers();
    
    // Protocol request handlers
    std::vector<uint8_t> handle_produce_request(
        const protocol::RequestHeader& header, protocol::BufferReader& reader);
    std::vector<uint8_t> handle_fetch_request(
        const protocol::RequestHeader& header, protocol::BufferReader& reader);
    std::vector<uint8_t> handle_list_offsets_request(
        const protocol::RequestHeader& header, protocol::BufferReader& reader);
    std::vector<uint8_t> handle_metadata_request(
        const protocol::RequestHeader& header, protocol::BufferReader& reader);
    std::vector<uint8_t> handle_delete_topics_request(
        const protocol::RequestHeader& header, protocol::BufferReader& reader);
    std::vector<uint8_t> handle_delete_records_request(
        const protocol::RequestHeader& header, protocol::BufferReader& reader);
    
    BrokerConfig config_;
    std::atomic<bool> running_{false};
    
    std::unique_ptr<network::Server> server_;
    std::unique_ptr<protocol::KafkaProtocolHandler> protocol_handler_;
    
    mutable std::shared_mutex topics_mutex_;
    std::unordered_map<std::string, std::vector<std::unique_ptr<Partition>>> topics_;
    
    // Topic ID (UUID) to topic name mapping for Fetch v13+
    std::unordered_map<std::string, std::vector<uint8_t>> topic_name_to_id_;
    std::string get_topic_name_by_id(const std::vector<uint8_t>& topic_id) const;
    std::vector<uint8_t> get_or_create_topic_id(const std::string& topic_name);
    
    // Consumer Groups Manager
    ConsumerGroupManager consumer_group_manager_;
    
    // Session expiration thread
    std::jthread session_expiration_thread_;
    void session_expiration_loop(std::stop_token stop_token);
};

} // namespace eventhorizon