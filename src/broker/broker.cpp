#include "broker.hpp"
#include "../network/server.hpp"
#include "../protocol/kafka_protocol.hpp"
#include "../logging/logger.hpp"
#include <filesystem>
#include <fstream>
#include <set>
#include <algorithm>
#include <ranges>  // C++23
#include <nlohmann/json.hpp>

namespace fs = std::filesystem;

namespace eventhorizon {

// CRC32C (Castagnoli) lookup table
static const uint32_t crc32c_table[256] = {
    0x00000000, 0xF26B8303, 0xE13B70F7, 0x1350F3F4, 0xC79A971F, 0x35F1141C, 0x26A1E7E8, 0xD4CA64EB,
    0x8AD958CF, 0x78B2DBCC, 0x6BE22838, 0x9989AB3B, 0x4D43CFD0, 0xBF284CD3, 0xAC78BF27, 0x5E133C24,
    0x105EC76F, 0xE235446C, 0xF165B798, 0x030E349B, 0xD7C45070, 0x25AFD373, 0x36FF2087, 0xC494A384,
    0x9A879FA0, 0x68EC1CA3, 0x7BBCEF57, 0x89D76C54, 0x5D1D08BF, 0xAF768BBC, 0xBC267848, 0x4E4DFB4B,
    0x20BD8EDE, 0xD2D60DDD, 0xC186FE29, 0x33ED7D2A, 0xE72719C1, 0x154C9AC2, 0x061C6936, 0xF477EA35,
    0xAA64D611, 0x580F5512, 0x4B5FA6E6, 0xB93425E5, 0x6DFE410E, 0x9F95C20D, 0x8CC531F9, 0x7EAEB2FA,
    0x30E349B1, 0xC288CAB2, 0xD1D83946, 0x23B3BA45, 0xF779DEAE, 0x05125DAD, 0x1642AE59, 0xE4292D5A,
    0xBA3A117E, 0x4851927D, 0x5B016189, 0xA96AE28A, 0x7DA08661, 0x8FCB0562, 0x9C9BF696, 0x6EF07595,
    0x417B1DBC, 0xB3109EBF, 0xA0406D4B, 0x522BEE48, 0x86E18AA3, 0x749809A0, 0x67C8FA54, 0x95A37957,
    0xCBB04573, 0x39DBC670, 0x2A8B3584, 0xD8E0B687, 0x0C2AD26C, 0xFE41516F, 0xED11A29B, 0x1F7A2198,
    0x5137DAD3, 0xA35C59D0, 0xB00CAA24, 0x42672927, 0x96AD4DCC, 0x64C6CECF, 0x77963D3B, 0x85FDBE38,
    0xDBEE821C, 0x2985011F, 0x3AD5F2EB, 0xC8BE71E8, 0x1C74150D, 0xEE1F960E, 0xFD4F65FA, 0x0F24E6F9,
    0x615E936E, 0x9335106D, 0x8065E399, 0x720E609A, 0xA6C40471, 0x54AF8772, 0x47FF7486, 0xB594F785,
    0xEBD7CBA1, 0x19BC48A2, 0x0AECBB56, 0xF8873855, 0x2C4D5CBE, 0xDE26DFBD, 0xCD762C49, 0x3F1DAF4A,
    0x71504A01, 0x833BC902, 0x906B3AF6, 0x6200B9F5, 0xB6CADD1E, 0x44A15E1D, 0x57F1ADE9, 0xA59A2EEA,
    0xFB8912CE, 0x09E291CD, 0x1AB26239, 0xE8D9E13A, 0x3C1385D1, 0xCE7806D2, 0xDD28F526, 0x2F437625,
    0x82F63B78, 0x709DB87B, 0x63CD4B8F, 0x91A6C88C, 0x456CAC67, 0xB7072F64, 0xA457DC90, 0x563C5F93,
    0x082F63B7, 0xFA44E0B4, 0xE9141340, 0x1B7F9043, 0xCFB5F4A8, 0x3DDE77AB, 0x2E8E845F, 0xDCE5075C,
    0x92A8FC17, 0x60C37F14, 0x73938CE0, 0x81F80FE3, 0x55326B08, 0xA759E80B, 0xB4091BFF, 0x466298FC,
    0x1871A4D8, 0xEA1A27DB, 0xF94AD42F, 0x0B21572C, 0xDFEB33C7, 0x2D80B0C4, 0x3ED04330, 0xCCBBC033,
    0xA24BB5A6, 0x502036A5, 0x4370C551, 0xB11B4652, 0x65D122B9, 0x97BAA1BA, 0x84EA524E, 0x7681D14D,
    0x2892ED69, 0xDAF96E6A, 0xC9A99D9E, 0x3BC21E9D, 0xEF087A76, 0x1D63F975, 0x0E330A81, 0xFC588982,
    0xB21572C9, 0x407EF1CA, 0x532E023E, 0xA145813D, 0x758FE5D6, 0x87E466D5, 0x94B49521, 0x66DF1622,
    0x38CC2A06, 0xCAA7A905, 0xD9F75AF1, 0x2B9CD9F2, 0xFF56BD19, 0x0D3D3E1A, 0x1E6DCDEE, 0xEC064EED,
    0xC38D26C4, 0x31E6A5C7, 0x22B65633, 0xD0DDD530, 0x0417B1DB, 0xF67C32D8, 0xE52CC12C, 0x1747422F,
    0x49547E0B, 0xBB3FFD08, 0xA86F0EFC, 0x5A048DFF, 0x8ECEE914, 0x7CA56A17, 0x6FF599E3, 0x9D9E1AE0,
    0xD3D3E1AB, 0x21B862A8, 0x32E8915C, 0xC083125F, 0x144976B4, 0xE622F5B7, 0xF5720643, 0x07198540,
    0x590AB964, 0xAB613A67, 0xB831C993, 0x4A5A4A90, 0x9E902E7B, 0x6CFBAD78, 0x7FAB5E8C, 0x8DC0DD8F,
    0xE330A81A, 0x115B2B19, 0x020BD8ED, 0xF0605BEE, 0x24AA3F05, 0xD6C1BC06, 0xC5914FF2, 0x37FACCF1,
    0x69E9F0D5, 0x9B8273D6, 0x88D28022, 0x7AB90321, 0xAE7367CA, 0x5C18E4C9, 0x4F48173D, 0xBD23943E,
    0xF36E6F75, 0x0105EC76, 0x12551F82, 0xE03E9C81, 0x34F4F86A, 0xC69F7B69, 0xD5CF889D, 0x27A40B9E,
    0x79B737BA, 0x8BDCB4B9, 0x988C474D, 0x6AE7C44E, 0xBE2DA0A5, 0x4C4623A6, 0x5F16D052, 0xAD7D5351
};

static uint32_t compute_crc32c(const uint8_t* data, size_t length) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; i++) {
        crc = crc32c_table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

// ============================================================================
// BrokerConfig Implementation
// ============================================================================

BrokerConfig BrokerConfig::load(const std::string& config_path) {
    BrokerConfig config;
    
    if (!fs::exists(config_path)) {
        LOG_INFO("Config file not found, using defaults: {}", config_path);
        return config;
    }
    
    try {
        std::ifstream file(config_path);
        nlohmann::json json;
        file >> json;
        
        if (json.contains("broker_id")) {
            config.broker_id = json["broker_id"].get<int32_t>();
        }
        if (json.contains("host")) {
            config.host = json["host"].get<std::string>();
        }
        if (json.contains("port")) {
            config.port = json["port"].get<uint16_t>();
        }
        if (json.contains("log_dir")) {
            config.log_dir = json["log_dir"].get<std::string>();
        }
        if (json.contains("num_partitions")) {
            config.num_partitions = json["num_partitions"].get<int32_t>();
        }
        if (json.contains("replication_factor")) {
            config.replication_factor = json["replication_factor"].get<int16_t>();
        }
        if (json.contains("thread_pool_size")) {
            config.thread_pool_size = json["thread_pool_size"].get<size_t>();
        }
        if (json.contains("cluster_id")) {
            config.cluster_id = json["cluster_id"].get<std::string>();
        }
        
        LOG_INFO("Loaded config from: {}", config_path);
    } catch (const std::exception& e) {
        LOG_ERROR("Error loading config: {}", e.what());
    }
    
    return config;
}

void BrokerConfig::save(const std::string& config_path) const {
    nlohmann::json json;
    json["broker_id"] = broker_id;
    json["host"] = host;
    json["port"] = port;
    json["log_dir"] = log_dir;
    json["num_partitions"] = num_partitions;
    json["replication_factor"] = replication_factor;
    json["thread_pool_size"] = thread_pool_size;
    json["cluster_id"] = cluster_id;
    
    std::ofstream file(config_path);
    file << json.dump(4);
}

// ============================================================================
// Broker Implementation
// ============================================================================

Broker::Broker(const std::string& config_path)
    : config_(BrokerConfig::load(config_path))
    , running_(false) {
    
    // Criar diretório de logs se não existir
    if (!fs::exists(config_.log_dir)) {
        fs::create_directories(config_.log_dir);
        LOG_INFO("Created log directory: {}", config_.log_dir);
    }
    
    // Inicializar protocol handler
    protocol_handler_ = std::make_unique<protocol::KafkaProtocolHandler>();
    protocol_handler_->set_broker_info(config_.broker_id, config_.host, config_.port);
    protocol_handler_->set_cluster_id(config_.cluster_id);
    protocol_handler_->set_consumer_group_manager(&consumer_group_manager_);
    
    // Configurar versão do broker
    protocol::BrokerVersion version;
    version.name = "eventhorizon";
    version.version = "1.0.0";
    version.commit_id = "dev";
    protocol_handler_->set_broker_version(version);
    
    // Configurar callback para obter tópicos
    protocol_handler_->set_topics_callback([this]() {
        std::vector<protocol::TopicInfo> topics;
        std::shared_lock<std::shared_mutex> lock(topics_mutex_);
        
        for (const auto& [name, partitions] : topics_) {
            protocol::TopicInfo info;
            info.name = name;
            info.num_partitions = static_cast<int32_t>(partitions.size());
            info.replication_factor = config_.replication_factor;
            info.is_internal = false;
            
            // Add partition details for DescribeLogDirs
            for (const auto& partition : partitions) {
                protocol::PartitionDetailInfo pd;
                pd.partition_id = partition->get_partition_id();
                pd.size_bytes = partition->get_size_bytes();
                pd.segment_count = partition->get_segment_count();
                info.partition_details.push_back(pd);
            }
            
            topics.push_back(info);
        }
        return topics;
    });
    
    // Configurar callback para obter consumer groups
    protocol_handler_->set_groups_callback([this]() {
        std::vector<protocol::ConsumerGroupInfo> groups;
        
        for (const auto& desc : consumer_group_manager_.list_groups()) {
            protocol::ConsumerGroupInfo info;
            info.group_id = desc.group_id;
            info.protocol_type = desc.protocol_type;
            info.state = desc.state;
            for (const auto& member : desc.members) {
                info.members.push_back(member.member_id);
            }
            groups.push_back(info);
        }
        return groups;
    });
    
    // Configurar callback para criação de tópicos
    protocol_handler_->set_create_topic_callback([this](const std::string& name, int32_t num_partitions, int16_t replication_factor) {
        // Check if topic already exists
        {
            std::shared_lock<std::shared_mutex> lock(topics_mutex_);
            if (topics_.find(name) != topics_.end()) {
                return; // Already exists
            }
        }
        create_topic(name, num_partitions, replication_factor);
    });
    
    // Registrar handlers customizados para Produce e Fetch
    register_protocol_handlers();
    
    // Iniciar thread de expiração de sessões
    session_expiration_thread_ = std::jthread([this](std::stop_token token) {
        session_expiration_loop(token);
    });
    
    // Carregar tópicos existentes
    load_topics();
    
    LOG_INFO("Broker {} initialized", config_.broker_id);
}

Broker::~Broker() {
    stop();
}

void Broker::register_protocol_handlers() {
    using namespace protocol;
    
    // Handler para Produce
    protocol_handler_->register_handler(ApiKey::Produce, 
        [this](const RequestHeader& header, BufferReader& reader, BufferWriter& writer) {
            return handle_produce_request(header, reader);
        });
    
    // Handler para Fetch
    protocol_handler_->register_handler(ApiKey::Fetch,
        [this](const RequestHeader& header, BufferReader& reader, BufferWriter& writer) {
            return handle_fetch_request(header, reader);
        });
    
    // Handler para ListOffsets
    protocol_handler_->register_handler(ApiKey::ListOffsets,
        [this](const RequestHeader& header, BufferReader& reader, BufferWriter& writer) {
            return handle_list_offsets_request(header, reader);
        });
    
    // Handler para Metadata (com tópicos reais)
    protocol_handler_->register_handler(ApiKey::Metadata,
        [this](const RequestHeader& header, BufferReader& reader, BufferWriter& writer) {
            return handle_metadata_request(header, reader);
        });
    
    // Handler para DeleteTopics (Kafka UI delete topic)
    protocol_handler_->register_handler(ApiKey::DeleteTopics,
        [this](const RequestHeader& header, BufferReader& reader, BufferWriter& writer) {
            return handle_delete_topics_request(header, reader);
        });
    
    // Handler para DeleteRecords (Kafka UI clear messages)
    protocol_handler_->register_handler(ApiKey::DeleteRecords,
        [this](const RequestHeader& header, BufferReader& reader, BufferWriter& writer) {
            return handle_delete_records_request(header, reader);
        });
}

void Broker::load_topics() {
    // Procurar diretórios de partição existentes
    for (const auto& entry : fs::directory_iterator(config_.log_dir)) {
        if (entry.is_directory()) {
            std::string dir_name = entry.path().filename().string();
            
            // Formato: topic-partition
            auto pos = dir_name.rfind('-');
            if (pos != std::string::npos) {
                std::string topic = dir_name.substr(0, pos);
                try {
                    int32_t partition_id = std::stoi(dir_name.substr(pos + 1));
                    
                    // Criar partição
                    auto partition = std::make_unique<Partition>(
                        topic, partition_id, config_.log_dir);
                    
                    topics_[topic].push_back(std::move(partition));
                    
                    LOG_DEBUG("Loaded topic: {} partition: {}", topic, partition_id);
                } catch (...) {
                    // Ignorar diretórios inválidos
                }
            }
        }
    }
    
    // Ordenar partições por ID
    for (auto& [topic, partitions] : topics_) {
        std::sort(partitions.begin(), partitions.end(),
            [](const auto& a, const auto& b) {
                return a->get_partition_id() < b->get_partition_id();
            });
    }
}

void Broker::start() {
    if (running_.exchange(true)) {
        return; // Já está rodando
    }
    
    LOG_INFO("Starting broker on {}:{}", config_.host, config_.port);
    
    // Criar servidor de rede
    server_ = std::make_unique<network::Server>(config_.port, config_.thread_pool_size);
    
    // Definir handler de mensagens
    server_->set_message_handler([this](const std::vector<uint8_t>& request) {
        return protocol_handler_->handle_request(request);
    });
    
    // Iniciar servidor
    server_->start();
    
    LOG_INFO("Broker {} started successfully", config_.broker_id);
}

void Broker::stop() {
    if (!running_.exchange(false)) {
        return;
    }
    
    LOG_INFO("Stopping broker...");
    
    // Parar thread de expiração de sessões
    if (session_expiration_thread_.joinable()) {
        session_expiration_thread_.request_stop();
        session_expiration_thread_.join();
    }
    
    // Parar servidor
    if (server_) {
        server_->stop();
    }
    
    // Flush de todos os tópicos
    {
        std::shared_lock<std::shared_mutex> lock(topics_mutex_);
        for (auto& [topic, partitions] : topics_) {
            for (auto& partition : partitions) {
                partition->flush();
            }
        }
    }
    
    LOG_INFO("Broker stopped");
}

void Broker::session_expiration_loop(std::stop_token stop_token) {
    while (!stop_token.stop_requested()) {
        // Verificar a cada 1 segundo
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        if (stop_token.stop_requested()) {
            break;
        }
        
        // Expirar sessões
        consumer_group_manager_.expire_sessions();
        
        // Limpar grupos vazios (a cada execução - a função tem throttle interno)
        consumer_group_manager_.cleanup_empty_groups();
    }
}

void Broker::create_topic(const std::string& name, int32_t num_partitions, 
                          int16_t replication_factor) {
    std::unique_lock<std::shared_mutex> lock(topics_mutex_);
    
    if (topics_.find(name) != topics_.end()) {
        throw std::runtime_error("Topic already exists: " + name);
    }
    
    std::vector<std::unique_ptr<Partition>> partitions;
    partitions.reserve(num_partitions);
    
    for (int32_t i = 0; i < num_partitions; ++i) {
        partitions.push_back(
            std::make_unique<Partition>(name, i, config_.log_dir));
    }
    
    topics_[name] = std::move(partitions);
    
    LOG_INFO("Created topic: {} with {} partitions", name, num_partitions);
}

void Broker::delete_topic(const std::string& name, bool purge_data) {
    std::unique_lock lock(topics_mutex_);  // C++23 CTAD
    
    auto it = topics_.find(name);
    if (it == topics_.end()) {
        throw std::runtime_error("Topic not found: " + name);
    }
    
    // Remover do mapa (destrutor vai limpar recursos)
    topics_.erase(it);
    
    // Remover diretórios físicos se solicitado
    if (purge_data) {
        for (const auto& entry : fs::directory_iterator(config_.log_dir)) {
            std::string dirname = entry.path().filename().string();
            // Match topic-N pattern (e.g., "my-topic-0", "my-topic-1")
            if (dirname.starts_with(name + "-")) {
                // Verificar se o sufixo é um número (partition id)
                std::string suffix = dirname.substr(name.size() + 1);
                if (!suffix.empty() && std::ranges::all_of(suffix, ::isdigit)) {
                    fs::remove_all(entry.path());
                    LOG_DEBUG("Removed partition directory: {}", entry.path().string());
                }
            }
        }
    }
    
    LOG_INFO("Deleted topic: {}", name);
}

void Broker::recreate_topic(const std::string& name) {
    int32_t num_partitions;
    int16_t replication_factor;
    
    // Primeiro, obter configuração atual
    {
        std::shared_lock lock(topics_mutex_);  // C++23 CTAD
        auto it = topics_.find(name);
        if (it == topics_.end()) {
            throw std::runtime_error("Topic not found: " + name);
        }
        num_partitions = static_cast<int32_t>(it->second.size());
        replication_factor = 1;  // TODO: obter do tópico quando suportarmos replicação
    }
    
    // Deletar com purge
    delete_topic(name, true);
    
    // Recriar
    create_topic(name, num_partitions, replication_factor);
    
    LOG_INFO("Recreated topic: {}", name);
}

int64_t Broker::delete_records(const std::string& topic, int32_t partition_id, int64_t offset) {
    std::shared_lock lock(topics_mutex_);  // C++23 CTAD
    
    auto it = topics_.find(topic);
    if (it == topics_.end()) {
        throw std::runtime_error("Topic not found: " + topic);
    }
    
    if (partition_id < 0 || partition_id >= static_cast<int32_t>(it->second.size())) {
        throw std::runtime_error("Invalid partition: " + std::to_string(partition_id));
    }
    
    return it->second[partition_id]->delete_records_before(offset);
}

void Broker::clear_topic_messages(const std::string& name) {
    std::shared_lock lock(topics_mutex_);  // C++23 CTAD
    
    auto it = topics_.find(name);
    if (it == topics_.end()) {
        throw std::runtime_error("Topic not found: " + name);
    }
    
    for (auto& partition : it->second) {
        partition->truncate();
    }
    
    LOG_INFO("Cleared all messages from topic: {}", name);
}

std::vector<std::string> Broker::list_topics() const {
    std::shared_lock<std::shared_mutex> lock(topics_mutex_);
    
    std::vector<std::string> result;
    result.reserve(topics_.size());
    
    for (const auto& [topic, _] : topics_) {
        result.push_back(topic);
    }
    
    return result;
}

Partition* Broker::get_partition(const std::string& topic, int32_t partition_id) {
    // First try with shared lock
    {
        std::shared_lock<std::shared_mutex> lock(topics_mutex_);
        auto it = topics_.find(topic);
        if (it != topics_.end()) {
            if (partition_id >= 0 && partition_id < static_cast<int32_t>(it->second.size())) {
                return it->second[partition_id].get();
            }
            return nullptr;
        }
    }
    
    // Topic not found in topics_, check if it exists in protocol_handler's stored topics
    if (protocol_handler_) {
        auto stored = protocol_handler_->get_stored_topics();
        for (const auto& st : stored) {
            if (st.name == topic) {
                // Create the topic now (upgrade to unique lock)
                std::unique_lock<std::shared_mutex> lock(topics_mutex_);
                
                // Double-check after getting exclusive lock
                auto it = topics_.find(topic);
                if (it != topics_.end()) {
                    if (partition_id >= 0 && partition_id < static_cast<int32_t>(it->second.size())) {
                        return it->second[partition_id].get();
                    }
                    return nullptr;
                }
                
                // Create the partitions now
                std::vector<std::unique_ptr<Partition>> partitions;
                int32_t num_parts = st.num_partitions > 0 ? st.num_partitions : 1;
                partitions.reserve(num_parts);
                
                for (int32_t i = 0; i < num_parts; ++i) {
                    partitions.push_back(
                        std::make_unique<Partition>(topic, i, config_.log_dir));
                }
                
                LOG_INFO("Lazily created topic: {} with {} partitions", topic, num_parts);
                
                topics_[topic] = std::move(partitions);
                
                // Now return the partition
                if (partition_id >= 0 && partition_id < num_parts) {
                    return topics_[topic][partition_id].get();
                }
                return nullptr;
            }
        }
    }
    
    return nullptr;
}

std::string Broker::get_topic_name_by_id(const std::vector<uint8_t>& topic_id) const {
    // Search through our topic_id mapping
    for (const auto& [name, id] : topic_name_to_id_) {
        if (id == topic_id) {
            return name;
        }
    }
    return ""; // Not found
}

std::vector<uint8_t> Broker::get_or_create_topic_id(const std::string& topic_name) {
    auto it = topic_name_to_id_.find(topic_name);
    if (it != topic_name_to_id_.end()) {
        return it->second;
    }
    
    // Generate a deterministic UUID based on topic name
    // Using a simple hash-based approach for consistency
    std::vector<uint8_t> uuid(16, 0);
    std::hash<std::string> hasher;
    size_t hash1 = hasher(topic_name);
    size_t hash2 = hasher(topic_name + "_uuid");
    
    // Fill UUID with hash bytes
    for (int i = 0; i < 8 && i < 16; ++i) {
        uuid[i] = static_cast<uint8_t>((hash1 >> (i * 8)) & 0xFF);
    }
    for (int i = 0; i < 8 && (i + 8) < 16; ++i) {
        uuid[i + 8] = static_cast<uint8_t>((hash2 >> (i * 8)) & 0xFF);
    }
    
    // Set version (4) and variant bits for valid UUID format
    uuid[6] = (uuid[6] & 0x0F) | 0x40; // Version 4
    uuid[8] = (uuid[8] & 0x3F) | 0x80; // Variant
    
    topic_name_to_id_[topic_name] = uuid;
    return uuid;
}

// Helper to decode zigzag-encoded varint
static int32_t decode_varint(const uint8_t* data, size_t& offset, size_t max_size) {
    int32_t result = 0;
    int shift = 0;
    while (offset < max_size) {
        uint8_t byte = data[offset++];
        result |= (static_cast<int32_t>(byte & 0x7F) << shift);
        if ((byte & 0x80) == 0) break;
        shift += 7;
    }
    // Decode zigzag
    return (result >> 1) ^ -(result & 1);
}

// Helper to decode zigzag-encoded varlong
static int64_t decode_varlong(const uint8_t* data, size_t& offset, size_t max_size) {
    int64_t result = 0;
    int shift = 0;
    while (offset < max_size) {
        uint8_t byte = data[offset++];
        result |= (static_cast<int64_t>(byte & 0x7F) << shift);
        if ((byte & 0x80) == 0) break;
        shift += 7;
    }
    // Decode zigzag
    return (result >> 1) ^ -(result & 1);
}

// Structure to hold parsed record from incoming RecordBatch
struct ParsedRecord {
    std::string key;
    std::vector<uint8_t> value;
    int64_t timestamp;
};

// Parse a Kafka RecordBatch and extract individual records
static std::vector<ParsedRecord> parse_record_batch(const std::vector<uint8_t>& data) {
    std::vector<ParsedRecord> records;
    
    if (data.size() < 61) {
        LOG_WARN("[parse_record_batch] Data too small: {} bytes", data.size());
        return records;
    }
    
    size_t pos = 0;
    
    // RecordBatch header
    // baseOffset (8 bytes) - big-endian
    int64_t base_offset = 0;
    for (int i = 0; i < 8; i++) {
        base_offset = (base_offset << 8) | data[pos++];
    }
    
    // batchLength (4 bytes) - big-endian
    int32_t batch_length = 0;
    for (int i = 0; i < 4; i++) {
        batch_length = (batch_length << 8) | data[pos++];
    }
    
    // partitionLeaderEpoch (4 bytes)
    pos += 4;
    
    // magic (1 byte)
    uint8_t magic = data[pos++];
    if (magic != 2) {
        LOG_WARN("[parse_record_batch] Unsupported magic: {}", (int)magic);
        return records;
    }
    
    // CRC (4 bytes)
    pos += 4;
    
    // attributes (2 bytes)
    pos += 2;
    
    // lastOffsetDelta (4 bytes)
    pos += 4;
    
    // firstTimestamp (8 bytes) - big-endian
    int64_t first_timestamp = 0;
    for (int i = 0; i < 8; i++) {
        first_timestamp = (first_timestamp << 8) | data[pos++];
    }
    
    // maxTimestamp (8 bytes)
    pos += 8;
    
    // producerId (8 bytes)
    pos += 8;
    
    // producerEpoch (2 bytes)
    pos += 2;
    
    // baseSequence (4 bytes)
    pos += 4;
    
    // recordCount (4 bytes) - big-endian
    int32_t record_count = 0;
    for (int i = 0; i < 4; i++) {
        record_count = (record_count << 8) | data[pos++];
    }
    
    LOG_TRACE("[parse_record_batch] base_offset={} batch_length={} first_timestamp={} record_count={} pos={}",
              base_offset, batch_length, first_timestamp, record_count, pos);
    
    // Now parse individual records
    for (int32_t i = 0; i < record_count && pos < data.size(); i++) {
        // Record length (varint)
        int32_t record_len = decode_varint(data.data(), pos, data.size());
        if (record_len <= 0) {
            LOG_WARN("[parse_record_batch] Invalid record length: {}", record_len);
            break;
        }
        
        size_t record_start = pos;
        
        // attributes (1 byte)
        pos++;
        
        // timestampDelta (varlong)
        int64_t ts_delta = decode_varlong(data.data(), pos, data.size());
        
        // offsetDelta (varint)
        int32_t offset_delta = decode_varint(data.data(), pos, data.size());
        
        // keyLength (varint, -1 means null)
        int32_t key_len = decode_varint(data.data(), pos, data.size());
        
        // key bytes
        std::string key;
        if (key_len > 0) {
            key.assign(reinterpret_cast<const char*>(data.data() + pos), key_len);
            pos += key_len;
        }
        
        // valueLength (varint, -1 means null)
        int32_t value_len = decode_varint(data.data(), pos, data.size());
        
        // value bytes
        std::vector<uint8_t> value;
        if (value_len > 0) {
            value.assign(data.begin() + pos, data.begin() + pos + value_len);
            pos += value_len;
        }
        
        // headersCount (varint)
        int32_t headers_count = decode_varint(data.data(), pos, data.size());
        
        // Skip headers
        for (int32_t h = 0; h < headers_count; h++) {
            int32_t header_key_len = decode_varint(data.data(), pos, data.size());
            if (header_key_len > 0) pos += header_key_len;
            int32_t header_val_len = decode_varint(data.data(), pos, data.size());
            if (header_val_len > 0) pos += header_val_len;
        }
        
        ParsedRecord rec;
        rec.key = key;
        rec.value = value;
        rec.timestamp = first_timestamp + ts_delta;
        
        LOG_TRACE("[parse_record_batch] Record {}: key={} value_len={} timestamp={}",
                  i, key, value_len, rec.timestamp);
        
        records.push_back(std::move(rec));
    }
    
    return records;
}

int64_t Broker::produce(const std::string& topic, int32_t partition_id,
                        const std::string& key, const std::vector<uint8_t>& value,
                        int64_t timestamp) {
    // Auto-criar tópico se não existir
    {
        std::shared_lock<std::shared_mutex> lock(topics_mutex_);
        if (topics_.find(topic) == topics_.end()) {
            lock.unlock();
            create_topic(topic, config_.num_partitions, config_.replication_factor);
        }
    }
    
    Partition* partition = get_partition(topic, partition_id);
    if (!partition) {
        throw std::runtime_error("Partition not found");
    }
    
    return partition->produce(key, value, timestamp);
}

int64_t Broker::produce_raw_batch(const std::string& topic, int32_t partition_id,
                                   const std::vector<uint8_t>& batch_data, int32_t record_count) {
    // get_partition already handles topic lookup and lazy creation
    Partition* partition = get_partition(topic, partition_id);
    if (!partition) {
        throw std::runtime_error("Partition not found");
    }
    
    return partition->produce_raw_batch(batch_data, record_count);
}

std::vector<Record> Broker::fetch(const std::string& topic, int32_t partition_id,
                                  int64_t offset, size_t max_bytes) {
    Partition* partition = get_partition(topic, partition_id);
    if (!partition) {
        return {};
    }
    
    return partition->fetch(offset, max_bytes);
}

std::pair<std::vector<uint8_t>, int32_t> Broker::fetch_raw(const std::string& topic, 
                                                            int32_t partition_id,
                                                            int64_t offset, size_t max_bytes) {
    Partition* partition = get_partition(topic, partition_id);
    if (!partition) {
        return {{}, 0};
    }
    
    return partition->fetch_raw(offset, max_bytes);
}

// ============================================================================
// Protocol Request Handlers
// ============================================================================

std::vector<uint8_t> Broker::handle_produce_request(
    const protocol::RequestHeader& header, protocol::BufferReader& reader) {
    
    using namespace protocol;
    
    // Produce v9+ uses flexible format
    bool is_flexible = (header.api_version >= 9);
    
    // Parse produce request
    std::string transactional_id;
    if (header.api_version >= 3) {
        if (is_flexible) {
            transactional_id = reader.read_compact_nullable_string();
        } else {
            transactional_id = reader.read_nullable_string();
        }
    }
    int16_t acks = reader.read_int16();
    int32_t timeout = reader.read_int32();
    
    // Pre-allocate response buffer (typical produce response is ~100-500 bytes)
    BufferWriter writer(512);
    writer.write_int32(header.correlation_id);
    
    // Response header v1 for flexible versions - TAG_BUFFER
    if (is_flexible) {
        writer.write_unsigned_varint(0);  // empty tagged fields in header
    }
    
    // Parse topics - COMPACT_ARRAY for flexible versions
    int32_t topic_count;
    if (is_flexible) {
        uint32_t n = reader.read_unsigned_varint();
        topic_count = (n > 0) ? static_cast<int32_t>(n - 1) : 0;
    } else {
        topic_count = reader.read_int32();
    }
    
    // Write topics response
    if (is_flexible) {
        writer.write_unsigned_varint(static_cast<uint32_t>(topic_count + 1));
    } else {
        writer.write_int32(topic_count);
    }
    
    for (int32_t t = 0; t < topic_count; ++t) {
        std::string topic_name;
        if (is_flexible) {
            topic_name = reader.read_compact_string();
        } else {
            topic_name = reader.read_string();
        }
        
        // Write topic name in response
        if (is_flexible) {
            writer.write_compact_string(topic_name);
        } else {
            writer.write_string(topic_name);
        }
        
        // Partitions - COMPACT_ARRAY for flexible versions
        int32_t partition_count;
        if (is_flexible) {
            uint32_t n = reader.read_unsigned_varint();
            partition_count = (n > 0) ? static_cast<int32_t>(n - 1) : 0;
        } else {
            partition_count = reader.read_int32();
        }
        
        if (is_flexible) {
            writer.write_unsigned_varint(static_cast<uint32_t>(partition_count + 1));
        } else {
            writer.write_int32(partition_count);
        }
        
        for (int32_t p = 0; p < partition_count; ++p) {
            int32_t partition_id = reader.read_int32();
            
            // Record set - COMPACT_BYTES for flexible versions
            std::vector<uint8_t> record_set;
            if (is_flexible) {
                record_set = reader.read_compact_nullable_bytes();
            } else {
                record_set = reader.read_nullable_bytes();
            }
            
            // Read partition tagged fields for flexible versions
            if (is_flexible) {
                reader.read_unsigned_varint(); // skip tagged fields
            }
            
            writer.write_int32(partition_id);
            
            try {
                // Store the RecordBatch directly (preserves original CRC)
                // We only need to parse it to get record_count for logging
                int64_t base_offset = 0;
                int32_t record_count = 0;
                if (!record_set.empty() && record_set.size() >= 61) {
                    // Extract record_count from batch header (offset 57-60, big-endian)
                    // RecordBatch format: baseOffset(8) + batchLength(4) + partitionLeaderEpoch(4) 
                    // + magic(1) + crc(4) + attributes(2) + lastOffsetDelta(4) 
                    // + firstTimestamp(8) + maxTimestamp(8) + producerId(8) + producerEpoch(2)
                    // + baseSequence(4) + recordCount(4)
                    size_t pos = 8 + 4 + 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4; // = 57
                    record_count = (static_cast<int32_t>(record_set[pos]) << 24) |
                                   (static_cast<int32_t>(record_set[pos+1]) << 16) |
                                   (static_cast<int32_t>(record_set[pos+2]) << 8) |
                                   static_cast<int32_t>(record_set[pos+3]);
                    
                    LOG_TRACE("[produce_raw_batch] Storing raw batch with {} records", record_count);
                    
                    // Store the raw batch (only updates baseOffset, keeps original CRC)
                    base_offset = produce_raw_batch(topic_name, partition_id, record_set, record_count);
                }
                
                writer.write_int16(static_cast<int16_t>(ErrorCode::None));
                writer.write_int64(base_offset);
                
                // Append time (v2+)
                if (header.api_version >= 2) {
                    writer.write_int64(-1); // log append time
                }
                
                // Log start offset (v5+)
                if (header.api_version >= 5) {
                    writer.write_int64(0);
                }
                
                // Record errors (v8+)
                if (header.api_version >= 8) {
                    if (is_flexible) {
                        writer.write_unsigned_varint(1); // empty COMPACT_ARRAY (length 0+1)
                    } else {
                        writer.write_int32(0);
                    }
                    // Error message (v8+) - COMPACT_NULLABLE_STRING
                    if (is_flexible) {
                        writer.write_unsigned_varint(0); // null string
                    }
                }
                
                // Partition tagged fields for flexible versions
                if (is_flexible) {
                    writer.write_unsigned_varint(0); // no tagged fields
                }
            } catch (const std::exception& e) {
                writer.write_int16(static_cast<int16_t>(ErrorCode::Unknown));
                writer.write_int64(-1);
                if (header.api_version >= 2) writer.write_int64(-1);
                if (header.api_version >= 5) writer.write_int64(-1);
                if (header.api_version >= 8) {
                    if (is_flexible) {
                        writer.write_unsigned_varint(1); // empty record errors array
                        writer.write_unsigned_varint(0); // null error message
                    } else {
                        writer.write_int32(0);
                    }
                }
                if (is_flexible) writer.write_unsigned_varint(0);
            }
        }
        
        // Topic tagged fields for flexible versions
        if (is_flexible) {
            reader.read_unsigned_varint(); // skip request tagged fields
            writer.write_unsigned_varint(0); // no response tagged fields
        }
    }
    
    // Request tagged fields for flexible versions
    if (is_flexible) {
        reader.read_unsigned_varint(); // skip tagged fields
    }
    
    // Throttle time (v1+)
    if (header.api_version >= 1) {
        writer.write_int32(0);
    }
    
    // Response tagged fields for flexible versions
    if (is_flexible) {
        writer.write_unsigned_varint(0); // no tagged fields
    }
    
    auto response = writer.data();
    LOG_TRACE("Produce response size={} bytes", response.size());
    
    return response;
}

std::vector<uint8_t> Broker::handle_fetch_request(
    const protocol::RequestHeader& header, protocol::BufferReader& reader) {
    
    using namespace protocol;
    
    // We only support Fetch v12 - reject other versions
    bool is_flexible = (header.api_version >= 12);
    
    LOG_TRACE("Fetch request v{} (flexible={})", header.api_version, is_flexible);
    
    // Return UNSUPPORTED_VERSION for v13+ since they use topic_id (UUID) format
    if (header.api_version > 12) {
        LOG_WARN("Unsupported Fetch version {}", header.api_version);
        BufferWriter writer;
        writer.write_int32(header.correlation_id);
        writer.write_unsigned_varint(0); // header tagged fields
        writer.write_int32(0); // throttle_time
        writer.write_int16(static_cast<int16_t>(ErrorCode::UnsupportedVersion));
        writer.write_int32(0); // session_id
        writer.write_unsigned_varint(1); // empty topics array
        writer.write_unsigned_varint(0); // tagged fields
        return writer.data();
    }
    
    // Parse Fetch request
    int32_t replica_id = reader.read_int32();
    int32_t max_wait_ms = reader.read_int32();
    int32_t min_bytes = reader.read_int32();
    
    // max_bytes (v3+)
    int32_t max_bytes = 0x7fffffff;
    if (header.api_version >= 3) {
        max_bytes = reader.read_int32();
    }
    
    // isolation_level (v4+)
    if (header.api_version >= 4) {
        reader.read_int8();
    }
    
    // session_id, session_epoch (v7+)
    int32_t session_id = 0;
    if (header.api_version >= 7) {
        session_id = reader.read_int32();
        reader.read_int32(); // session_epoch
    }
    
    // Parse topics array
    struct FetchPartition {
        int32_t partition_id;
        int32_t current_leader_epoch;
        int64_t fetch_offset;
        int64_t log_start_offset;
        int32_t partition_max_bytes;
    };
    struct FetchTopic {
        std::string name;
        std::vector<uint8_t> topic_id; // 16-byte UUID for v13+
        std::vector<FetchPartition> partitions;
    };
    std::vector<FetchTopic> topics;
    
    int32_t topic_count;
    if (is_flexible) {
        uint32_t n = reader.read_unsigned_varint();
        topic_count = (n > 0) ? static_cast<int32_t>(n - 1) : 0;
    } else {
        topic_count = reader.read_int32();
    }
    
    for (int32_t t = 0; t < topic_count; ++t) {
        FetchTopic topic;
        
        // v12 uses topic name (compact string for flexible format)
        if (is_flexible) {
            topic.name = reader.read_compact_string();
        } else {
            topic.name = reader.read_string();
        }
        
        int32_t partition_count;
        if (is_flexible) {
            uint32_t n = reader.read_unsigned_varint();
            partition_count = (n > 0) ? static_cast<int32_t>(n - 1) : 0;
        } else {
            partition_count = reader.read_int32();
        }
        
        for (int32_t p = 0; p < partition_count; ++p) {
            FetchPartition fp;
            fp.partition_id = reader.read_int32();
            fp.current_leader_epoch = (header.api_version >= 9) ? reader.read_int32() : -1;
            fp.fetch_offset = reader.read_int64();
            
            // last_fetched_epoch (v12+)
            if (header.api_version >= 12) {
                reader.read_int32(); // last_fetched_epoch
            }
            
            fp.log_start_offset = (header.api_version >= 5) ? reader.read_int64() : -1;
            fp.partition_max_bytes = reader.read_int32();
            
            if (is_flexible) {
                reader.read_unsigned_varint(); // partition tagged fields
            }
            
            topic.partitions.push_back(fp);
        }
        
        if (is_flexible) {
            reader.read_unsigned_varint(); // topic tagged fields
        }
        
        topics.push_back(topic);
    }
    
    // forgotten_topics_data (v7+)
    if (header.api_version >= 7) {
        int32_t forgotten_count;
        if (is_flexible) {
            uint32_t n = reader.read_unsigned_varint();
            forgotten_count = (n > 0) ? static_cast<int32_t>(n - 1) : 0;
        } else {
            forgotten_count = reader.read_int32();
        }
        for (int32_t i = 0; i < forgotten_count; ++i) {
            // Skip topic name or UUID
            if (header.api_version >= 13) {
                for (int j = 0; j < 16; ++j) reader.read_int8(); // topic_id UUID
            } else if (is_flexible) {
                reader.read_compact_string();
            } else {
                reader.read_string();
            }
            // Skip partitions
            int32_t fp_count;
            if (is_flexible) {
                uint32_t n = reader.read_unsigned_varint();
                fp_count = (n > 0) ? static_cast<int32_t>(n - 1) : 0;
            } else {
                fp_count = reader.read_int32();
            }
            for (int32_t j = 0; j < fp_count; ++j) {
                reader.read_int32(); // partition
            }
            if (is_flexible) {
                reader.read_unsigned_varint(); // tagged fields
            }
        }
    }
    
    // rack_id (v11+)
    if (header.api_version >= 11) {
        if (is_flexible) {
            reader.read_compact_string();
        } else {
            reader.read_string();
        }
    }
    
    // tagged fields at end of request
    if (is_flexible) {
        reader.read_unsigned_varint();
    }
    
    // Build response
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    // Response header v1 for flexible versions - TAG_BUFFER
    if (is_flexible) {
        writer.write_unsigned_varint(0);
    }
    
    // Throttle time (v1+)
    if (header.api_version >= 1) {
        writer.write_int32(0);
    }
    
    // Error code (v7+)
    if (header.api_version >= 7) {
        writer.write_int16(static_cast<int16_t>(ErrorCode::None));
        writer.write_int32(session_id); // session_id
    }
    
    // Topics array
    if (is_flexible) {
        writer.write_unsigned_varint(static_cast<uint32_t>(topics.size() + 1));
    } else {
        writer.write_int32(static_cast<int32_t>(topics.size()));
    }
    
    for (const auto& topic : topics) {
        // v12 uses topic name (compact string for flexible format)
        if (is_flexible) {
            writer.write_compact_string(topic.name);
        } else {
            writer.write_string(topic.name);
        }
        
        // Partitions array
        if (is_flexible) {
            writer.write_unsigned_varint(static_cast<uint32_t>(topic.partitions.size() + 1));
        } else {
            writer.write_int32(static_cast<int32_t>(topic.partitions.size()));
        }
        
        for (const auto& fp : topic.partitions) {
            writer.write_int32(fp.partition_id);
            
            // Get partition data
            Partition* partition = get_partition(topic.name, fp.partition_id);
            
            if (!partition) {
                // Partition not found
                writer.write_int16(static_cast<int16_t>(ErrorCode::UnknownTopicOrPartition));
                writer.write_int64(0); // high_watermark
                if (header.api_version >= 4) writer.write_int64(-1); // last_stable_offset
                if (header.api_version >= 5) writer.write_int64(0);  // log_start_offset
                if (header.api_version >= 4) {
                    // aborted_transactions
                    if (is_flexible) writer.write_unsigned_varint(1);
                    else writer.write_int32(0);
                }
                if (header.api_version >= 11) writer.write_int32(-1); // preferred_read_replica
                // records (null)
                if (is_flexible) writer.write_unsigned_varint(0);
                else writer.write_int32(-1);
                if (is_flexible) writer.write_unsigned_varint(0); // tagged fields
                continue;
            }
            
            // Fetch raw bytes directly from log file (already in Kafka format)
            auto [raw_data, record_count] = fetch_raw(topic.name, fp.partition_id, 
                                                       fp.fetch_offset, 
                                                       static_cast<size_t>(fp.partition_max_bytes));
            
            int64_t high_watermark = partition->get_log_end_offset();
            int64_t log_start_offset = partition->get_log_start_offset();
            
            LOG_TRACE("Fetch: topic={} partition={} fetch_offset={} hwm={} raw_bytes={} records={}",
                      topic.name, fp.partition_id, fp.fetch_offset, high_watermark, 
                      raw_data.size(), record_count);
            
            writer.write_int16(static_cast<int16_t>(ErrorCode::None));
            writer.write_int64(high_watermark);
            
            if (header.api_version >= 4) {
                writer.write_int64(high_watermark); // last_stable_offset
            }
            if (header.api_version >= 5) {
                writer.write_int64(log_start_offset);
            }
            if (header.api_version >= 4) {
                // aborted_transactions (empty)
                if (is_flexible) writer.write_unsigned_varint(1);
                else writer.write_int32(0);
            }
            if (header.api_version >= 11) {
                writer.write_int32(-1); // preferred_read_replica
            }
            
            // Write raw log data (already in Kafka RecordBatch format)
            // In flexible versions (v12+), records use compact_bytes format (varint size + 1)
            // In non-flexible versions, it uses int32 size
            if (raw_data.empty()) {
                // No records - null bytes
                if (is_flexible) {
                    writer.write_unsigned_varint(0); // compact null (0 = null)
                } else {
                    writer.write_int32(-1);
                }
            } else {
                // Write raw data directly - it's already in Kafka format
                if (is_flexible) {
                    // compact_bytes: write size+1 as varint, then data
                    writer.write_unsigned_varint(static_cast<uint32_t>(raw_data.size() + 1));
                    writer.write_raw(raw_data.data(), raw_data.size());
                } else {
                    writer.write_int32(static_cast<int32_t>(raw_data.size()));
                    writer.write_raw(raw_data.data(), raw_data.size());
                }
            }
            
            // Partition tagged fields
            if (is_flexible) {
                writer.write_unsigned_varint(0);
            }
        }
        
        // Topic tagged fields
        if (is_flexible) {
            writer.write_unsigned_varint(0);
        }
    }
    
    // Response tagged fields
    if (is_flexible) {
        writer.write_unsigned_varint(0);
    }
    
    return writer.data();
}

std::vector<uint8_t> Broker::handle_list_offsets_request(
    const protocol::RequestHeader& header, protocol::BufferReader& reader) {
    
    using namespace protocol;
    
    // ListOffsets v6+ uses flexible format
    bool is_flexible = (header.api_version >= 6);
    
    LOG_TRACE("ListOffsets v{} (flexible={})", header.api_version, is_flexible);
    
    // Parse request
    // replica_id (all versions)
    int32_t replica_id = reader.read_int32();
    LOG_TRACE("ListOffsets replica_id={}", replica_id);
    
    // isolation_level (v2+)
    if (header.api_version >= 2) {
        int8_t iso = reader.read_int8();
        LOG_TRACE("ListOffsets isolation_level={}", (int)iso);
    }
    
    // Parse topics array
    struct OffsetPartition {
        int32_t partition_id;
        int32_t current_leader_epoch;
        int64_t timestamp;
    };
    struct OffsetTopic {
        std::string name;
        std::vector<OffsetPartition> partitions;
    };
    std::vector<OffsetTopic> topics;
    
    int32_t topic_count;
    if (is_flexible) {
        uint32_t n = reader.read_unsigned_varint();
        topic_count = (n > 0) ? static_cast<int32_t>(n - 1) : 0;
    } else {
        topic_count = reader.read_int32();
    }
    
    for (int32_t t = 0; t < topic_count; ++t) {
        OffsetTopic topic;
        if (is_flexible) {
            topic.name = reader.read_compact_string();
        } else {
            topic.name = reader.read_string();
        }
        
        int32_t partition_count;
        if (is_flexible) {
            uint32_t n = reader.read_unsigned_varint();
            partition_count = (n > 0) ? static_cast<int32_t>(n - 1) : 0;
        } else {
            partition_count = reader.read_int32();
        }
        
        for (int32_t p = 0; p < partition_count; ++p) {
            OffsetPartition op;
            op.partition_id = reader.read_int32();
            op.current_leader_epoch = (header.api_version >= 4) ? reader.read_int32() : -1;
            op.timestamp = reader.read_int64();
            
            LOG_TRACE("ListOffsets partition={} leader_epoch={} timestamp={}",
                      op.partition_id, op.current_leader_epoch, op.timestamp);
            
            if (is_flexible) {
                reader.read_unsigned_varint(); // partition tagged fields
            }
            
            topic.partitions.push_back(op);
        }
        
        if (is_flexible) {
            reader.read_unsigned_varint(); // topic tagged fields
        }
        
        // Sort partitions by partition_id for consistent response ordering
        std::sort(topic.partitions.begin(), topic.partitions.end(),
            [](const OffsetPartition& a, const OffsetPartition& b) {
                return a.partition_id < b.partition_id;
            });
        
        topics.push_back(topic);
    }
    
    // Build response
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    // Response header v1 for flexible versions - TAG_BUFFER
    if (is_flexible) {
        writer.write_unsigned_varint(0);
    }
    
    // Throttle time (v2+)
    if (header.api_version >= 2) {
        writer.write_int32(0);
    }
    
    // Topics array
    if (is_flexible) {
        writer.write_unsigned_varint(static_cast<uint32_t>(topics.size() + 1));
    } else {
        writer.write_int32(static_cast<int32_t>(topics.size()));
    }
    
    for (const auto& topic : topics) {
        // Topic name
        if (is_flexible) {
            writer.write_compact_string(topic.name);
        } else {
            writer.write_string(topic.name);
        }
        
        // Partitions array
        if (is_flexible) {
            writer.write_unsigned_varint(static_cast<uint32_t>(topic.partitions.size() + 1));
        } else {
            writer.write_int32(static_cast<int32_t>(topic.partitions.size()));
        }
        
        for (const auto& op : topic.partitions) {
            writer.write_int32(op.partition_id);
            
            Partition* partition = get_partition(topic.name, op.partition_id);
            
            if (!partition) {
                writer.write_int16(static_cast<int16_t>(ErrorCode::UnknownTopicOrPartition));
                // v8+ has different field order
                if (header.api_version >= 8) {
                    writer.write_int64(-1); // offset
                    writer.write_int64(-1); // timestamp
                    writer.write_int32(-1); // leader_epoch
                } else {
                    if (header.api_version >= 1) {
                        writer.write_int64(-1); // timestamp
                    }
                    writer.write_int64(-1); // offset
                    if (header.api_version >= 4) {
                        writer.write_int32(-1); // leader_epoch
                    }
                }
                if (is_flexible) {
                    writer.write_unsigned_varint(0); // tagged fields
                }
                continue;
            }
            
            int64_t offset;
            int64_t timestamp = -1;
            
            // timestamp: -1 = latest, -2 = earliest, -3 = max timestamp (v7+)
            if (op.timestamp == -1) {
                // Latest offset
                offset = partition->get_log_end_offset();
            } else if (op.timestamp == -2) {
                // Earliest offset
                offset = partition->get_log_start_offset();
            } else {
                // Find offset by timestamp (simplified - return end offset)
                offset = partition->get_log_end_offset();
            }
            
            LOG_TRACE("ListOffsets returning offset={} timestamp={}", offset, timestamp);
            
            writer.write_int16(static_cast<int16_t>(ErrorCode::None));
            
            // ListOffsets response format (all versions v1+):
            // error_code, timestamp, offset, [leader_epoch v4+]
            // Note: The field order is the same for all versions
            if (header.api_version >= 1) {
                writer.write_int64(timestamp);
            }
            writer.write_int64(offset);
            if (header.api_version >= 4) {
                writer.write_int32(0); // leader_epoch (0 = valid epoch)
            }
            if (is_flexible) {
                writer.write_unsigned_varint(0); // tagged fields
            }
        }
        
        // Topic tagged fields
        if (is_flexible) {
            writer.write_unsigned_varint(0);
        }
    }
    
    // Response tagged fields
    if (is_flexible) {
        writer.write_unsigned_varint(0);
    }
    
    auto response = writer.data();
    LOG_TRACE("ListOffsets response size={} bytes", response.size());
    
    return response;
}

std::vector<uint8_t> Broker::handle_metadata_request(
    const protocol::RequestHeader& header, protocol::BufferReader& reader) {
    
    using namespace protocol;
    
    // v9+ usa formato flexible
    bool flexible = header.api_version >= 9;
    
    // Parse topics (pode ser null = todos os tópicos)
    std::vector<std::string> requested_topics;
    bool all_topics = false;
    
    try {
        if (flexible) {
            // Compact array: length + 1
            uint32_t topics_count = reader.read_unsigned_varint();
            if (topics_count == 0) {
                all_topics = true; // null array
            } else {
                topics_count--;
                for (uint32_t i = 0; i < topics_count; ++i) {
                    // Compact string: length + 1
                    uint32_t name_len = reader.read_unsigned_varint();
                    if (name_len > 0) {
                        name_len--;
                        std::string name(name_len, '\0');
                        for (uint32_t j = 0; j < name_len; j++) {
                            name[j] = static_cast<char>(reader.read_int8());
                        }
                        requested_topics.push_back(name);
                    }
                    // Tagged fields para cada entry
                    reader.read_unsigned_varint();
                }
            }
            // allow_auto_topic_creation
            reader.read_bool();
            // include_topic_authorized_operations (v8+)
            reader.read_bool();
            // tagged fields
            reader.read_unsigned_varint();
        } else {
            int32_t topics_count = reader.read_int32();
            if (topics_count < 0) {
                all_topics = true;
            } else {
                for (int32_t i = 0; i < topics_count; ++i) {
                    requested_topics.push_back(reader.read_string());
                }
            }
            // Auto-create topics (v4+)
            if (header.api_version >= 4 && reader.remaining() > 0) {
                try {
                    reader.read_bool();
                } catch (...) {}
            }
        }
    } catch (...) {
        all_topics = true;
    }
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    if (flexible) {
        // Tagged fields no header
        writer.write_unsigned_varint(0);
    }
    
    // Throttle time (v3+)
    if (header.api_version >= 3) {
        writer.write_int32(0);
    }
    
    // Brokers array
    if (flexible) {
        writer.write_unsigned_varint(2); // 1 broker + 1
        writer.write_int32(config_.broker_id);
        // Compact string for host
        writer.write_unsigned_varint(static_cast<uint32_t>(config_.host.size() + 1));
        for (char c : config_.host) {
            writer.write_int8(static_cast<int8_t>(c));
        }
        writer.write_int32(config_.port);
        // Rack (nullable compact string)
        writer.write_unsigned_varint(0); // null
        // Tagged fields
        writer.write_unsigned_varint(0);
    } else {
        writer.write_int32(1); // count
        writer.write_int32(config_.broker_id);
        writer.write_string(config_.host);
        writer.write_int32(config_.port);
        if (header.api_version >= 1) {
            writer.write_nullable_string(nullptr); // rack
        }
    }
    
    // Cluster ID (v2+)
    if (header.api_version >= 2) {
        if (flexible) {
            writer.write_unsigned_varint(static_cast<uint32_t>(config_.cluster_id.size() + 1));
            for (char c : config_.cluster_id) {
                writer.write_int8(static_cast<int8_t>(c));
            }
        } else {
            writer.write_nullable_string(&config_.cluster_id);
        }
    }
    
    // Controller ID (v1+)
    if (header.api_version >= 1) {
        writer.write_int32(config_.broker_id);
    }
    
    // Topics
    std::shared_lock<std::shared_mutex> lock(topics_mutex_);
    
    // Coletar tópicos com informações de partição
    // Para tópicos reais (do disco), temos partitions. Para tópicos criados recentemente,
    // usamos as informações do protocol_handler
    
    struct TopicMetadata {
        std::string name;
        int32_t num_partitions;
        const std::vector<std::unique_ptr<Partition>>* real_partitions; // nullptr se virtual
    };
    
    std::vector<TopicMetadata> topics_to_return;
    std::set<std::string> topic_names_added;
    
    // Primeiro, adiciona tópicos reais do disco
    if (all_topics || requested_topics.empty()) {
        for (const auto& [topic_name, partitions] : topics_) {
            topics_to_return.push_back({topic_name, static_cast<int32_t>(partitions.size()), &partitions});
            topic_names_added.insert(topic_name);
        }
    } else {
        for (const auto& req_topic : requested_topics) {
            auto it = topics_.find(req_topic);
            if (it != topics_.end()) {
                topics_to_return.push_back({req_topic, static_cast<int32_t>(it->second.size()), &it->second});
                topic_names_added.insert(req_topic);
            }
        }
    }
    
    // Também incluir tópicos criados via CreateTopics (armazenados no protocol_handler)
    auto stored_topics = protocol_handler_->get_stored_topics();
    for (const auto& t : stored_topics) {
        if (topic_names_added.find(t.name) == topic_names_added.end()) {
            // Tópico ainda não está na lista, adiciona como "virtual"
            if (all_topics || requested_topics.empty() ||
                std::find(requested_topics.begin(), requested_topics.end(), t.name) != requested_topics.end()) {
                topics_to_return.push_back({t.name, t.num_partitions, nullptr});
                topic_names_added.insert(t.name);
            }
        }
    }
    
    // Debug: mostrar tópicos sendo retornados
    if (!topics_to_return.empty()) {
        std::string topic_list;
        for (const auto& t : topics_to_return) {
            if (!topic_list.empty()) topic_list += ", ";
            topic_list += t.name;
        }
        LOG_TRACE("Metadata returning {} topics: {}", topics_to_return.size(), topic_list);
    }
    
    if (flexible) {
        writer.write_unsigned_varint(static_cast<uint32_t>(topics_to_return.size() + 1));
    } else {
        writer.write_int32(static_cast<int32_t>(topics_to_return.size()));
    }
    
    for (const auto& topic_meta : topics_to_return) {
        writer.write_int16(static_cast<int16_t>(ErrorCode::None));
        
        // Topic name
        if (flexible) {
            writer.write_unsigned_varint(static_cast<uint32_t>(topic_meta.name.size() + 1));
            for (char c : topic_meta.name) {
                writer.write_int8(static_cast<int8_t>(c));
            }
        } else {
            writer.write_string(topic_meta.name);
        }
        
        // Topic ID (v10+)
        if (header.api_version >= 10) {
            // Generate or retrieve topic UUID
            auto topic_uuid = const_cast<Broker*>(this)->get_or_create_topic_id(topic_meta.name);
            for (int i = 0; i < 16; i++) {
                writer.write_int8(static_cast<int8_t>(topic_uuid[i]));
            }
        }
        
        // Is internal (v1+)
        if (header.api_version >= 1) {
            writer.write_bool(false);
        }
        
        // Partitions - use real partitions or create virtual ones
        int32_t num_partitions = topic_meta.real_partitions ? 
            static_cast<int32_t>(topic_meta.real_partitions->size()) : topic_meta.num_partitions;
        
        if (flexible) {
            writer.write_unsigned_varint(static_cast<uint32_t>(num_partitions + 1));
        } else {
            writer.write_int32(num_partitions);
        }
        
        if (topic_meta.real_partitions) {
            // Partições reais
            for (const auto& partition : *topic_meta.real_partitions) {
                writer.write_int16(static_cast<int16_t>(ErrorCode::None));
                writer.write_int32(partition->get_partition_id());
                writer.write_int32(config_.broker_id); // leader
                
                // Leader epoch (v7+)
                if (header.api_version >= 7) {
                    writer.write_int32(0);
                }
                
                // Replicas
                if (flexible) {
                    writer.write_unsigned_varint(2); // 1 replica + 1
                } else {
                    writer.write_int32(1);
                }
                writer.write_int32(config_.broker_id);
                
                // ISR
                if (flexible) {
                    writer.write_unsigned_varint(2); // 1 isr + 1
                } else {
                    writer.write_int32(1);
                }
                writer.write_int32(config_.broker_id);
                
                // Offline replicas (v5+)
                if (header.api_version >= 5) {
                    if (flexible) {
                        writer.write_unsigned_varint(1); // 0 + 1
                    } else {
                        writer.write_int32(0);
                    }
                }
                
                // Tagged fields for partition (flexible)
                if (flexible) {
                    writer.write_unsigned_varint(0);
                }
            }
        } else {
            // Partições virtuais (tópico recém criado, ainda não materializado)
            for (int32_t p = 0; p < num_partitions; p++) {
                writer.write_int16(static_cast<int16_t>(ErrorCode::None));
                writer.write_int32(p); // partition id
                writer.write_int32(config_.broker_id); // leader
                
                // Leader epoch (v7+)
                if (header.api_version >= 7) {
                    writer.write_int32(0);
                }
                
                // Replicas
                if (flexible) {
                    writer.write_unsigned_varint(2);
                } else {
                    writer.write_int32(1);
                }
                writer.write_int32(config_.broker_id);
                
                // ISR
                if (flexible) {
                    writer.write_unsigned_varint(2);
                } else {
                    writer.write_int32(1);
                }
                writer.write_int32(config_.broker_id);
                
                // Offline replicas (v5+)
                if (header.api_version >= 5) {
                    if (flexible) {
                        writer.write_unsigned_varint(1);
                    } else {
                        writer.write_int32(0);
                    }
                }
                
                // Tagged fields for partition (flexible)
                if (flexible) {
                    writer.write_unsigned_varint(0);
                }
            }
        }
        
        // Topic authorized operations (v8+)
        if (header.api_version >= 8) {
            writer.write_int32(-2147483648); // not requested
        }
        
        // Tagged fields for topic (flexible)
        if (flexible) {
            writer.write_unsigned_varint(0);
        }
    }
    
    // Cluster authorized operations (v8+)
    if (header.api_version >= 8) {
        writer.write_int32(-2147483648);
    }
    
    // Tagged fields no final (flexible)
    if (flexible) {
        writer.write_unsigned_varint(0);
    }
    
    return writer.data();
}

// ============================================================================
// DeleteTopics Request Handler
// ============================================================================
std::vector<uint8_t> Broker::handle_delete_topics_request(
    const protocol::RequestHeader& header, protocol::BufferReader& reader) {
    
    using namespace protocol;
    
    // v4+ uses flexible format
    bool flexible = (header.api_version >= 4);
    
    std::vector<std::string> topic_names;
    
    if (flexible) {
        // Skip leading 0x00 byte if present (workaround)
        const uint8_t* ptr = reader.current();
        size_t rem = reader.remaining();
        if (rem >= 2 && ptr[0] == 0x00 && ptr[1] >= 0x01 && ptr[1] <= 0x20) {
            reader.skip(1);
        }
        
        // COMPACT_ARRAY of topics
        uint32_t topic_count = reader.read_unsigned_varint();
        if (topic_count > 0) {
            topic_count--; // compact array uses length + 1
            for (uint32_t i = 0; i < topic_count; ++i) {
                // Topic name (compact string)
                std::string topic_name = reader.read_compact_string();
                topic_names.push_back(topic_name);
                
                // Tagged fields for each topic entry
                reader.read_unsigned_varint();
            }
        }
        
        // timeout_ms
        reader.read_int32();
        
        // Tagged fields at end of request
        reader.read_unsigned_varint();
    } else {
        // Non-flexible format
        int32_t topic_count = reader.read_int32();
        for (int32_t i = 0; i < topic_count; ++i) {
            topic_names.push_back(reader.read_string());
        }
        
        // timeout
        reader.read_int32();
    }
    
    // Delete topics and their data
    std::vector<std::pair<std::string, ErrorCode>> results;
    for (const auto& name : topic_names) {
        try {
            delete_topic(name, true);
            results.push_back({name, ErrorCode::None});
            LOG_INFO("Deleted topic: {}", name);
        } catch (const std::exception& e) {
            LOG_WARN("Failed to delete topic {}: {}", name, e.what());
            results.push_back({name, ErrorCode::UnknownTopicOrPartition});
        }
    }
    
    // Also remove from protocol handler's stored_topics
    protocol_handler_->remove_stored_topics(topic_names);
    
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    if (flexible) {
        // Header tagged fields
        writer.write_unsigned_varint(0);
    }
    
    // Throttle time (v1+)
    if (header.api_version >= 1) {
        writer.write_int32(0);
    }
    
    // Topics response array
    if (flexible) {
        writer.write_unsigned_varint(static_cast<uint32_t>(results.size() + 1));
    } else {
        writer.write_int32(static_cast<int32_t>(results.size()));
    }
    
    for (const auto& [topic_name, error_code] : results) {
        if (flexible) {
            // Topic name (compact string)
            writer.write_unsigned_varint(static_cast<uint32_t>(topic_name.size() + 1));
            for (char c : topic_name) writer.write_int8(static_cast<int8_t>(c));
        } else {
            writer.write_string(topic_name);
        }
        
        // Topic ID (v6+) - 16 bytes UUID
        if (header.api_version >= 6) {
            for (int i = 0; i < 16; i++) writer.write_int8(0);
        }
        
        // Error code
        writer.write_int16(static_cast<int16_t>(error_code));
        
        // Error message (v5+, nullable compact string)
        if (header.api_version >= 5) {
            if (flexible) {
                writer.write_unsigned_varint(0); // null
            } else {
                writer.write_int16(-1); // null
            }
        }
        
        // Tagged fields for each topic (flexible)
        if (flexible) {
            writer.write_unsigned_varint(0);
        }
    }
    
    // Tagged fields at end (flexible)
    if (flexible) {
        writer.write_unsigned_varint(0);
    }
    
    return writer.data();
}

// ============================================================================
// DeleteRecords Request Handler
// ============================================================================
std::vector<uint8_t> Broker::handle_delete_records_request(
    const protocol::RequestHeader& header, protocol::BufferReader& reader) {
    
    using namespace protocol;
    
    // v1+ uses flexible format in Kafka 4.1
    bool flexible = (header.api_version >= 1);
    
    struct PartitionRequest {
        int32_t partition_id;
        int64_t offset;  // -1 means high watermark (delete all)
    };
    
    struct TopicRequest {
        std::string name;
        std::vector<PartitionRequest> partitions;
    };
    
    std::vector<TopicRequest> topics;
    
    // Parse request
    if (flexible) {
        // Skip leading 0x00 if present (workaround for some clients)
        const uint8_t* ptr = reader.current();
        if (reader.remaining() >= 2 && ptr[0] == 0x00 && ptr[1] >= 0x01 && ptr[1] <= 0x40) {
            reader.skip(1);
        }
        
        // COMPACT_ARRAY of topics
        uint32_t topic_count = reader.read_unsigned_varint();
        if (topic_count > 0) {
            topic_count--;  // compact array uses length + 1
            topics.reserve(topic_count);
            
            for (uint32_t i = 0; i < topic_count; ++i) {
                TopicRequest topic;
                topic.name = reader.read_compact_string();
                
                // COMPACT_ARRAY of partitions
                uint32_t partition_count = reader.read_unsigned_varint();
                if (partition_count > 0) {
                    partition_count--;
                    topic.partitions.reserve(partition_count);
                    
                    for (uint32_t j = 0; j < partition_count; ++j) {
                        PartitionRequest part;
                        part.partition_id = reader.read_int32();
                        part.offset = reader.read_int64();
                        
                        // Tagged fields per partition
                        reader.read_unsigned_varint();
                        
                        topic.partitions.push_back(part);
                    }
                }
                
                // Tagged fields per topic
                reader.read_unsigned_varint();
                
                topics.push_back(std::move(topic));
            }
        }
        
        // timeout_ms
        reader.read_int32();
        
        // Tagged fields at end
        reader.read_unsigned_varint();
    } else {
        // Non-flexible format (v0)
        int32_t topic_count = reader.read_int32();
        topics.reserve(topic_count);
        
        for (int32_t i = 0; i < topic_count; ++i) {
            TopicRequest topic;
            topic.name = reader.read_string();
            
            int32_t partition_count = reader.read_int32();
            topic.partitions.reserve(partition_count);
            
            for (int32_t j = 0; j < partition_count; ++j) {
                PartitionRequest part;
                part.partition_id = reader.read_int32();
                part.offset = reader.read_int64();
                topic.partitions.push_back(part);
            }
            
            topics.push_back(std::move(topic));
        }
        
        // timeout
        reader.read_int32();
    }
    
    // Process delete records
    struct PartitionResult {
        int32_t partition_id;
        int64_t low_watermark;
        ErrorCode error_code;
    };
    
    struct TopicResult {
        std::string name;
        std::vector<PartitionResult> partitions;
    };
    
    std::vector<TopicResult> results;
    
    for (const auto& topic_req : topics) {
        TopicResult topic_result;
        topic_result.name = topic_req.name;
        
        for (const auto& part_req : topic_req.partitions) {
            PartitionResult part_result;
            part_result.partition_id = part_req.partition_id;
            
            try {
                // offset -1 means delete to high watermark (all messages)
                int64_t target_offset = part_req.offset;
                if (target_offset == -1) {
                    target_offset = std::numeric_limits<int64_t>::max();
                }
                
                part_result.low_watermark = delete_records(topic_req.name, part_req.partition_id, target_offset);
                part_result.error_code = ErrorCode::None;
                LOG_INFO("Deleted records from topic {} partition {} before offset {}",
                         topic_req.name, part_req.partition_id, target_offset);
            } catch (const std::exception& e) {
                LOG_WARN("Failed to delete records from {} partition {}: {}", 
                         topic_req.name, part_req.partition_id, e.what());
                part_result.low_watermark = -1;
                part_result.error_code = ErrorCode::UnknownTopicOrPartition;
            }
            
            topic_result.partitions.push_back(part_result);
        }
        
        results.push_back(std::move(topic_result));
    }
    
    // Build response
    BufferWriter writer;
    writer.write_int32(header.correlation_id);
    
    if (flexible) {
        // Header tagged fields
        writer.write_unsigned_varint(0);
    }
    
    // Throttle time
    writer.write_int32(0);
    
    // Topics response array
    if (flexible) {
        writer.write_unsigned_varint(static_cast<uint32_t>(results.size() + 1));
    } else {
        writer.write_int32(static_cast<int32_t>(results.size()));
    }
    
    for (const auto& topic_result : results) {
        // Topic name
        if (flexible) {
            writer.write_unsigned_varint(static_cast<uint32_t>(topic_result.name.size() + 1));
            for (char c : topic_result.name) writer.write_int8(static_cast<int8_t>(c));
        } else {
            writer.write_string(topic_result.name);
        }
        
        // Partitions response array
        if (flexible) {
            writer.write_unsigned_varint(static_cast<uint32_t>(topic_result.partitions.size() + 1));
        } else {
            writer.write_int32(static_cast<int32_t>(topic_result.partitions.size()));
        }
        
        for (const auto& part_result : topic_result.partitions) {
            writer.write_int32(part_result.partition_id);
            writer.write_int64(part_result.low_watermark);
            writer.write_int16(static_cast<int16_t>(part_result.error_code));
            
            // Tagged fields per partition (flexible)
            if (flexible) {
                writer.write_unsigned_varint(0);
            }
        }
        
        // Tagged fields per topic (flexible)
        if (flexible) {
            writer.write_unsigned_varint(0);
        }
    }
    
    // Tagged fields at end (flexible)
    if (flexible) {
        writer.write_unsigned_varint(0);
    }
    
    return writer.data();
}

} // namespace eventhorizon
