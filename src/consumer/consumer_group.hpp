#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <map>
#include <unordered_map>
#include <set>
#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <memory>
#include <functional>
#include <random>
#include <optional>
#include <expected>
#include <format>
#include <ranges>
#include <span>

namespace eventhorizon {

// ============================================================================
// Consumer Group Types
// ============================================================================

/**
 * @brief Estado do consumer group
 */
enum class GroupState {
    Empty,              // Nenhum membro ativo
    PreparingRebalance, // Aguardando todos membros entrarem
    CompletingRebalance,// Aguardando SyncGroup de todos
    Stable,             // Grupo estável, consumindo normalmente
    Dead                // Grupo será removido
};

inline std::string group_state_to_string(GroupState state) {
    switch (state) {
        case GroupState::Empty: return "Empty";
        case GroupState::PreparingRebalance: return "PreparingRebalance";
        case GroupState::CompletingRebalance: return "CompletingRebalance";
        case GroupState::Stable: return "Stable";
        case GroupState::Dead: return "Dead";
    }
    return "Unknown";
}

/**
 * @brief Informações de uma partição assinada
 */
struct TopicPartition {
    std::string topic;
    int32_t partition;
    
    auto operator<=>(const TopicPartition&) const = default;
};

/**
 * @brief Offset commitado para uma partição
 */
struct CommittedOffset {
    int64_t offset;
    std::string metadata;
    int64_t commit_timestamp;
};

/**
 * @brief Informações de um membro do grupo
 */
struct MemberInfo {
    std::string member_id;
    std::string client_id;
    std::string client_host;
    std::optional<std::string> group_instance_id;  // Para static membership
    
    int32_t session_timeout_ms{10000};
    int32_t rebalance_timeout_ms{60000};
    
    std::vector<uint8_t> protocol_metadata;        // Subscription/metadata do cliente
    std::vector<uint8_t> assignment;               // Assignment atual (set pelo SyncGroup)
    
    std::chrono::steady_clock::time_point last_heartbeat{std::chrono::steady_clock::now()};
    
    // Protocolos suportados pelo membro (protocol_name -> metadata)
    std::vector<std::pair<std::string, std::vector<uint8_t>>> supported_protocols;
    
    [[nodiscard]] bool is_session_expired() const {
        auto elapsed = std::chrono::steady_clock::now() - last_heartbeat;
        return elapsed > std::chrono::milliseconds{session_timeout_ms};
    }
    
    void touch_heartbeat() {
        last_heartbeat = std::chrono::steady_clock::now();
    }
};

/**
 * @brief Consumer Group completo
 */
class ConsumerGroup {
public:
    explicit ConsumerGroup(std::string_view group_id);
    
    // Identificação
    [[nodiscard]] std::string_view group_id() const { return group_id_; }
    [[nodiscard]] int32_t generation_id() const { return generation_id_; }
    [[nodiscard]] GroupState state() const { return state_; }
    [[nodiscard]] std::string_view state_string() const { return group_state_to_string(state_); }
    [[nodiscard]] std::string_view protocol_type() const { return protocol_type_; }
    [[nodiscard]] std::string_view protocol_name() const { return protocol_name_; }
    
    // Leader
    [[nodiscard]] std::string_view leader_id() const { return leader_id_; }
    [[nodiscard]] bool is_leader(std::string_view member_id) const { return member_id == leader_id_; }
    
    // Members
    [[nodiscard]] bool has_member(std::string_view member_id) const;
    [[nodiscard]] MemberInfo* get_member(std::string_view member_id);
    [[nodiscard]] const MemberInfo* get_member(std::string_view member_id) const;
    [[nodiscard]] std::vector<std::string> member_ids() const;
    [[nodiscard]] size_t member_count() const { return members_.size(); }
    [[nodiscard]] bool is_empty() const { return members_.empty(); }
    
    // Join/Leave
    struct JoinResult {
        int16_t error_code{0};
        std::string member_id;
        int32_t generation_id{0};
        std::string leader_id;
        std::string protocol_name;
        std::vector<MemberInfo*> members;  // Só retornado para o leader
    };
    
    [[nodiscard]] JoinResult join(
        std::string_view member_id,
        std::optional<std::string_view> group_instance_id,
        std::string_view client_id,
        std::string_view client_host,
        std::string_view protocol_type,
        std::span<const std::pair<std::string, std::vector<uint8_t>>> protocols,
        int32_t session_timeout_ms,
        int32_t rebalance_timeout_ms
    );
    
    void leave(std::string_view member_id);
    void leave_by_instance_id(std::string_view group_instance_id);
    
    // Sync
    struct SyncResult {
        int16_t error_code{0};
        std::vector<uint8_t> assignment;
    };
    
    [[nodiscard]] SyncResult sync(
        std::string_view member_id,
        int32_t generation_id,
        std::span<const std::pair<std::string, std::vector<uint8_t>>> assignments
    );
    
    // Heartbeat
    [[nodiscard]] int16_t heartbeat(std::string_view member_id, int32_t generation_id);
    
    // Offsets
    void commit_offset(
        std::string_view topic, 
        int32_t partition,
        int64_t offset,
        std::string_view metadata = ""
    );
    
    [[nodiscard]] std::optional<CommittedOffset> fetch_offset(
        std::string_view topic,
        int32_t partition
    ) const;
    
    [[nodiscard]] std::map<TopicPartition, CommittedOffset> all_offsets() const;
    
    // Expiration
    [[nodiscard]] std::vector<std::string> expire_sessions();
    
    // Serialização para ListGroups/DescribeGroups
    struct GroupDescription {
        std::string group_id;
        std::string protocol_type;
        std::string state;
        std::string protocol_name;
        std::vector<MemberInfo> members;
    };
    
    [[nodiscard]] GroupDescription describe() const;

private:
    [[nodiscard]] std::string generate_member_id(std::string_view client_id);
    [[nodiscard]] std::string select_protocol();
    void transition_to(GroupState new_state);
    void maybe_elect_new_leader();
    void reset_generation();
    
    std::string group_id_;
    int32_t generation_id_{0};
    GroupState state_{GroupState::Empty};
    
    std::string protocol_type_;  // "consumer" para consumer groups padrão
    std::string protocol_name_;  // "range", "roundrobin", etc.
    
    std::string leader_id_;
    std::map<std::string, MemberInfo, std::less<>> members_;  // transparent comparator
    
    // Static membership: group_instance_id -> member_id
    std::map<std::string, std::string, std::less<>> static_members_;
    
    // Offsets commitados
    std::map<TopicPartition, CommittedOffset> committed_offsets_;
    
    // Tracking para SyncGroup
    std::set<std::string, std::less<>> members_awaiting_sync_;
    std::map<std::string, std::vector<uint8_t>, std::less<>> pending_assignments_;
    
    // Random para geração de IDs
    mutable std::mt19937 rng_{std::random_device{}()};
    
    mutable std::shared_mutex mutex_;
};

// ============================================================================
// Consumer Group Manager
// ============================================================================

/**
 * @brief Gerencia todos os consumer groups do broker
 */
class ConsumerGroupManager {
public:
    ConsumerGroupManager() = default;
    ~ConsumerGroupManager() = default;
    
    // Não permitir cópia
    ConsumerGroupManager(const ConsumerGroupManager&) = delete;
    ConsumerGroupManager& operator=(const ConsumerGroupManager&) = delete;
    
    /**
     * @brief Obtém ou cria um consumer group
     */
    [[nodiscard]] ConsumerGroup* get_or_create_group(std::string_view group_id);
    
    /**
     * @brief Obtém um grupo existente (nullptr se não existir)
     */
    [[nodiscard]] ConsumerGroup* get_group(std::string_view group_id);
    
    /**
     * @brief Remove um grupo
     */
    void delete_group(std::string_view group_id);
    
    /**
     * @brief Lista todos os grupos
     */
    [[nodiscard]] std::vector<ConsumerGroup::GroupDescription> list_groups() const;
    
    /**
     * @brief Descreve grupos específicos
     */
    [[nodiscard]] std::vector<ConsumerGroup::GroupDescription> describe_groups(
        std::span<const std::string> group_ids) const;
    
    /**
     * @brief Executa limpeza de sessões expiradas (chamar periodicamente)
     */
    void expire_sessions();
    
    /**
     * @brief Remove grupos vazios há mais de X tempo
     */
    void cleanup_empty_groups(std::chrono::seconds max_empty_time = std::chrono::seconds{300});
    
private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::unique_ptr<ConsumerGroup>> groups_;
    
    // Track quando grupos ficaram vazios
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> empty_group_times_;
};

} // namespace eventhorizon
