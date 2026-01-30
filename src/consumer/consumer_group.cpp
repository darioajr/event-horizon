#include "consumer_group.hpp"
#include <algorithm>
#include <chrono>
#include <print>

namespace eventhorizon {

// ============================================================================
// ConsumerGroup Implementation
// ============================================================================

ConsumerGroup::ConsumerGroup(std::string_view group_id)
    : group_id_(group_id) {}

bool ConsumerGroup::has_member(std::string_view member_id) const {
    std::shared_lock lock(mutex_);
    return members_.contains(std::string(member_id));
}

MemberInfo* ConsumerGroup::get_member(std::string_view member_id) {
    std::shared_lock lock(mutex_);
    if (auto it = members_.find(std::string(member_id)); it != members_.end()) {
        return &it->second;
    }
    return nullptr;
}

const MemberInfo* ConsumerGroup::get_member(std::string_view member_id) const {
    std::shared_lock lock(mutex_);
    if (auto it = members_.find(std::string(member_id)); it != members_.end()) {
        return &it->second;
    }
    return nullptr;
}

std::vector<std::string> ConsumerGroup::member_ids() const {
    std::shared_lock lock(mutex_);
    std::vector<std::string> ids;
    ids.reserve(members_.size());
    for (const auto& [id, _] : members_) {
        ids.push_back(id);
    }
    return ids;
}

std::string ConsumerGroup::generate_member_id(std::string_view client_id) {
    std::uniform_int_distribution<uint64_t> dist;
    return std::format("{}-{:016x}", client_id, dist(rng_));
}

std::string ConsumerGroup::select_protocol() {
    if (members_.empty()) {
        return "";
    }
    
    // Encontrar protocolo suportado por todos os membros
    // Usa o primeiro protocolo do primeiro membro como candidato
    const auto& first_member = members_.begin()->second;
    
    for (const auto& [protocol_name, proto_data] : first_member.supported_protocols) {
        bool supported_by_all = true;
        (void)proto_data; // unused
        
        for (const auto& [member_id, member] : members_) {
            (void)member_id; // unused
            bool has_protocol = std::ranges::any_of(
                member.supported_protocols,
                [&](const auto& p) { return p.first == protocol_name; }
            );
            if (!has_protocol) {
                supported_by_all = false;
                break;
            }
        }
        
        if (supported_by_all) {
            return protocol_name;
        }
    }
    
    return "";
}

void ConsumerGroup::transition_to(GroupState new_state) {
    if (state_ != new_state) {
        std::println("  Group {} state: {} -> {}", 
            group_id_, group_state_to_string(state_), group_state_to_string(new_state));
        state_ = new_state;
    }
}

void ConsumerGroup::maybe_elect_new_leader() {
    if (members_.empty()) {
        leader_id_.clear();
        return;
    }
    
    // Se o leader ainda está no grupo, mantém
    if (!leader_id_.empty() && members_.contains(leader_id_)) {
        return;
    }
    
    // Elege o primeiro membro (ordem alfabética do member_id)
    leader_id_ = members_.begin()->first;
    std::println("  Group {} elected new leader: {}", group_id_, leader_id_);
}

void ConsumerGroup::reset_generation() {
    ++generation_id_;
    members_awaiting_sync_.clear();
    pending_assignments_.clear();
    
    // Limpa assignments de todos os membros
    for (auto& [_, member] : members_) {
        member.assignment.clear();
    }
}

ConsumerGroup::JoinResult ConsumerGroup::join(
    std::string_view member_id,
    std::optional<std::string_view> group_instance_id,
    std::string_view client_id,
    std::string_view client_host,
    std::string_view protocol_type,
    std::span<const std::pair<std::string, std::vector<uint8_t>>> protocols,
    int32_t session_timeout_ms,
    int32_t rebalance_timeout_ms
) {
    std::unique_lock lock(mutex_);
    
    JoinResult result{};
    
    std::println("  JoinGroup: group={}, member_id={}, client_id={}, protocols={}",
        group_id_, member_id, client_id, protocols.size());
    
    // Validações
    if (protocols.empty()) {
        result.error_code = 23; // INCONSISTENT_GROUP_PROTOCOL
        return result;
    }
    
    // Verificar protocol_type
    if (!protocol_type_.empty() && protocol_type_ != protocol_type) {
        result.error_code = 23; // INCONSISTENT_GROUP_PROTOCOL
        return result;
    }
    
    // Gerar member_id se necessário
    std::string actual_member_id;
    if (member_id.empty()) {
        actual_member_id = generate_member_id(client_id);
        std::println("    Generated member_id: {}", actual_member_id);
    } else {
        actual_member_id = std::string(member_id);
    }
    
    // Static membership: verificar group_instance_id
    if (group_instance_id.has_value()) {
        std::string instance_id{*group_instance_id};
        if (auto it = static_members_.find(instance_id); it != static_members_.end()) {
            // Rejoin do mesmo static member
            actual_member_id = it->second;
            std::println("    Static member rejoin: {} -> {}", instance_id, actual_member_id);
        } else {
            static_members_[instance_id] = actual_member_id;
        }
    }
    
    // Criar ou atualizar membro
    MemberInfo& member = members_[actual_member_id];
    member.member_id = actual_member_id;
    member.client_id = std::string(client_id);
    member.client_host = std::string(client_host);
    member.session_timeout_ms = session_timeout_ms;
    member.rebalance_timeout_ms = rebalance_timeout_ms;
    member.touch_heartbeat();
    
    if (group_instance_id.has_value()) {
        member.group_instance_id = std::string(*group_instance_id);
    }
    
    // Copiar protocolos suportados
    member.supported_protocols.clear();
    for (const auto& p : protocols) {
        member.supported_protocols.push_back(p);
    }
    
    // Definir protocol_type do grupo
    if (protocol_type_.empty()) {
        protocol_type_ = std::string(protocol_type);
    }
    
    // Transição de estado
    bool needs_rebalance = (state_ == GroupState::Stable);
    
    if (state_ == GroupState::Empty || state_ == GroupState::Dead) {
        transition_to(GroupState::PreparingRebalance);
    }
    
    if (needs_rebalance || state_ == GroupState::PreparingRebalance) {
        reset_generation();
        transition_to(GroupState::CompletingRebalance);
    }
    
    // Eleger líder
    maybe_elect_new_leader();
    
    // Selecionar protocolo
    protocol_name_ = select_protocol();
    
    // Add member to awaiting sync
    members_awaiting_sync_.insert(actual_member_id);
    
    // Preencher resultado
    result.error_code = 0;
    result.member_id = actual_member_id;
    result.generation_id = generation_id_;
    result.leader_id = leader_id_;
    result.protocol_name = protocol_name_;
    
    // Se é o líder, retorna todos os membros
    if (actual_member_id == leader_id_) {
        for (auto& [id, m] : members_) {
            result.members.push_back(&m);
        }
    }
    
    std::println("    JoinGroup result: gen={}, leader={}, is_leader={}, members={}",
        result.generation_id, result.leader_id, 
        actual_member_id == leader_id_, result.members.size());
    
    return result;
}

void ConsumerGroup::leave(std::string_view member_id) {
    std::unique_lock lock(mutex_);
    
    std::string id{member_id};
    if (auto it = members_.find(id); it != members_.end()) {
        // Remover static membership se aplicável
        if (it->second.group_instance_id.has_value()) {
            static_members_.erase(*it->second.group_instance_id);
        }
        
        members_.erase(it);
        members_awaiting_sync_.erase(id);
        pending_assignments_.erase(id);
        
        std::println("  Member {} left group {}", member_id, group_id_);
        
        if (members_.empty()) {
            transition_to(GroupState::Empty);
            leader_id_.clear();
        } else {
            maybe_elect_new_leader();
            // Trigger rebalance
            reset_generation();
            transition_to(GroupState::PreparingRebalance);
        }
    }
}

void ConsumerGroup::leave_by_instance_id(std::string_view group_instance_id) {
    std::unique_lock lock(mutex_);
    
    std::string instance_id{group_instance_id};
    if (auto it = static_members_.find(instance_id); it != static_members_.end()) {
        std::string member_id = it->second;
        static_members_.erase(it);
        
        if (auto mit = members_.find(member_id); mit != members_.end()) {
            members_.erase(mit);
            members_awaiting_sync_.erase(member_id);
            pending_assignments_.erase(member_id);
        }
        
        if (members_.empty()) {
            transition_to(GroupState::Empty);
        }
    }
}

ConsumerGroup::SyncResult ConsumerGroup::sync(
    std::string_view member_id,
    int32_t gen_id,
    std::span<const std::pair<std::string, std::vector<uint8_t>>> assignments
) {
    std::unique_lock lock(mutex_);
    
    SyncResult result{};
    std::string id{member_id};
    
    std::println("  SyncGroup: group={}, member={}, gen={}, assignments={}",
        group_id_, member_id, gen_id, assignments.size());
    
    // Verificar se membro existe
    auto it = members_.find(id);
    if (it == members_.end()) {
        result.error_code = 25; // UNKNOWN_MEMBER_ID
        return result;
    }
    
    // Verificar generation
    if (gen_id != generation_id_) {
        result.error_code = 22; // ILLEGAL_GENERATION
        return result;
    }
    
    // Verificar estado
    if (state_ != GroupState::CompletingRebalance && state_ != GroupState::Stable) {
        result.error_code = 27; // REBALANCE_IN_PROGRESS
        return result;
    }
    
    // Se é o líder, armazenar assignments
    if (id == leader_id_ && !assignments.empty()) {
        for (const auto& [mid, assignment] : assignments) {
            pending_assignments_[mid] = assignment;
            std::println("    Stored assignment for {}: {} bytes", mid, assignment.size());
        }
    }
    
    // Marcar membro como sincronizado
    members_awaiting_sync_.erase(id);
    
    // Verificar se todos sincronizaram
    if (members_awaiting_sync_.empty()) {
        // Aplicar assignments a todos os membros
        for (auto& [mid, member] : members_) {
            if (auto ait = pending_assignments_.find(mid); ait != pending_assignments_.end()) {
                member.assignment = ait->second;
            }
        }
        pending_assignments_.clear();
        transition_to(GroupState::Stable);
        std::println("    Group {} is now Stable", group_id_);
    }
    
    // Retornar assignment deste membro
    result.error_code = 0;
    result.assignment = it->second.assignment;
    
    // Se ainda não temos assignment (esperando outros), retornar do pending
    if (result.assignment.empty()) {
        if (auto ait = pending_assignments_.find(id); ait != pending_assignments_.end()) {
            result.assignment = ait->second;
        }
    }
    
    return result;
}

int16_t ConsumerGroup::heartbeat(std::string_view member_id, int32_t gen_id) {
    std::unique_lock lock(mutex_);
    
    std::string id{member_id};
    
    // Verificar se membro existe
    auto it = members_.find(id);
    if (it == members_.end()) {
        return 25; // UNKNOWN_MEMBER_ID
    }
    
    // Verificar generation
    if (gen_id != generation_id_) {
        return 22; // ILLEGAL_GENERATION
    }
    
    // Verificar se rebalance em progresso
    if (state_ == GroupState::PreparingRebalance || 
        state_ == GroupState::CompletingRebalance) {
        return 27; // REBALANCE_IN_PROGRESS
    }
    
    // Atualizar heartbeat
    it->second.touch_heartbeat();
    
    return 0; // None
}

void ConsumerGroup::commit_offset(
    std::string_view topic,
    int32_t partition,
    int64_t offset,
    std::string_view metadata
) {
    std::unique_lock lock(mutex_);
    
    TopicPartition tp{std::string(topic), partition};
    committed_offsets_[tp] = CommittedOffset{
        .offset = offset,
        .metadata = std::string(metadata),
        .commit_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count()
    };
    
    std::println("  Committed offset: group={}, topic={}, partition={}, offset={}",
        group_id_, topic, partition, offset);
}

std::optional<CommittedOffset> ConsumerGroup::fetch_offset(
    std::string_view topic,
    int32_t partition
) const {
    std::shared_lock lock(mutex_);
    
    TopicPartition tp{std::string(topic), partition};
    if (auto it = committed_offsets_.find(tp); it != committed_offsets_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::map<TopicPartition, CommittedOffset> ConsumerGroup::all_offsets() const {
    std::shared_lock lock(mutex_);
    return committed_offsets_;
}

std::vector<std::string> ConsumerGroup::expire_sessions() {
    std::unique_lock lock(mutex_);
    
    std::vector<std::string> expired;
    
    for (auto it = members_.begin(); it != members_.end(); ) {
        if (it->second.is_session_expired()) {
            std::println("  Session expired: group={}, member={}", group_id_, it->first);
            expired.push_back(it->first);
            
            if (it->second.group_instance_id.has_value()) {
                static_members_.erase(*it->second.group_instance_id);
            }
            
            it = members_.erase(it);
        } else {
            ++it;
        }
    }
    
    if (!expired.empty()) {
        if (members_.empty()) {
            transition_to(GroupState::Empty);
        } else {
            maybe_elect_new_leader();
            reset_generation();
            transition_to(GroupState::PreparingRebalance);
        }
    }
    
    return expired;
}

ConsumerGroup::GroupDescription ConsumerGroup::describe() const {
    std::shared_lock lock(mutex_);
    
    GroupDescription desc{
        .group_id = group_id_,
        .protocol_type = protocol_type_,
        .state = group_state_to_string(state_),
        .protocol_name = protocol_name_
    };
    
    for (const auto& [_, member] : members_) {
        desc.members.push_back(member);
    }
    
    return desc;
}

// ============================================================================
// ConsumerGroupManager Implementation
// ============================================================================

ConsumerGroup* ConsumerGroupManager::get_or_create_group(std::string_view group_id) {
    std::unique_lock lock(mutex_);
    
    std::string id{group_id};
    if (auto it = groups_.find(id); it != groups_.end()) {
        return it->second.get();
    }
    
    auto group = std::make_unique<ConsumerGroup>(group_id);
    auto* ptr = group.get();
    groups_[id] = std::move(group);
    
    std::println("  Created consumer group: {}", group_id);
    return ptr;
}

ConsumerGroup* ConsumerGroupManager::get_group(std::string_view group_id) {
    std::shared_lock lock(mutex_);
    
    std::string id{group_id};
    if (auto it = groups_.find(id); it != groups_.end()) {
        return it->second.get();
    }
    return nullptr;
}

void ConsumerGroupManager::delete_group(std::string_view group_id) {
    std::unique_lock lock(mutex_);
    
    std::string id{group_id};
    groups_.erase(id);
    empty_group_times_.erase(id);
    
    std::println("  Deleted consumer group: {}", group_id);
}

std::vector<ConsumerGroup::GroupDescription> ConsumerGroupManager::list_groups() const {
    std::shared_lock lock(mutex_);
    
    std::vector<ConsumerGroup::GroupDescription> result;
    result.reserve(groups_.size());
    
    for (const auto& [_, group] : groups_) {
        result.push_back(group->describe());
    }
    
    return result;
}

std::vector<ConsumerGroup::GroupDescription> ConsumerGroupManager::describe_groups(
    std::span<const std::string> group_ids) const {
    std::shared_lock lock(mutex_);
    
    std::vector<ConsumerGroup::GroupDescription> result;
    result.reserve(group_ids.size());
    
    for (const auto& gid : group_ids) {
        if (auto it = groups_.find(gid); it != groups_.end()) {
            result.push_back(it->second->describe());
        } else {
            // Grupo não encontrado - retorna descrição vazia com estado Dead
            result.push_back(ConsumerGroup::GroupDescription{
                .group_id = gid,
                .protocol_type = "",
                .state = "Dead",
                .protocol_name = ""
            });
        }
    }
    
    return result;
}

void ConsumerGroupManager::expire_sessions() {
    std::shared_lock lock(mutex_);
    
    for (auto& [_, group] : groups_) {
        [[maybe_unused]] auto expired = group->expire_sessions();
    }
}

void ConsumerGroupManager::cleanup_empty_groups(std::chrono::seconds max_empty_time) {
    std::unique_lock lock(mutex_);
    
    auto now = std::chrono::steady_clock::now();
    
    // Primeiro, atualizar tracking de grupos vazios
    for (auto& [id, group] : groups_) {
        if (group->is_empty()) {
            if (!empty_group_times_.contains(id)) {
                empty_group_times_[id] = now;
            }
        } else {
            empty_group_times_.erase(id);
        }
    }
    
    // Remover grupos vazios há muito tempo
    std::vector<std::string> to_remove;
    for (const auto& [id, empty_since] : empty_group_times_) {
        if (now - empty_since > max_empty_time) {
            to_remove.push_back(id);
        }
    }
    
    for (const auto& id : to_remove) {
        std::println("  Cleaning up empty group: {}", id);
        groups_.erase(id);
        empty_group_times_.erase(id);
    }
}

} // namespace eventhorizon
