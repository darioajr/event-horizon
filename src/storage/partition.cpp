#include "partition.hpp"
#include "../logging/logger.hpp"
#include <filesystem>
#include <algorithm>

namespace fs = std::filesystem;

// ============================================================================
// Constantes
// ============================================================================
constexpr size_t MAX_RECORDS_PER_SEGMENT = 10000000;     // 10M records

// ============================================================================
// Partition Implementation
// ============================================================================

Partition::Partition(const std::string& topic, int32_t partition_id, 
                     const std::string& log_dir)
    : topic_(topic)
    , partition_id_(partition_id)
    , log_start_offset_(0)
    , log_end_offset_(0) {
    
    // Criar diretório da partição
    partition_dir_ = log_dir + "/" + topic + "-" + std::to_string(partition_id);
    
    if (!fs::exists(partition_dir_)) {
        fs::create_directories(partition_dir_);
        LOG_DEBUG("Created partition directory: {}", partition_dir_);
    }
    
    // Carregar segments existentes ou criar novo
    load_segments();
    
    if (segments_.empty()) {
        create_new_segment(0);
    }
}

Partition::~Partition() {
    flush();
}

void Partition::load_segments() {
    std::vector<int64_t> base_offsets;
    
    // Encontrar todos os arquivos .log no diretório
    for (const auto& entry : fs::directory_iterator(partition_dir_)) {
        if (entry.path().extension() == ".log") {
            std::string filename = entry.path().stem().string();
            try {
                int64_t base_offset = std::stoll(filename);
                base_offsets.push_back(base_offset);
            } catch (...) {
                LOG_WARN("Invalid segment file: {}", filename);
            }
        }
    }
    
    // Ordenar e carregar segments
    std::sort(base_offsets.begin(), base_offsets.end());
    
    for (int64_t base_offset : base_offsets) {
        auto segment = std::make_unique<LogSegment>(partition_dir_, base_offset);
        
        // Atualizar offsets
        if (segments_.empty()) {
            log_start_offset_ = segment->get_base_offset();
        }
        log_end_offset_ = segment->get_next_offset();
        
        segments_.push_back(std::move(segment));
    }
    
    if (!segments_.empty()) {
        LOG_DEBUG("Loaded {} segments for {}-{} (offsets {} to {})",
                  segments_.size(), topic_, partition_id_, log_start_offset_, log_end_offset_);
    }
}

void Partition::create_new_segment(int64_t base_offset) {
    auto segment = std::make_unique<LogSegment>(partition_dir_, base_offset);
    segments_.push_back(std::move(segment));
    
    LOG_DEBUG("Created new segment with base_offset={} for {}-{}",
              base_offset, topic_, partition_id_);
}

int64_t Partition::produce(const std::string& key, 
                           const std::vector<uint8_t>& value,
                           int64_t timestamp) {
    std::lock_guard lock(mutex_);  // C++17 CTAD
    
    // Verificar se precisa criar novo segment
    if (should_roll_segment()) {
        roll_segment();
    }
    
    // Usar segment ativo (último)
    LogSegment* active_segment = segments_.back().get();
    
    Record record;
    record.offset = log_end_offset_;
    record.timestamp = timestamp; // Use provided timestamp
    record.key = key;
    record.value = value;
    
    int64_t offset = active_segment->append(record);
    log_end_offset_ = offset + 1;
    
    return offset;
}

int64_t Partition::produce_raw_batch(std::span<const uint8_t> batch_data, int32_t record_count) {
    std::lock_guard lock(mutex_);  // C++17 CTAD
    
    // Verificar se precisa criar novo segment
    if (should_roll_segment()) {
        roll_segment();
    }
    
    // Usar segment ativo (último)
    LogSegment* active_segment = segments_.back().get();
    
    int64_t offset = active_segment->append_raw_batch(batch_data, record_count);
    log_end_offset_ = offset + record_count;
    
    return offset;
}

int64_t Partition::produce_raw_batches(std::span<const std::pair<std::span<const uint8_t>, int32_t>> batches) {
    if (batches.empty()) {
        return log_end_offset_;
    }
    
    std::lock_guard lock(mutex_);  // C++17 CTAD - single lock for all batches
    
    // Verificar se precisa criar novo segment
    if (should_roll_segment()) {
        roll_segment();
    }
    
    // Usar segment ativo (último)
    LogSegment* active_segment = segments_.back().get();
    
    int64_t base_offset = -1;
    
    // Process all batches with single lock
    for (const auto& [batch_data, record_count] : batches) {
        int64_t offset = active_segment->append_raw_batch(batch_data, record_count);
        if (base_offset < 0) {
            base_offset = offset;
        }
        log_end_offset_ = offset + record_count;
        
        // Check if we need to roll segment mid-batch
        if (should_roll_segment()) {
            roll_segment();
            active_segment = segments_.back().get();
        }
    }
    
    return base_offset;
}

std::vector<Record> Partition::fetch(int64_t offset, size_t max_bytes) {
    std::lock_guard lock(mutex_);  // C++17 CTAD
    
    std::vector<Record> results;
    size_t total_bytes = 0;
    
    if (offset < log_start_offset_ || offset >= log_end_offset_) {
        return results;
    }
    
    // Encontrar segment que contém o offset
    LogSegment* segment = find_segment_for_offset(offset);
    if (!segment) {
        return results;
    }
    
    // Ler records
    size_t max_records = max_bytes / 100; // Estimativa conservadora
    if (max_records == 0) max_records = 1;
    
    auto records = segment->read(offset, max_records);
    
    for (auto& record : records) {
        size_t record_size = record.key.size() + record.value.size() + 28;
        if (total_bytes + record_size > max_bytes && !results.empty()) {
            break;
        }
        total_bytes += record_size;
        results.push_back(std::move(record));
    }
    
    return results;
}

std::pair<std::vector<uint8_t>, int32_t> Partition::fetch_raw(int64_t offset, size_t max_bytes) {
    std::lock_guard lock(mutex_);  // C++17 CTAD
    
    if (offset < log_start_offset_ || offset >= log_end_offset_) {
        return {{}, 0};
    }
    
    // Find segment containing the offset
    LogSegment* segment = find_segment_for_offset(offset);
    if (!segment) {
        return {{}, 0};
    }
    
    // Read raw bytes directly from segment
    return segment->read_raw(offset, max_bytes);
}

LogSegment* Partition::find_segment_for_offset(int64_t offset) {
    for (auto& segment : segments_) {
        if (segment->contains_offset(offset)) {
            return segment.get();
        }
    }
    return nullptr;
}

bool Partition::should_roll_segment() const {
    if (segments_.empty()) {
        return true;
    }
    
    const auto& active = segments_.back();
    return static_cast<int64_t>(active->size()) >= static_cast<int64_t>(MAX_RECORDS_PER_SEGMENT);
}

void Partition::roll_segment() {
    if (!segments_.empty()) {
        segments_.back()->flush();
    }
    
    create_new_segment(log_end_offset_);
}

int64_t Partition::get_log_end_offset() const {
    return log_end_offset_;
}

int64_t Partition::get_log_start_offset() const {
    return log_start_offset_;
}

std::string Partition::get_topic() const {
    return topic_;
}

int32_t Partition::get_partition_id() const {
    return partition_id_;
}

int64_t Partition::get_size_bytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    int64_t total = 0;
    for (const auto& segment : segments_) {
        total += segment->get_size_bytes();
    }
    return total;
}

int32_t Partition::get_segment_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return static_cast<int32_t>(segments_.size());
}

void Partition::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    for (auto& segment : segments_) {
        segment->flush();
    }
}

int64_t Partition::delete_records_before(int64_t offset) {
    std::lock_guard lock(mutex_);  // C++23 CTAD
    
    // Se offset é -1 (high watermark), usar log_end_offset
    int64_t target_offset = (offset == -1) ? log_end_offset_ : offset;
    
    // Remover segments antigos cujos records estão todos antes do offset
    while (segments_.size() > 1) {
        auto& oldest = segments_.front();
        if (oldest->get_next_offset() <= target_offset) {
            LOG_DEBUG("Deleting old segment with base_offset={}", oldest->get_base_offset());
            segments_.pop_front();
        } else {
            break;
        }
    }
    
    // Atualizar log_start_offset
    if (!segments_.empty()) {
        // Se todos os records foram deletados, mover start para target
        if (segments_.front()->get_base_offset() < target_offset) {
            log_start_offset_ = target_offset;
        } else {
            log_start_offset_ = segments_.front()->get_base_offset();
        }
    }
    
    return log_start_offset_;
}

void Partition::truncate() {
    std::lock_guard lock(mutex_);  // C++23 CTAD
    
    LOG_INFO("Truncating partition {}-{}", topic_, partition_id_);
    
    // Remover todos os segments
    segments_.clear();
    
    // Remover arquivos físicos
    namespace fs = std::filesystem;
    for (const auto& entry : fs::directory_iterator(partition_dir_)) {
        if (entry.is_regular_file()) {
            fs::remove(entry.path());
        }
    }
    
    // Criar novo segment vazio com base_offset = log_end_offset atual
    // Isso preserva a continuidade do offset
    int64_t new_base = log_end_offset_;
    log_start_offset_ = new_base;
    
    create_new_segment(new_base);
    
    LOG_DEBUG("Partition truncated, new base_offset={}", new_base);
}
