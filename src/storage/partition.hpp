#pragma once

#include "log_segment.hpp"
#include <memory>
#include <deque>
#include <string>
#include <span>
#include <shared_mutex>

/**
 * @brief Representa uma partição de um tópico
 * 
 * Cada partição mantém uma sequência ordenada de segments
 * e gerencia o ciclo de vida dos logs.
 */
class Partition {
public:
    Partition(const std::string& topic, int32_t partition_id, 
              const std::string& log_dir);
    ~Partition();
    
    // Não permitir cópia
    Partition(const Partition&) = delete;
    Partition& operator=(const Partition&) = delete;
    
    /**
     * @brief Produz uma mensagem na partição
     * @return Offset da mensagem produzida
     */
    int64_t produce(const std::string& key, 
                    const std::vector<uint8_t>& value,
                    int64_t timestamp = 0);
    
    /**
     * @brief Armazena um RecordBatch bruto na partição (mantém CRC original)
     * @param batch_data Bytes do RecordBatch original do cliente (C++23 span)
     * @param record_count Número de records no batch
     * @return Offset base atribuído ao batch
     */
    int64_t produce_raw_batch(std::span<const uint8_t> batch_data, int32_t record_count);
    
    /**
     * @brief Produz múltiplos batches de uma vez (batch coalescing)
     * 
     * Mais eficiente que múltiplas chamadas a produce_raw_batch pois
     * usa um único lock para todos os batches.
     * 
     * @param batches Vector de pares (data, record_count)
     * @return Base offset do primeiro batch
     */
    int64_t produce_raw_batches(std::span<const std::pair<std::span<const uint8_t>, int32_t>> batches);
    
    /**
     * @brief Busca mensagens a partir de um offset
     */
    std::vector<Record> fetch(int64_t offset, size_t max_bytes);
    
    /**
     * @brief Busca bytes brutos do log (formato Kafka nativo)
     * @return Par de (bytes do log, número de records)
     */
    std::pair<std::vector<uint8_t>, int32_t> fetch_raw(int64_t offset, size_t max_bytes);
    
    /**
     * @brief Retorna o offset do final do log (próximo offset disponível)
     */
    int64_t get_log_end_offset() const;
    
    /**
     * @brief Retorna o offset do início do log (primeiro offset disponível)
     */
    int64_t get_log_start_offset() const;
    
    /**
     * @brief Retorna o nome do tópico
     */
    std::string get_topic() const;
    
    /**
     * @brief Retorna o ID da partição
     */
    int32_t get_partition_id() const;
    
    /**
     * @brief Retorna o tamanho total da partição em bytes
     */
    int64_t get_size_bytes() const;
    
    /**
     * @brief Retorna o número de segments
     */
    int32_t get_segment_count() const;
    
    /**
     * @brief Força escrita de todos os segments em disco
     */
    void flush();
    
    /**
     * @brief Remove records antes de um offset (log compaction)
     * @return O novo log_start_offset após a operação
     */
    int64_t delete_records_before(int64_t offset);
    
    /**
     * @brief Trunca completamente a partição (remove todas as mensagens)
     * Usado pelo Kafka UI "Clear Messages"
     */
    void truncate();

private:
    void load_segments();
    void create_new_segment(int64_t base_offset);
    void roll_segment();
    bool should_roll_segment() const;
    LogSegment* find_segment_for_offset(int64_t offset);
    
    std::string topic_;
    int32_t partition_id_;
    std::string partition_dir_;
    std::deque<std::unique_ptr<LogSegment>> segments_;
    mutable std::mutex mutex_;
    
    int64_t log_start_offset_;
    int64_t log_end_offset_;
};