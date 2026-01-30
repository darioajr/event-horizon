#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <mutex>
#include <cstdint>

/**
 * @brief Representa um registro/evento no log
 */
struct Record {
    int64_t offset = 0;
    int64_t timestamp = 0;
    std::string key;
    std::vector<uint8_t> value;
};

/**
 * @brief Segmento de log que armazena records em disco
 * 
 * Cada segmento consiste em:
 * - Arquivo .log: dados dos records
 * - Arquivo .index: índice offset -> posição no arquivo
 */
class LogSegment {
public:
    LogSegment(const std::string& path, int64_t base_offset);
    ~LogSegment() = default;
    
    // Não permitir cópia
    LogSegment(const LogSegment&) = delete;
    LogSegment& operator=(const LogSegment&) = delete;
    
    /**
     * @brief Adiciona um record ao segmento
     * @return Offset do record adicionado
     */
    int64_t append(const Record& record);
    
    /**
     * @brief Adiciona um RecordBatch bruto ao segmento (atualiza apenas o baseOffset)
     * @param batch_data Bytes do RecordBatch original
     * @param record_count Número de records no batch
     * @return Offset base atribuído ao batch
     */
    int64_t append_raw_batch(const std::vector<uint8_t>& batch_data, int32_t record_count);
    
    /**
     * @brief Lê records a partir de um offset
     */
    std::vector<Record> read(int64_t start_offset, size_t max_records);
    
    /**
     * @brief Lê bytes brutos do arquivo de log (formato Kafka nativo)
     * @param start_offset Offset inicial
     * @param max_bytes Número máximo de bytes para ler
     * @return Par de (bytes do log, número de records)
     */
    std::pair<std::vector<uint8_t>, int32_t> read_raw(int64_t start_offset, size_t max_bytes);
    
    /**
     * @brief Força escrita em disco
     */
    void flush();
    
    /**
     * @brief Retorna número de records no segmento
     */
    int64_t size() const;
    
    /**
     * @brief Retorna o tamanho do segmento em bytes
     */
    int64_t get_size_bytes() const;
    
    /**
     * @brief Retorna o offset base do segmento
     */
    int64_t get_base_offset() const;
    
    /**
     * @brief Retorna o próximo offset disponível
     */
    int64_t get_next_offset() const;
    
    /**
     * @brief Verifica se o segmento contém um offset específico
     */
    bool contains_offset(int64_t offset) const;
    
private:
    void recover_offset();
    std::vector<uint8_t> build_record_batch(const Record& record, int64_t offset);
    
    std::string path_;
    int64_t base_offset_;
    int64_t next_offset_;
    std::fstream data_file_;
    std::fstream index_file_;
    mutable std::mutex mutex_;
};