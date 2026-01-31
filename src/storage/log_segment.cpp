#include "log_segment.hpp"
#include "../logging/logger.hpp"
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <cstring>
#include <chrono>

namespace fs = std::filesystem;

// ============================================================================
// Kafka Format Constants
// ============================================================================

// Kafka index entry: 4 bytes relative offset + 4 bytes position
constexpr size_t KAFKA_INDEX_ENTRY_SIZE = 8;

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
// Helper functions for Kafka binary format (big-endian)
// ============================================================================

static void write_int8(std::vector<uint8_t>& buf, int8_t val) {
    buf.push_back(static_cast<uint8_t>(val));
}

static void write_int16(std::vector<uint8_t>& buf, int16_t val) {
    buf.push_back(static_cast<uint8_t>((val >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(val & 0xFF));
}

static void write_int32(std::vector<uint8_t>& buf, int32_t val) {
    buf.push_back(static_cast<uint8_t>((val >> 24) & 0xFF));
    buf.push_back(static_cast<uint8_t>((val >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((val >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(val & 0xFF));
}

static void write_int64(std::vector<uint8_t>& buf, int64_t val) {
    buf.push_back(static_cast<uint8_t>((val >> 56) & 0xFF));
    buf.push_back(static_cast<uint8_t>((val >> 48) & 0xFF));
    buf.push_back(static_cast<uint8_t>((val >> 40) & 0xFF));
    buf.push_back(static_cast<uint8_t>((val >> 32) & 0xFF));
    buf.push_back(static_cast<uint8_t>((val >> 24) & 0xFF));
    buf.push_back(static_cast<uint8_t>((val >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((val >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>(val & 0xFF));
}

static void write_varint(std::vector<uint8_t>& buf, int32_t value) {
    // Zigzag encode
    uint32_t encoded = static_cast<uint32_t>((value << 1) ^ (value >> 31));
    while (encoded >= 0x80) {
        buf.push_back(static_cast<uint8_t>((encoded & 0x7F) | 0x80));
        encoded >>= 7;
    }
    buf.push_back(static_cast<uint8_t>(encoded));
}

static void write_varlong(std::vector<uint8_t>& buf, int64_t value) {
    // Zigzag encode
    uint64_t encoded = static_cast<uint64_t>((value << 1) ^ (value >> 63));
    while (encoded >= 0x80) {
        buf.push_back(static_cast<uint8_t>((encoded & 0x7F) | 0x80));
        encoded >>= 7;
    }
    buf.push_back(static_cast<uint8_t>(encoded));
}

static int64_t read_int64_be(const uint8_t* data) {
    return (static_cast<int64_t>(data[0]) << 56) |
           (static_cast<int64_t>(data[1]) << 48) |
           (static_cast<int64_t>(data[2]) << 40) |
           (static_cast<int64_t>(data[3]) << 32) |
           (static_cast<int64_t>(data[4]) << 24) |
           (static_cast<int64_t>(data[5]) << 16) |
           (static_cast<int64_t>(data[6]) << 8) |
           static_cast<int64_t>(data[7]);
}

static int32_t read_int32_be(const uint8_t* data) {
    return (static_cast<int32_t>(data[0]) << 24) |
           (static_cast<int32_t>(data[1]) << 16) |
           (static_cast<int32_t>(data[2]) << 8) |
           static_cast<int32_t>(data[3]);
}

static int32_t read_varint(const uint8_t* data, size_t& offset, size_t max_size) {
    int32_t result = 0;
    int shift = 0;
    while (offset < max_size) {
        uint8_t byte = data[offset++];
        result |= (static_cast<int32_t>(byte & 0x7F) << shift);
        if ((byte & 0x80) == 0) break;
        shift += 7;
    }
    // Zigzag decode
    return (result >> 1) ^ -(result & 1);
}

static int64_t read_varlong(const uint8_t* data, size_t& offset, size_t max_size) {
    int64_t result = 0;
    int shift = 0;
    while (offset < max_size) {
        uint8_t byte = data[offset++];
        result |= (static_cast<int64_t>(byte & 0x7F) << shift);
        if ((byte & 0x80) == 0) break;
        shift += 7;
    }
    // Zigzag decode
    return (result >> 1) ^ -(result & 1);
}

// ============================================================================
// LogSegment Implementation - Kafka Native Format
// ============================================================================

static std::string format_offset_filename(int64_t offset) {
    std::ostringstream oss;
    oss << std::setw(20) << std::setfill('0') << offset;
    return oss.str();
}

LogSegment::LogSegment(const std::string& path, int64_t base_offset)
    : path_(path)
    , base_offset_(base_offset)
    , next_offset_(base_offset) {
    
    // Create directory if it doesn't exist
    fs::path dir_path(path);
    if (!fs::exists(dir_path)) {
        fs::create_directories(dir_path);
    }
    
    // Kafka-style filenames: 00000000000000000000.log
    std::string offset_str = format_offset_filename(base_offset);
    std::string data_path = path + "/" + offset_str + ".log";
    std::string index_path = path + "/" + offset_str + ".index";
    
    // Open data file
    data_file_.open(data_path, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!data_file_.is_open()) {
        data_file_.open(data_path, std::ios::out | std::ios::binary);
        data_file_.close();
        data_file_.open(data_path, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    }
    
    // Open index file
    index_file_.open(index_path, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!index_file_.is_open()) {
        index_file_.open(index_path, std::ios::out | std::ios::binary);
        index_file_.close();
        index_file_.open(index_path, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    }
    
    // Recover state from existing files
    recover_offset();
}

void LogSegment::recover_offset() {
    // Read through log file to find all batches and recover next_offset
    data_file_.clear();
    data_file_.seekg(0, std::ios::end);
    auto file_size = data_file_.tellg();
    
    if (file_size <= 0) {
        return;
    }
    
    data_file_.seekg(0, std::ios::beg);
    
    int64_t max_offset = base_offset_;
    size_t batch_count = 0;
    
    while (data_file_.good() && data_file_.tellg() < file_size) {
        // Read batch header (first 12 bytes to get baseOffset and batchLength)
        uint8_t header[12];
        data_file_.read(reinterpret_cast<char*>(header), 12);
        
        if (!data_file_.good() || data_file_.gcount() < 12) break;
        
        int64_t batch_base_offset = read_int64_be(header);
        int32_t batch_length = read_int32_be(header + 8);
        
        if (batch_length <= 0 || batch_length > 100 * 1024 * 1024) {
            break; // Invalid batch
        }
        
        // Read rest of header up to lastOffsetDelta
        // Offset 12: partitionLeaderEpoch (4)
        // Offset 16: magic (1)
        // Offset 17: crc (4)
        // Offset 21: attributes (2)
        // Offset 23: lastOffsetDelta (4)
        // So we need to read 15 more bytes (offset 12-26)
        uint8_t mid_header[15];
        data_file_.read(reinterpret_cast<char*>(mid_header), 15);
        
        if (!data_file_.good()) break;
        
        // lastOffsetDelta is at offset 23, which is mid_header[11..14] (23 - 12 = 11)
        int32_t last_offset_delta = read_int32_be(mid_header + 11);
        
        // Skip rest of batch (batch_length includes bytes from partitionLeaderEpoch to end)
        // We've read 12 bytes (baseOffset + batchLength) + 15 bytes = 27 bytes
        // Total batch size is 12 + batch_length, so remaining = 12 + batch_length - 27 = batch_length - 15
        int64_t remaining = static_cast<int64_t>(batch_length) - 15;
        if (remaining > 0) {
            data_file_.seekg(remaining, std::ios::cur);
        }
        
        int64_t batch_max_offset = batch_base_offset + last_offset_delta;
        if (batch_max_offset >= max_offset) {
            max_offset = batch_max_offset + 1;
        }
        batch_count++;
    }
    
    next_offset_ = max_offset;
    
    if (batch_count > 0) {
        LOG_DEBUG("Recovered segment {} with {} batches, next_offset={}",
                  path_, batch_count, next_offset_);
    }
}

// Build a Kafka RecordBatch for a single record
std::vector<uint8_t> LogSegment::build_record_batch(const Record& record, int64_t offset) {
    std::vector<uint8_t> batch;
    
    int64_t timestamp = record.timestamp;
    if (timestamp == 0) {
        timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }
    
    // Build the record first
    std::vector<uint8_t> rec;
    write_int8(rec, 0);  // attributes
    write_varlong(rec, 0);  // timestampDelta (0 for single record)
    write_varint(rec, 0);   // offsetDelta (0 for single record)
    
    // key
    if (record.key.empty()) {
        write_varint(rec, -1);  // null key
    } else {
        write_varint(rec, static_cast<int32_t>(record.key.size()));
        for (char c : record.key) {
            rec.push_back(static_cast<uint8_t>(c));
        }
    }
    
    // value
    if (record.value.empty()) {
        write_varint(rec, -1);  // null value
    } else {
        write_varint(rec, static_cast<int32_t>(record.value.size()));
        rec.insert(rec.end(), record.value.begin(), record.value.end());
    }
    
    // headers (empty)
    write_varint(rec, 0);
    
    // Now build the batch
    // Record with length prefix
    std::vector<uint8_t> records_data;
    write_varint(records_data, static_cast<int32_t>(rec.size()));
    records_data.insert(records_data.end(), rec.begin(), rec.end());
    
    // RecordBatch header
    write_int64(batch, offset);       // baseOffset
    
    // Calculate batchLength (everything after batchLength field)
    // = 4 (partitionLeaderEpoch) + 1 (magic) + 4 (crc) + 2 (attributes) + 4 (lastOffsetDelta)
    // + 8 (firstTimestamp) + 8 (maxTimestamp) + 8 (producerId) + 2 (producerEpoch)
    // + 4 (baseSequence) + 4 (recordCount) + records_data.size()
    int32_t batch_length = 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + static_cast<int32_t>(records_data.size());
    write_int32(batch, batch_length);
    
    write_int32(batch, -1);           // partitionLeaderEpoch
    write_int8(batch, 2);             // magic (v2)
    
    size_t crc_pos = batch.size();
    write_int32(batch, 0);            // CRC placeholder
    
    size_t crc_data_start = batch.size();
    
    write_int16(batch, 0);            // attributes
    write_int32(batch, 0);            // lastOffsetDelta (0 for single record)
    write_int64(batch, timestamp);    // firstTimestamp
    write_int64(batch, timestamp);    // maxTimestamp
    write_int64(batch, -1);           // producerId
    write_int16(batch, -1);           // producerEpoch
    write_int32(batch, -1);           // baseSequence
    write_int32(batch, 1);            // recordCount
    
    // Append records
    batch.insert(batch.end(), records_data.begin(), records_data.end());
    
    // Calculate and write CRC32C
    uint32_t crc = compute_crc32c(batch.data() + crc_data_start, batch.size() - crc_data_start);
    batch[crc_pos]     = static_cast<uint8_t>((crc >> 24) & 0xFF);
    batch[crc_pos + 1] = static_cast<uint8_t>((crc >> 16) & 0xFF);
    batch[crc_pos + 2] = static_cast<uint8_t>((crc >> 8) & 0xFF);
    batch[crc_pos + 3] = static_cast<uint8_t>(crc & 0xFF);
    
    return batch;
}

int64_t LogSegment::append(const Record& record) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Get current position in data file
    data_file_.seekp(0, std::ios::end);
    int64_t position = data_file_.tellp();
    
    int64_t offset = next_offset_;
    
    // Build Kafka RecordBatch
    std::vector<uint8_t> batch = build_record_batch(record, offset);
    
    // Write batch to log file
    data_file_.write(reinterpret_cast<const char*>(batch.data()), batch.size());
    data_file_.flush();
    
    // Update index (Kafka format: 4 bytes relative offset, 4 bytes position)
    index_file_.seekp(0, std::ios::end);
    int32_t relative_offset = static_cast<int32_t>(offset - base_offset_);
    int32_t pos32 = static_cast<int32_t>(position);
    
    // Write in big-endian format
    uint8_t index_entry[8];
    index_entry[0] = static_cast<uint8_t>((relative_offset >> 24) & 0xFF);
    index_entry[1] = static_cast<uint8_t>((relative_offset >> 16) & 0xFF);
    index_entry[2] = static_cast<uint8_t>((relative_offset >> 8) & 0xFF);
    index_entry[3] = static_cast<uint8_t>(relative_offset & 0xFF);
    index_entry[4] = static_cast<uint8_t>((pos32 >> 24) & 0xFF);
    index_entry[5] = static_cast<uint8_t>((pos32 >> 16) & 0xFF);
    index_entry[6] = static_cast<uint8_t>((pos32 >> 8) & 0xFF);
    index_entry[7] = static_cast<uint8_t>(pos32 & 0xFF);
    
    index_file_.write(reinterpret_cast<const char*>(index_entry), 8);
    index_file_.flush();
    
    next_offset_++;
    
    return offset;
}

int64_t LogSegment::append_raw_batch(const std::vector<uint8_t>& batch_data, int32_t record_count) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (batch_data.size() < 12) {
        throw std::runtime_error("Invalid batch data: too short");
    }
    
    // Get current position in data file
    data_file_.seekp(0, std::ios::end);
    int64_t position = data_file_.tellp();
    
    int64_t offset = next_offset_;
    
    // Make a copy of the batch to modify the baseOffset
    std::vector<uint8_t> batch = batch_data;
    
    // Update baseOffset (first 8 bytes) to our assigned offset - big-endian
    batch[0] = static_cast<uint8_t>((offset >> 56) & 0xFF);
    batch[1] = static_cast<uint8_t>((offset >> 48) & 0xFF);
    batch[2] = static_cast<uint8_t>((offset >> 40) & 0xFF);
    batch[3] = static_cast<uint8_t>((offset >> 32) & 0xFF);
    batch[4] = static_cast<uint8_t>((offset >> 24) & 0xFF);
    batch[5] = static_cast<uint8_t>((offset >> 16) & 0xFF);
    batch[6] = static_cast<uint8_t>((offset >> 8) & 0xFF);
    batch[7] = static_cast<uint8_t>(offset & 0xFF);
    
    // Write batch to log file (CRC remains valid because baseOffset is not included in CRC)
    data_file_.write(reinterpret_cast<const char*>(batch.data()), batch.size());
    data_file_.flush();
    
    // Update index for each record in the batch
    index_file_.seekp(0, std::ios::end);
    
    for (int32_t i = 0; i < record_count; i++) {
        int32_t relative_offset = static_cast<int32_t>((offset + i) - base_offset_);
        int32_t pos32 = static_cast<int32_t>(position);
        
        // Write in big-endian format
        uint8_t index_entry[8];
        index_entry[0] = static_cast<uint8_t>((relative_offset >> 24) & 0xFF);
        index_entry[1] = static_cast<uint8_t>((relative_offset >> 16) & 0xFF);
        index_entry[2] = static_cast<uint8_t>((relative_offset >> 8) & 0xFF);
        index_entry[3] = static_cast<uint8_t>(relative_offset & 0xFF);
        index_entry[4] = static_cast<uint8_t>((pos32 >> 24) & 0xFF);
        index_entry[5] = static_cast<uint8_t>((pos32 >> 16) & 0xFF);
        index_entry[6] = static_cast<uint8_t>((pos32 >> 8) & 0xFF);
        index_entry[7] = static_cast<uint8_t>(pos32 & 0xFF);
        
        index_file_.write(reinterpret_cast<const char*>(index_entry), 8);
    }
    index_file_.flush();
    
    next_offset_ += record_count;
    
    return offset;
}

std::vector<Record> LogSegment::read(int64_t start_offset, size_t max_records) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<Record> records;
    
    if (start_offset < base_offset_ || start_offset >= next_offset_) {
        return records;
    }
    
    data_file_.clear();
    data_file_.flush();
    index_file_.clear();
    index_file_.flush();
    
    // Find position using index
    int64_t relative_offset = start_offset - base_offset_;
    int64_t index_position = relative_offset * KAFKA_INDEX_ENTRY_SIZE;
    
    index_file_.seekg(index_position, std::ios::beg);
    
    if (!index_file_.good()) {
        return records;
    }
    
    size_t count = 0;
    while (count < max_records && index_file_.good()) {
        uint8_t index_entry[8];
        index_file_.read(reinterpret_cast<char*>(index_entry), 8);
        
        if (!index_file_.good() || index_file_.gcount() < 8) break;
        
        [[maybe_unused]] int32_t rel_off = read_int32_be(index_entry);
        int32_t position = read_int32_be(index_entry + 4);
        
        // Read batch from data file
        data_file_.seekg(position, std::ios::beg);
        
        // Read batch header
        uint8_t batch_header[61];
        data_file_.read(reinterpret_cast<char*>(batch_header), 61);
        
        if (!data_file_.good()) break;
        
        int64_t batch_base_offset = read_int64_be(batch_header);
        int32_t batch_length = read_int32_be(batch_header + 8);
        int64_t first_timestamp = read_int64_be(batch_header + 25);
        int32_t record_count = read_int32_be(batch_header + 57);
        
        // Read records data
        size_t records_size = batch_length - 49; // 49 = bytes after batchLength before records
        std::vector<uint8_t> records_data(records_size);
        data_file_.read(reinterpret_cast<char*>(records_data.data()), records_size);
        
        if (!data_file_.good()) break;
        
        // Parse records
        size_t pos = 0;
        for (int32_t i = 0; i < record_count && pos < records_data.size(); i++) {
            int32_t record_len = read_varint(records_data.data(), pos, records_data.size());
            if (record_len <= 0) break;
            
            [[maybe_unused]] size_t record_end = pos + record_len;
            
            // attributes
            pos++;
            
            // timestampDelta
            int64_t ts_delta = read_varlong(records_data.data(), pos, records_data.size());
            
            // offsetDelta
            int32_t offset_delta = read_varint(records_data.data(), pos, records_data.size());
            
            // keyLength
            int32_t key_len = read_varint(records_data.data(), pos, records_data.size());
            
            Record record;
            record.offset = batch_base_offset + offset_delta;
            record.timestamp = first_timestamp + ts_delta;
            
            if (key_len > 0 && pos + key_len <= records_data.size()) {
                record.key.assign(reinterpret_cast<const char*>(records_data.data() + pos), key_len);
                pos += key_len;
            } else if (key_len > 0) {
                break;
            }
            
            // valueLength
            int32_t value_len = read_varint(records_data.data(), pos, records_data.size());
            
            if (value_len > 0 && pos + value_len <= records_data.size()) {
                record.value.assign(records_data.begin() + pos, records_data.begin() + pos + value_len);
                pos += value_len;
            } else if (value_len > 0) {
                break;
            }
            
            // Skip headers
            int32_t headers_count = read_varint(records_data.data(), pos, records_data.size());
            for (int32_t h = 0; h < headers_count && pos < records_data.size(); h++) {
                int32_t hkey_len = read_varint(records_data.data(), pos, records_data.size());
                if (hkey_len > 0) pos += hkey_len;
                int32_t hval_len = read_varint(records_data.data(), pos, records_data.size());
                if (hval_len > 0) pos += hval_len;
            }
            
            records.push_back(std::move(record));
            count++;
            
            if (count >= max_records) break;
        }
    }
    
    return records;
}

std::pair<std::vector<uint8_t>, int32_t> LogSegment::read_raw(int64_t start_offset, size_t max_bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::vector<uint8_t> data;
    int32_t record_count = 0;
    
    if (start_offset < base_offset_ || start_offset >= next_offset_) {
        return {data, 0};
    }
    
    data_file_.clear();
    data_file_.flush();
    index_file_.clear();
    index_file_.flush();
    
    // Find position using index
    int64_t relative_offset = start_offset - base_offset_;
    int64_t index_position = relative_offset * KAFKA_INDEX_ENTRY_SIZE;
    
    index_file_.seekg(index_position, std::ios::beg);
    
    if (!index_file_.good()) {
        return {data, 0};
    }
    
    // Read index entry to get file position
    uint8_t index_entry[8];
    index_file_.read(reinterpret_cast<char*>(index_entry), 8);
    
    if (!index_file_.good() || index_file_.gcount() < 8) {
        return {data, 0};
    }
    
    int32_t position = read_int32_be(index_entry + 4);
    
    // Read from data file starting at position
    data_file_.seekg(position, std::ios::beg);
    
    // Read batches until max_bytes is reached
    size_t total_bytes = 0;
    while (data_file_.good() && total_bytes < max_bytes) {
        // Read batch header to get length
        std::streampos batch_start = data_file_.tellg();
        
        uint8_t header[12];
        data_file_.read(reinterpret_cast<char*>(header), 12);
        
        if (!data_file_.good() || data_file_.gcount() < 12) break;
        
        int32_t batch_length = read_int32_be(header + 8);
        
        if (batch_length <= 0 || batch_length > 100 * 1024 * 1024) {
            break; // Invalid batch
        }
        
        // Total batch size: 8 (baseOffset) + 4 (batchLength) + batchLength
        size_t batch_total_size = 12 + batch_length;
        
        if (total_bytes + batch_total_size > max_bytes && total_bytes > 0) {
            // Would exceed max_bytes, stop here
            break;
        }
        
        // Seek back to batch start
        data_file_.seekg(batch_start, std::ios::beg);
        
        // Read the entire batch
        std::vector<uint8_t> batch(batch_total_size);
        data_file_.read(reinterpret_cast<char*>(batch.data()), batch_total_size);
        
        if (!data_file_.good() || static_cast<size_t>(data_file_.gcount()) < batch_total_size) {
            break;
        }
        
        // Get record count from batch (at offset 57 from start: baseOffset(8) + batchLength(4) + ... + recordCount(4))
        int32_t batch_record_count = read_int32_be(batch.data() + 57);
        record_count += batch_record_count;
        
        // Append to output
        data.insert(data.end(), batch.begin(), batch.end());
        total_bytes += batch_total_size;
    }
    
    return {data, record_count};
}

void LogSegment::flush() {
    std::lock_guard<std::mutex> lock(mutex_);
    data_file_.flush();
    index_file_.flush();
}

int64_t LogSegment::size() const {
    return next_offset_ - base_offset_;
}

int64_t LogSegment::get_size_bytes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Get current file position to restore later
    auto& file = const_cast<std::fstream&>(data_file_);
    auto current_pos = file.tellg();
    
    // Seek to end to get file size
    file.seekg(0, std::ios::end);
    int64_t size = file.tellg();
    
    // Restore position
    file.seekg(current_pos);
    
    return size > 0 ? size : 0;
}

int64_t LogSegment::get_base_offset() const {
    return base_offset_;
}

int64_t LogSegment::get_next_offset() const {
    return next_offset_;
}

bool LogSegment::contains_offset(int64_t offset) const {
    return offset >= base_offset_ && offset < next_offset_;
}
