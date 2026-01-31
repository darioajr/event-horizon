#pragma once

#include <boost/asio.hpp>
#include <memory>
#include <functional>
#include <atomic>
#include <thread>
#include <vector>

namespace eventhorizon {
namespace network {

using boost::asio::ip::tcp;

/**
 * @brief Representa uma sessão de cliente conectado
 */
class ClientSession : public std::enable_shared_from_this<ClientSession> {
public:
    using MessageHandler = std::function<std::vector<uint8_t>(const std::vector<uint8_t>&)>;

    ClientSession(tcp::socket socket, MessageHandler handler);
    ~ClientSession();

    void start();
    void close();
    
    std::string get_client_address() const;
    bool is_connected() const;

private:
    void do_read_header();
    void do_read_body(uint32_t body_length);
    void do_write(std::vector<uint8_t> response);

    tcp::socket socket_;
    MessageHandler message_handler_;
    std::atomic<bool> connected_{true};
    
    // Buffer para leitura do header (4 bytes para tamanho)
    std::array<uint8_t, 4> header_buffer_;
    std::vector<uint8_t> body_buffer_;
};

/**
 * @brief Servidor TCP compatível com protocolo Kafka
 */
class Server {
public:
    using MessageHandler = ClientSession::MessageHandler;

    Server(uint16_t port, size_t thread_pool_size = 4);
    ~Server();

    /**
     * @brief Inicia o servidor
     */
    void start();

    /**
     * @brief Para o servidor
     */
    void stop();

    /**
     * @brief Define o handler para processar mensagens recebidas
     * @param handler Função que recebe bytes e retorna resposta
     */
    void set_message_handler(MessageHandler handler);

    /**
     * @brief Retorna a porta em que o servidor está escutando
     */
    uint16_t get_port() const { return port_; }

    /**
     * @brief Verifica se o servidor está rodando
     */
    bool is_running() const { return running_; }

    /**
     * @brief Retorna número de conexões ativas
     */
    size_t get_connection_count() const;

private:
    void do_accept();
    void remove_session(std::shared_ptr<ClientSession> session);

    uint16_t port_;
    size_t thread_pool_size_;
    std::atomic<bool> running_{false};
    
    boost::asio::io_context io_context_;
    std::unique_ptr<tcp::acceptor> acceptor_;
    std::vector<std::thread> thread_pool_;
    
    MessageHandler message_handler_;
    
    mutable std::mutex sessions_mutex_;
    std::vector<std::weak_ptr<ClientSession>> sessions_;
};

} // namespace network
} // namespace eventhorizon
