#include "server.hpp"
#include <iostream>
#include <algorithm>

namespace eventhorizon {
namespace network {

// ============================================================================
// ClientSession Implementation
// ============================================================================

ClientSession::ClientSession(tcp::socket socket, MessageHandler handler)
    : socket_(std::move(socket))
    , message_handler_(std::move(handler)) {
}

ClientSession::~ClientSession() {
    close();
}

void ClientSession::start() {
    do_read_header();
}

void ClientSession::close() {
    if (connected_.exchange(false)) {
        boost::system::error_code ec;
        socket_.shutdown(tcp::socket::shutdown_both, ec);
        socket_.close(ec);
    }
}

std::string ClientSession::get_client_address() const {
    try {
        auto endpoint = socket_.remote_endpoint();
        return endpoint.address().to_string() + ":" + std::to_string(endpoint.port());
    } catch (...) {
        return "unknown";
    }
}

bool ClientSession::is_connected() const {
    return connected_.load();
}

void ClientSession::do_read_header() {
    auto self = shared_from_this();
    
    boost::asio::async_read(
        socket_,
        boost::asio::buffer(header_buffer_),
        [this, self](boost::system::error_code ec, std::size_t /*length*/) {
            if (!ec) {
                // Kafka usa big-endian para o tamanho da mensagem
                uint32_t body_length = 
                    (static_cast<uint32_t>(header_buffer_[0]) << 24) |
                    (static_cast<uint32_t>(header_buffer_[1]) << 16) |
                    (static_cast<uint32_t>(header_buffer_[2]) << 8) |
                    (static_cast<uint32_t>(header_buffer_[3]));
                
                // Limite de segurança: máximo 100MB por mensagem
                if (body_length > 100 * 1024 * 1024) {
                    std::cerr << "Message too large: " << body_length << " bytes\n";
                    close();
                    return;
                }
                
                do_read_body(body_length);
            } else {
                if (ec != boost::asio::error::eof && 
                    ec != boost::asio::error::connection_reset) {
                    std::cerr << "Read header error: " << ec.message() << "\n";
                }
                close();
            }
        });
}

void ClientSession::do_read_body(uint32_t body_length) {
    auto self = shared_from_this();
    
    body_buffer_.resize(body_length);
    
    boost::asio::async_read(
        socket_,
        boost::asio::buffer(body_buffer_),
        [this, self](boost::system::error_code ec, std::size_t /*length*/) {
            if (!ec) {
                // Processar a mensagem e obter resposta
                std::vector<uint8_t> response;
                if (message_handler_) {
                    try {
                        response = message_handler_(body_buffer_);
                    } catch (const std::exception& e) {
                        std::cerr << "Handler error: " << e.what() << "\n";
                    }
                }
                
                if (!response.empty()) {
                    do_write(std::move(response));
                } else {
                    // Continuar lendo próxima mensagem
                    do_read_header();
                }
            } else {
                std::cerr << "Read body error: " << ec.message() << "\n";
                close();
            }
        });
}

void ClientSession::do_write(std::vector<uint8_t> response) {
    auto self = shared_from_this();
    
    // Preparar buffer com header de tamanho (4 bytes big-endian)
    auto write_buffer = std::make_shared<std::vector<uint8_t>>();
    uint32_t size = static_cast<uint32_t>(response.size());
    
    write_buffer->reserve(4 + response.size());
    write_buffer->push_back(static_cast<uint8_t>((size >> 24) & 0xFF));
    write_buffer->push_back(static_cast<uint8_t>((size >> 16) & 0xFF));
    write_buffer->push_back(static_cast<uint8_t>((size >> 8) & 0xFF));
    write_buffer->push_back(static_cast<uint8_t>(size & 0xFF));
    write_buffer->insert(write_buffer->end(), response.begin(), response.end());
    
    boost::asio::async_write(
        socket_,
        boost::asio::buffer(*write_buffer),
        [this, self, write_buffer](boost::system::error_code ec, std::size_t /*length*/) {
            if (!ec) {
                // Continuar lendo próxima mensagem
                do_read_header();
            } else {
                std::cerr << "Write error: " << ec.message() << "\n";
                close();
            }
        });
}

// ============================================================================
// Server Implementation
// ============================================================================

Server::Server(uint16_t port, size_t thread_pool_size)
    : port_(port)
    , thread_pool_size_(thread_pool_size) {
}

Server::~Server() {
    stop();
}

void Server::start() {
    if (running_.exchange(true)) {
        return; // Já está rodando
    }
    
    try {
        acceptor_ = std::make_unique<tcp::acceptor>(
            io_context_, 
            tcp::endpoint(tcp::v4(), port_)
        );
        
        // Permitir reuso do endereço
        acceptor_->set_option(boost::asio::socket_base::reuse_address(true));
        
        std::cout << "Server listening on port " << port_ << "\n";
        
        do_accept();
        
        // Iniciar thread pool
        thread_pool_.reserve(thread_pool_size_);
        for (size_t i = 0; i < thread_pool_size_; ++i) {
            thread_pool_.emplace_back([this]() {
                try {
                    io_context_.run();
                } catch (const std::exception& e) {
                    std::cerr << "IO context error: " << e.what() << "\n";
                }
            });
        }
    } catch (const std::exception& e) {
        running_ = false;
        throw std::runtime_error("Failed to start server: " + std::string(e.what()));
    }
}

void Server::stop() {
    if (!running_.exchange(false)) {
        return; // Já está parado
    }
    
    std::cout << "Stopping server...\n";
    
    // Fechar o acceptor
    if (acceptor_) {
        boost::system::error_code ec;
        acceptor_->close(ec);
    }
    
    // Fechar todas as sessões
    {
        std::lock_guard<std::mutex> lock(sessions_mutex_);
        for (auto& weak_session : sessions_) {
            if (auto session = weak_session.lock()) {
                session->close();
            }
        }
        sessions_.clear();
    }
    
    // Parar o io_context
    io_context_.stop();
    
    // Aguardar threads terminarem
    for (auto& thread : thread_pool_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    thread_pool_.clear();
    
    std::cout << "Server stopped.\n";
}

void Server::set_message_handler(MessageHandler handler) {
    message_handler_ = std::move(handler);
}

size_t Server::get_connection_count() const {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    
    size_t count = 0;
    for (const auto& weak_session : sessions_) {
        if (auto session = weak_session.lock()) {
            if (session->is_connected()) {
                ++count;
            }
        }
    }
    return count;
}

void Server::do_accept() {
    acceptor_->async_accept(
        [this](boost::system::error_code ec, tcp::socket socket) {
            if (!ec) {
                std::cout << "New connection from: " 
                          << socket.remote_endpoint().address().to_string() 
                          << ":" << socket.remote_endpoint().port() << "\n";
                
                // Configurar socket
                socket.set_option(tcp::no_delay(true));
                
                // Criar sessão
                auto session = std::make_shared<ClientSession>(
                    std::move(socket), 
                    message_handler_
                );
                
                // Registrar sessão
                {
                    std::lock_guard<std::mutex> lock(sessions_mutex_);
                    
                    // Limpar sessões expiradas
                    sessions_.erase(
                        std::remove_if(sessions_.begin(), sessions_.end(),
                            [](const std::weak_ptr<ClientSession>& w) {
                                return w.expired();
                            }),
                        sessions_.end()
                    );
                    
                    sessions_.push_back(session);
                }
                
                session->start();
            } else if (ec != boost::asio::error::operation_aborted) {
                std::cerr << "Accept error: " << ec.message() << "\n";
            }
            
            // Continuar aceitando conexões se ainda estiver rodando
            if (running_) {
                do_accept();
            }
        });
}

void Server::remove_session(std::shared_ptr<ClientSession> session) {
    std::lock_guard<std::mutex> lock(sessions_mutex_);
    
    sessions_.erase(
        std::remove_if(sessions_.begin(), sessions_.end(),
            [&session](const std::weak_ptr<ClientSession>& w) {
                auto s = w.lock();
                return !s || s == session;
            }),
        sessions_.end()
    );
}

} // namespace network
} // namespace eventhorizon
