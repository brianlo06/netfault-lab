#include "netfault/proxy.hpp"

#include "netfault/connection_state.hpp"
#include "netfault/logger.hpp"
#include "netfault/relay.hpp"
#include "netfault/socket_io.hpp"
#include "netfault/unique_fd.hpp"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <sys/socket.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace netfault {
namespace {

constexpr std::uint64_t kListenerToken = 1;
constexpr std::uint64_t kSignalToken = 2;
constexpr int kMaxEvents = 128;

// Forwards relay events into the shared JSON Lines logger under one connection id.
class ConnectionLogObserver final : public RelayObserver {
 public:
  ConnectionLogObserver(const Logger& logger, std::uint64_t connection_id)
      : logger_(logger), connection_id_(connection_id) {}

  void on_event(std::string_view event, ConnectionState state, std::string_view detail) override {
    logger_.event(event, connection_id_, state_name(state), detail);
  }

 private:
  const Logger& logger_;
  std::uint64_t connection_id_;
};

struct Connection {
  Connection(std::uint64_t connection_id, UniqueFd client_socket, UniqueFd upstream_socket,
             bool upstream_connecting, const RelayConfig& relay_config, SocketIo& io, const Logger& logger)
      : id(connection_id),
        client_fd(std::move(client_socket)),
        upstream_fd(std::move(upstream_socket)),
        observer(logger, connection_id),
        relay(relay_config, client_fd.get(), upstream_fd.get(), upstream_connecting, io, observer) {}

  std::uint64_t id;
  UniqueFd client_fd;
  UniqueFd upstream_fd;
  std::uint64_t client_token{0};
  std::uint64_t upstream_token{0};
  ConnectionLogObserver observer;
  Relay relay;
};

struct SocketBinding {
  std::uint64_t connection_id;
  Side side;
};

UniqueFd create_listener(const Endpoint& endpoint, std::size_t max_connections) {
  UniqueFd listener{::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)};
  if (!listener) {
    throw std::runtime_error("socket(listener): " + std::string{std::strerror(errno)});
  }

  int enabled = 1;
  if (::setsockopt(listener.get(), SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) < 0) {
    throw std::runtime_error("setsockopt(SO_REUSEADDR): " + std::string{std::strerror(errno)});
  }

  const auto address = endpoint.to_sockaddr();
  if (::bind(listener.get(), reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0) {
    throw std::runtime_error("bind(" + endpoint.to_string() + "): " + std::string{std::strerror(errno)});
  }

  const auto capped_backlog = std::min<std::size_t>(max_connections, static_cast<std::size_t>(SOMAXCONN));
  if (::listen(listener.get(), static_cast<int>(capped_backlog)) < 0) {
    throw std::runtime_error("listen: " + std::string{std::strerror(errno)});
  }
  return listener;
}

UniqueFd create_signal_fd() {
  sigset_t mask{};
  if (::sigemptyset(&mask) < 0 || ::sigaddset(&mask, SIGINT) < 0 || ::sigaddset(&mask, SIGTERM) < 0) {
    throw std::runtime_error("could not initialize signal mask");
  }
  if (::sigprocmask(SIG_BLOCK, &mask, nullptr) < 0) {
    throw std::runtime_error("sigprocmask: " + std::string{std::strerror(errno)});
  }
  UniqueFd fd{::signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC)};
  if (!fd) {
    throw std::runtime_error("signalfd: " + std::string{std::strerror(errno)});
  }
  return fd;
}

class ProxyRuntime {
 public:
  explicit ProxyRuntime(ProxyConfig config)
      : config_(std::move(config)),
        listener_(create_listener(config_.listen, config_.max_connections)),
        signal_fd_(create_signal_fd()),
        epoll_fd_(::epoll_create1(EPOLL_CLOEXEC)) {
    if (!epoll_fd_) {
      throw std::runtime_error("epoll_create1: " + std::string{std::strerror(errno)});
    }
    add_epoll(listener_.get(), EPOLLIN, kListenerToken);
    add_epoll(signal_fd_.get(), EPOLLIN, kSignalToken);
  }

  int run() {
    logger_.event("proxy_started", 0, "listening",
                  "listen=" + config_.listen.to_string() + ",upstream=" + config_.upstream.to_string());
    std::array<epoll_event, kMaxEvents> events{};
    bool stopping = false;

    while (!stopping) {
      const int count = ::epoll_wait(epoll_fd_.get(), events.data(), kMaxEvents, -1);
      if (count < 0) {
        if (errno == EINTR) {
          continue;
        }
        throw std::runtime_error("epoll_wait: " + std::string{std::strerror(errno)});
      }

      for (int index = 0; index < count && !stopping; ++index) {
        const auto token = events[static_cast<std::size_t>(index)].data.u64;
        const auto event_mask = events[static_cast<std::size_t>(index)].events;
        if (token == kListenerToken) {
          accept_ready();
        } else if (token == kSignalToken) {
          signalfd_siginfo signal_info{};
          ssize_t bytes_read = -1;
          do {
            bytes_read = ::read(signal_fd_.get(), &signal_info, sizeof(signal_info));
          } while (bytes_read < 0 && errno == EINTR);
          if (bytes_read != static_cast<ssize_t>(sizeof(signal_info))) {
            logger_.event("signal_read_incomplete", 0, "failed", std::strerror(errno));
          }
          stopping = true;
        } else {
          socket_ready(token, event_mask);
        }
      }
    }

    while (!connections_.empty()) {
      close_connection(connections_.begin()->first, ConnectionState::FullyClosed, "proxy_shutdown");
    }
    logger_.event("proxy_stopped", 0, "fully_closed",
                  "dropped_log_events=" + std::to_string(logger_.dropped_events()));
    return 0;
  }

 private:
  void add_epoll(int fd, std::uint32_t events, std::uint64_t token) {
    epoll_event event{};
    event.events = events;
    event.data.u64 = token;
    if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, fd, &event) < 0) {
      throw std::runtime_error("epoll_ctl(ADD): " + std::string{std::strerror(errno)});
    }
  }

  void modify_epoll(int fd, std::uint32_t events, std::uint64_t token) {
    epoll_event event{};
    event.events = events;
    event.data.u64 = token;
    if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_MOD, fd, &event) < 0 && errno != ENOENT) {
      throw std::runtime_error("epoll_ctl(MOD): " + std::string{std::strerror(errno)});
    }
  }

  [[nodiscard]] static std::uint32_t to_epoll_mask(InterestSet interest) {
    std::uint32_t events = EPOLLRDHUP | EPOLLERR | EPOLLHUP;
    if (interest.read) {
      events |= EPOLLIN;
    }
    if (interest.write) {
      events |= EPOLLOUT;
    }
    return events;
  }

  void accept_ready() {
    while (true) {
      UniqueFd client{::accept4(listener_.get(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC)};
      if (!client) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          return;
        }
        if (errno == EINTR) {
          continue;
        }
        logger_.event("accept_failed", 0, "failed", std::strerror(errno));
        return;
      }

      if (connections_.size() >= config_.max_connections) {
        logger_.event("connection_rejected", 0, "failed", "max_connections_reached");
        continue;
      }

      UniqueFd upstream{::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)};
      if (!upstream) {
        logger_.event("upstream_socket_failed", 0, "failed", std::strerror(errno));
        continue;
      }

      const auto upstream_address = config_.upstream.to_sockaddr();
      const int connect_result = ::connect(upstream.get(), reinterpret_cast<const sockaddr*>(&upstream_address),
                                           sizeof(upstream_address));
      const bool connecting = connect_result < 0 && errno == EINPROGRESS;
      if (connect_result < 0 && !connecting) {
        logger_.event("upstream_connect_failed", 0, "failed", std::strerror(errno));
        continue;
      }

      const auto connection_id = next_connection_id_++;
      const RelayConfig relay_config{
          .buffer_bytes_per_direction = config_.buffer_bytes_per_direction,
          .low_water_bytes = config_.low_water_bytes,
          .high_water_bytes = config_.high_water_bytes,
      };
      auto connection = std::make_unique<Connection>(connection_id, std::move(client), std::move(upstream),
                                                     connecting, relay_config, socket_io_, logger_);
      connection->client_token = next_token_++;
      connection->upstream_token = next_token_++;

      const int client_fd = connection->client_fd.get();
      const int upstream_fd = connection->upstream_fd.get();
      bindings_.emplace(connection->client_token, SocketBinding{connection_id, Side::Client});
      bindings_.emplace(connection->upstream_token, SocketBinding{connection_id, Side::Upstream});
      const auto& inserted = *connections_.emplace(connection_id, std::move(connection)).first->second;
      add_epoll(client_fd, to_epoll_mask(inserted.relay.desired_interest(Side::Client)), inserted.client_token);
      add_epoll(upstream_fd, to_epoll_mask(inserted.relay.desired_interest(Side::Upstream)),
                inserted.upstream_token);
      logger_.event("connection_accepted", connection_id, state_name(inserted.relay.state()));
      if (!connecting) {
        logger_.event("upstream_connected", connection_id, "active");
      }
    }
  }

  void socket_ready(std::uint64_t token, std::uint32_t events) {
    const auto binding_iterator = bindings_.find(token);
    if (binding_iterator == bindings_.end()) {
      return;
    }
    const SocketBinding binding = binding_iterator->second;
    const auto connection_iterator = connections_.find(binding.connection_id);
    if (connection_iterator == connections_.end()) {
      return;
    }
    Connection& connection = *connection_iterator->second;

    if (binding.side == Side::Upstream && connection.relay.upstream_connecting() && (events & EPOLLOUT) != 0U) {
      int socket_error = 0;
      socklen_t length = sizeof(socket_error);
      if (::getsockopt(connection.upstream_fd.get(), SOL_SOCKET, SO_ERROR, &socket_error, &length) < 0 ||
          socket_error != 0) {
        const int error = socket_error != 0 ? socket_error : errno;
        close_connection(connection.id, ConnectionState::Failed,
                         "upstream_connect=" + std::string{std::strerror(error)});
        return;
      }
      connection.relay.mark_upstream_connected();
      logger_.event("upstream_connected", connection.id, "active");
    }

    if ((events & EPOLLERR) != 0U && !connection.relay.upstream_connecting()) {
      int socket_error = 0;
      socklen_t length = sizeof(socket_error);
      const int fd = binding.side == Side::Client ? connection.client_fd.get() : connection.upstream_fd.get();
      if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &length) == 0 && socket_error != 0) {
        close_connection(connection.id,
                         socket_error == ECONNRESET ? ConnectionState::Reset : ConnectionState::Failed,
                         std::strerror(socket_error));
        return;
      }
    }

    const bool readable_event = (events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) != 0U;
    const bool hangup_hint = (events & (EPOLLRDHUP | EPOLLHUP)) != 0U;
    if (readable_event && !(binding.side == Side::Upstream && connection.relay.upstream_connecting())) {
      const auto result = connection.relay.handle_readable(binding.side, hangup_hint);
      if (result.status == PumpStatus::CloseConnection) {
        close_connection(connection.id, result.final_state, result.reason);
        return;
      }
    }

    if (!connection.relay.upstream_connecting()) {
      for (const Side destination : {Side::Upstream, Side::Client}) {
        const auto result = connection.relay.flush(destination);
        if (result.status == PumpStatus::CloseConnection) {
          close_connection(connection.id, result.final_state, result.reason);
          return;
        }
      }
    }
    connection.relay.propagate_half_closes();
    refresh_or_close(connection.id);
  }

  void refresh_or_close(std::uint64_t connection_id) {
    const auto iterator = connections_.find(connection_id);
    if (iterator == connections_.end()) {
      return;
    }
    Connection& connection = *iterator->second;
    if (connection.relay.fully_drained()) {
      close_connection(connection_id, ConnectionState::FullyClosed, "orderly_shutdown");
      return;
    }
    modify_epoll(connection.client_fd.get(), to_epoll_mask(connection.relay.desired_interest(Side::Client)),
                 connection.client_token);
    modify_epoll(connection.upstream_fd.get(), to_epoll_mask(connection.relay.desired_interest(Side::Upstream)),
                 connection.upstream_token);
  }

  void close_connection(std::uint64_t connection_id, ConnectionState final_state, std::string detail) {
    const auto iterator = connections_.find(connection_id);
    if (iterator == connections_.end()) {
      return;
    }
    Connection& connection = *iterator->second;
    static_cast<void>(::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, connection.client_fd.get(), nullptr));
    static_cast<void>(::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, connection.upstream_fd.get(), nullptr));
    bindings_.erase(connection.client_token);
    bindings_.erase(connection.upstream_token);
    detail += connection.relay.close_detail();
    logger_.event("connection_closed", connection.id, state_name(final_state), detail);
    connections_.erase(iterator);
  }

  ProxyConfig config_;
  Logger logger_;
  SystemSocketIo socket_io_;
  UniqueFd listener_;
  UniqueFd signal_fd_;
  UniqueFd epoll_fd_;
  std::uint64_t next_connection_id_{1};
  std::uint64_t next_token_{3};
  std::unordered_map<std::uint64_t, std::unique_ptr<Connection>> connections_;
  std::unordered_map<std::uint64_t, SocketBinding> bindings_;
};

}  // namespace

Proxy::Proxy(ProxyConfig config) : config_(std::move(config)) {
  if (config_.max_connections == 0) {
    throw std::invalid_argument("max_connections must be positive");
  }
  if (config_.buffer_bytes_per_direction < 1'024 || config_.buffer_bytes_per_direction > 16U * 1'024U * 1'024U) {
    throw std::invalid_argument("buffer_bytes_per_direction must be between 1024 and 16777216");
  }
  if (config_.low_water_bytes >= config_.high_water_bytes ||
      config_.high_water_bytes > config_.buffer_bytes_per_direction) {
    throw std::invalid_argument("watermarks must satisfy 0 <= low_water_bytes < high_water_bytes <= buffer_bytes");
  }
  if (!config_.listen.is_loopback() && !config_.allow_non_loopback_listen) {
    throw std::invalid_argument("non-loopback listen requires --unsafe-allow-non-loopback-listen");
  }
  if (!config_.upstream.is_loopback() && !config_.allow_non_loopback_upstream) {
    throw std::invalid_argument("non-loopback upstream requires --unsafe-allow-non-loopback-upstream");
  }
}

int Proxy::run() { return ProxyRuntime{config_}.run(); }

}  // namespace netfault
