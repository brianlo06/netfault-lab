#include "netfault/proxy.hpp"

#include "netfault/backpressure.hpp"
#include "netfault/byte_queue.hpp"
#include "netfault/connection_state.hpp"
#include "netfault/logger.hpp"
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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace netfault {
namespace {

constexpr std::uint64_t kListenerToken = 1;
constexpr std::uint64_t kSignalToken = 2;
constexpr int kMaxEvents = 128;

enum class Side { Client, Upstream };

struct EndpointState {
  UniqueFd fd;
  bool read_open{true};
  bool write_open{true};
  bool connecting{false};
  std::uint64_t token{0};
};

struct ConnectionMetrics {
  std::uint64_t client_bytes_read{0};
  std::uint64_t upstream_bytes_read{0};
  std::uint64_t client_bytes_written{0};
  std::uint64_t upstream_bytes_written{0};
  std::uint64_t read_operations{0};
  std::uint64_t write_operations{0};
  std::uint64_t partial_writes{0};
  std::uint64_t eagain_events{0};
};

struct Connection {
  Connection(std::uint64_t connection_id, UniqueFd client_socket, UniqueFd upstream_socket,
             std::size_t buffer_capacity, std::size_t low_water_bytes, std::size_t high_water_bytes)
      : id(connection_id),
        client{std::move(client_socket)},
        upstream{std::move(upstream_socket)},
        client_to_upstream(buffer_capacity),
        upstream_to_client(buffer_capacity),
        client_to_upstream_backpressure(low_water_bytes, high_water_bytes),
        upstream_to_client_backpressure(low_water_bytes, high_water_bytes) {}

  std::uint64_t id;
  ConnectionState state{ConnectionState::ConnectingUpstream};
  EndpointState client;
  EndpointState upstream;
  ByteQueue client_to_upstream;
  ByteQueue upstream_to_client;
  BackpressureTracker client_to_upstream_backpressure;
  BackpressureTracker upstream_to_client_backpressure;
  ConnectionMetrics metrics;
  std::chrono::steady_clock::time_point accepted_at{std::chrono::steady_clock::now()};
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
      auto connection = std::make_unique<Connection>(connection_id, std::move(client), std::move(upstream),
                                                     config_.buffer_bytes_per_direction, config_.low_water_bytes,
                                                     config_.high_water_bytes);
      connection->upstream.connecting = connecting;
      connection->state = connecting ? ConnectionState::ConnectingUpstream : ConnectionState::Active;
      connection->client.token = next_token_++;
      connection->upstream.token = next_token_++;

      const int client_fd = connection->client.fd.get();
      const int upstream_fd = connection->upstream.fd.get();
      bindings_.emplace(connection->client.token, SocketBinding{connection_id, Side::Client});
      bindings_.emplace(connection->upstream.token, SocketBinding{connection_id, Side::Upstream});
      connections_.emplace(connection_id, std::move(connection));
      add_epoll(client_fd, desired_events(*connections_.at(connection_id), Side::Client),
                connections_.at(connection_id)->client.token);
      add_epoll(upstream_fd, desired_events(*connections_.at(connection_id), Side::Upstream),
                connections_.at(connection_id)->upstream.token);
      logger_.event("connection_accepted", connection_id, state_name(connections_.at(connection_id)->state));
      if (!connecting) {
        logger_.event("upstream_connected", connection_id, "active");
      }
    }
  }

  [[nodiscard]] std::uint32_t desired_events(const Connection& connection, Side side) const {
    constexpr std::uint32_t base = EPOLLRDHUP | EPOLLERR | EPOLLHUP;
    if (side == Side::Client) {
      std::uint32_t events = base;
      if (connection.client.read_open && !connection.client_to_upstream_backpressure.read_paused()) {
        events |= EPOLLIN;
      }
      if (connection.client.write_open && !connection.upstream_to_client.empty()) {
        events |= EPOLLOUT;
      }
      return events;
    }

    std::uint32_t events = base;
    if (connection.upstream.connecting) {
      return events | EPOLLOUT;
    }
    if (connection.upstream.read_open && !connection.upstream_to_client_backpressure.read_paused()) {
      events |= EPOLLIN;
    }
    if (connection.upstream.write_open && !connection.client_to_upstream.empty()) {
      events |= EPOLLOUT;
    }
    return events;
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

    if (binding.side == Side::Upstream && connection.upstream.connecting && (events & EPOLLOUT) != 0U) {
      int socket_error = 0;
      socklen_t length = sizeof(socket_error);
      if (::getsockopt(connection.upstream.fd.get(), SOL_SOCKET, SO_ERROR, &socket_error, &length) < 0 ||
          socket_error != 0) {
        const int error = socket_error != 0 ? socket_error : errno;
        close_connection(connection.id, ConnectionState::Failed,
                         "upstream_connect=" + std::string{std::strerror(error)});
        return;
      }
      connection.upstream.connecting = false;
      connection.state = ConnectionState::Active;
      logger_.event("upstream_connected", connection.id, "active");
    }

    if ((events & EPOLLERR) != 0U && !connection.upstream.connecting) {
      int socket_error = 0;
      socklen_t length = sizeof(socket_error);
      if (::getsockopt(binding.side == Side::Client ? connection.client.fd.get() : connection.upstream.fd.get(),
                       SOL_SOCKET, SO_ERROR, &socket_error, &length) == 0 && socket_error != 0) {
        close_connection(connection.id, socket_error == ECONNRESET ? ConnectionState::Reset : ConnectionState::Failed,
                         std::strerror(socket_error));
        return;
      }
    }

    if (connections_.find(binding.connection_id) == connections_.end()) {
      return;
    }

    bool ok = true;
    if (binding.side == Side::Client && (events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) != 0U) {
      ok = read_into(connection, Side::Client, (events & (EPOLLRDHUP | EPOLLHUP)) != 0U);
    } else if (binding.side == Side::Upstream && !connection.upstream.connecting &&
               (events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) != 0U) {
      ok = read_into(connection, Side::Upstream, (events & (EPOLLRDHUP | EPOLLHUP)) != 0U);
    }
    if (!ok || connections_.find(binding.connection_id) == connections_.end()) {
      return;
    }

    if (!connection.upstream.connecting) {
      if (!flush(connection, Side::Upstream) || !flush(connection, Side::Client)) {
        return;
      }
    }
    propagate_half_closes(connection);
    refresh_or_close(connection.id);
  }

  bool read_into(Connection& connection, Side source_side, bool hangup_hint) {
    EndpointState& source = source_side == Side::Client ? connection.client : connection.upstream;
    ByteQueue& destination_queue =
        source_side == Side::Client ? connection.client_to_upstream : connection.upstream_to_client;
    BackpressureTracker& backpressure = source_side == Side::Client
                                                ? connection.client_to_upstream_backpressure
                                                : connection.upstream_to_client_backpressure;

    while (source.read_open && !backpressure.read_paused()) {
      const auto available_to_high_water = backpressure.high_water_bytes() - destination_queue.size();
      const auto full_writable_span = destination_queue.writable_span();
      const auto writable = full_writable_span.first(std::min(full_writable_span.size(), available_to_high_water));
      const auto count = ::recv(source.fd.get(), writable.data(), writable.size(), 0);
      if (count > 0) {
        const auto bytes = static_cast<std::size_t>(count);
        destination_queue.commit_write(bytes);
        ++connection.metrics.read_operations;
        if (source_side == Side::Client) {
          connection.metrics.client_bytes_read += static_cast<std::uint64_t>(bytes);
        } else {
          connection.metrics.upstream_bytes_read += static_cast<std::uint64_t>(bytes);
        }
        observe_backpressure(connection, source_side);
        continue;
      }
      if (count == 0) {
        source.read_open = false;
        logger_.event("peer_half_closed", connection.id, state_name(connection.state),
                      source_side == Side::Client ? "direction=client_read" : "direction=upstream_read");
        break;
      }
      if (errno == EINTR) {
        continue;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        ++connection.metrics.eagain_events;
        if (hangup_hint) {
          source.read_open = false;
          logger_.event("peer_half_closed", connection.id, state_name(connection.state),
                        source_side == Side::Client ? "direction=client_read" : "direction=upstream_read");
        }
        break;
      }
      close_connection(connection.id, errno == ECONNRESET ? ConnectionState::Reset : ConnectionState::Failed,
                       "read=" + std::string{std::strerror(errno)});
      return false;
    }
    return true;
  }

  bool flush(Connection& connection, Side destination_side) {
    EndpointState& destination = destination_side == Side::Client ? connection.client : connection.upstream;
    ByteQueue& source_queue =
        destination_side == Side::Client ? connection.upstream_to_client : connection.client_to_upstream;
    if (!destination.write_open || destination.connecting) {
      return true;
    }

    while (!source_queue.empty()) {
      const auto readable = source_queue.readable_span();
      const auto count = ::send(destination.fd.get(), readable.data(), readable.size(), MSG_NOSIGNAL);
      if (count > 0) {
        const auto bytes = static_cast<std::size_t>(count);
        if (bytes < readable.size()) {
          ++connection.metrics.partial_writes;
        }
        source_queue.consume(bytes);
        observe_backpressure(connection, destination_side == Side::Upstream ? Side::Client : Side::Upstream);
        ++connection.metrics.write_operations;
        if (destination_side == Side::Client) {
          connection.metrics.client_bytes_written += static_cast<std::uint64_t>(bytes);
        } else {
          connection.metrics.upstream_bytes_written += static_cast<std::uint64_t>(bytes);
        }
        continue;
      }
      if (count < 0 && errno == EINTR) {
        continue;
      }
      if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        ++connection.metrics.eagain_events;
        break;
      }
      const int error = count < 0 ? errno : EPIPE;
      close_connection(connection.id, error == ECONNRESET || error == EPIPE ? ConnectionState::Reset
                                                                            : ConnectionState::Failed,
                       "write=" + std::string{std::strerror(error)});
      return false;
    }
    return true;
  }

  void observe_backpressure(Connection& connection, Side source_side) {
    ByteQueue& queue = source_side == Side::Client ? connection.client_to_upstream
                                                   : connection.upstream_to_client;
    BackpressureTracker& tracker = source_side == Side::Client
                                       ? connection.client_to_upstream_backpressure
                                       : connection.upstream_to_client_backpressure;
    const auto transition = tracker.observe(queue.size(), queue.capacity(), BackpressureTracker::Clock::now());
    if (transition == BackpressureTransition::None) {
      return;
    }
    const auto direction = source_side == Side::Client ? "client_to_upstream" : "upstream_to_client";
    const auto event = transition == BackpressureTransition::Paused ? "read_paused" : "read_resumed";
    logger_.event(event, connection.id, state_name(connection.state),
                  "direction=" + std::string{direction} + ",queue_bytes=" + std::to_string(queue.size()) +
                      ",low_water_bytes=" + std::to_string(tracker.low_water_bytes()) +
                      ",high_water_bytes=" + std::to_string(tracker.high_water_bytes()));
  }

  void propagate_half_closes(Connection& connection) {
    if (!connection.client.read_open && connection.client_to_upstream.empty() &&
        connection.upstream.write_open && !connection.upstream.connecting) {
      if (::shutdown(connection.upstream.fd.get(), SHUT_WR) == 0 || errno == ENOTCONN) {
        connection.upstream.write_open = false;
        logger_.event("half_close_forwarded", connection.id, state_name(connection.state),
                      "direction=upstream_write");
      }
    }
    if (!connection.upstream.read_open && connection.upstream_to_client.empty() && connection.client.write_open) {
      if (::shutdown(connection.client.fd.get(), SHUT_WR) == 0 || errno == ENOTCONN) {
        connection.client.write_open = false;
        logger_.event("half_close_forwarded", connection.id, state_name(connection.state),
                      "direction=client_write");
      }
    }
  }

  void refresh_or_close(std::uint64_t connection_id) {
    const auto iterator = connections_.find(connection_id);
    if (iterator == connections_.end()) {
      return;
    }
    Connection& connection = *iterator->second;
    if (!connection.client.read_open && !connection.upstream.read_open && connection.client_to_upstream.empty() &&
        connection.upstream_to_client.empty()) {
      close_connection(connection_id, ConnectionState::FullyClosed, "orderly_shutdown");
      return;
    }

    connection.state = derive_connection_state(ConnectionLifecycle{
        .upstream_connecting = connection.upstream.connecting,
        .client_read_open = connection.client.read_open,
        .upstream_read_open = connection.upstream.read_open,
    });

    modify_epoll(connection.client.fd.get(), desired_events(connection, Side::Client), connection.client.token);
    modify_epoll(connection.upstream.fd.get(), desired_events(connection, Side::Upstream), connection.upstream.token);
  }

  void close_connection(std::uint64_t connection_id, ConnectionState final_state, std::string detail) {
    const auto iterator = connections_.find(connection_id);
    if (iterator == connections_.end()) {
      return;
    }
    Connection& connection = *iterator->second;
    connection.state = final_state;
    static_cast<void>(::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, connection.client.fd.get(), nullptr));
    static_cast<void>(::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, connection.upstream.fd.get(), nullptr));
    bindings_.erase(connection.client.token);
    bindings_.erase(connection.upstream.token);

    const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - connection.accepted_at)
                              .count();
    detail += ",duration_ms=" + std::to_string(duration);
    detail += ",client_read=" + std::to_string(connection.metrics.client_bytes_read);
    detail += ",upstream_read=" + std::to_string(connection.metrics.upstream_bytes_read);
    detail += ",client_written=" + std::to_string(connection.metrics.client_bytes_written);
    detail += ",upstream_written=" + std::to_string(connection.metrics.upstream_bytes_written);
    detail += ",read_operations=" + std::to_string(connection.metrics.read_operations);
    detail += ",write_operations=" + std::to_string(connection.metrics.write_operations);
    detail += ",partial_writes=" + std::to_string(connection.metrics.partial_writes);
    detail += ",eagain_events=" + std::to_string(connection.metrics.eagain_events);
    detail += ",rejected_bytes=0";
    detail += ",c2u_high_water=" + std::to_string(connection.client_to_upstream.high_water_mark());
    detail += ",u2c_high_water=" + std::to_string(connection.upstream_to_client.high_water_mark());
    const auto now = BackpressureTracker::Clock::now();
    detail += ",c2u_pause_count=" +
              std::to_string(connection.client_to_upstream_backpressure.pause_count());
    detail += ",c2u_resume_count=" +
              std::to_string(connection.client_to_upstream_backpressure.resume_count());
    detail += ",c2u_saturation_count=" +
              std::to_string(connection.client_to_upstream_backpressure.saturation_count());
    detail += ",c2u_paused_us=" +
              std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(
                                 connection.client_to_upstream_backpressure.paused_duration(now))
                                 .count());
    detail += ",u2c_pause_count=" +
              std::to_string(connection.upstream_to_client_backpressure.pause_count());
    detail += ",u2c_resume_count=" +
              std::to_string(connection.upstream_to_client_backpressure.resume_count());
    detail += ",u2c_saturation_count=" +
              std::to_string(connection.upstream_to_client_backpressure.saturation_count());
    detail += ",u2c_paused_us=" +
              std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(
                                 connection.upstream_to_client_backpressure.paused_duration(now))
                                 .count());
    logger_.event("connection_closed", connection.id, state_name(final_state), detail);
    connections_.erase(iterator);
  }

  ProxyConfig config_;
  Logger logger_;
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
