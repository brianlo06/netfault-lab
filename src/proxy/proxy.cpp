#include "netfault/proxy.hpp"

#include "netfault/connection_state.hpp"
#include "netfault/fault_rng.hpp"
#include "netfault/logger.hpp"
#include "netfault/relay.hpp"
#include "netfault/socket_io.hpp"
#include "netfault/timer_queue.hpp"
#include "netfault/unique_fd.hpp"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/timerfd.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>

namespace netfault {
namespace {

using TimePoint = std::chrono::steady_clock::time_point;

constexpr std::uint64_t kListenerToken = 1;
constexpr std::uint64_t kSignalToken = 2;
constexpr std::uint64_t kTimerToken = 3;
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
             bool upstream_connecting, const RelayConfig& relay_config, const FaultPlan& faults,
             SocketIo& io, const Logger& logger, TimePoint now)
      : id(connection_id),
        client_fd(std::move(client_socket)),
        upstream_fd(std::move(upstream_socket)),
        observer(logger, connection_id),
        relay(relay_config, faults, connection_id, client_fd.get(), upstream_fd.get(),
              upstream_connecting, io, observer, now) {}

  std::uint64_t id;
  UniqueFd client_fd;
  UniqueFd upstream_fd;
  std::uint64_t client_token{0};
  std::uint64_t upstream_token{0};
  std::uint32_t client_mask{0};    // last mask applied via epoll_ctl
  std::uint32_t upstream_mask{0};
  std::optional<TimePoint> scheduled_wake;
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
  if (::sigemptyset(&mask) < 0 || ::sigaddset(&mask, SIGINT) < 0 || ::sigaddset(&mask, SIGTERM) < 0 ||
      ::sigaddset(&mask, SIGUSR1) < 0) {
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

// std::chrono::steady_clock is CLOCK_MONOTONIC on the supported Linux/glibc
// target, so heap deadlines arm the timerfd in absolute monotonic time.
UniqueFd create_timer_fd() {
  UniqueFd fd{::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC)};
  if (!fd) {
    throw std::runtime_error("timerfd_create: " + std::string{std::strerror(errno)});
  }
  return fd;
}

class ProxyRuntime {
 public:
  explicit ProxyRuntime(ProxyConfig config)
      : config_(std::move(config)),
        listener_(create_listener(config_.listen, config_.max_connections)),
        signal_fd_(create_signal_fd()),
        timer_fd_(create_timer_fd()),
        epoll_fd_(::epoll_create1(EPOLL_CLOEXEC)) {
    if (!epoll_fd_) {
      throw std::runtime_error("epoll_create1: " + std::string{std::strerror(errno)});
    }
    add_epoll(listener_.get(), EPOLLIN, kListenerToken);
    add_epoll(signal_fd_.get(), EPOLLIN, kSignalToken);
    add_epoll(timer_fd_.get(), EPOLLIN, kTimerToken);
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
            stopping = true;
          } else if (signal_info.ssi_signo == SIGUSR1) {
            emit_metrics_snapshot();
            write_metrics_file();
          } else {
            stopping = true;
          }
        } else if (token == kTimerToken) {
          drain_timer_fd();
          const auto now = std::chrono::steady_clock::now();
          const auto processed = process_due_timers(now);
          logger_.event("timer_fired", 0, "listening",
                        "processed=" + std::to_string(processed) +
                            ",pending=" + std::to_string(timer_queue_.size()));
        } else {
          socket_ready(token, event_mask);
        }
      }
      arm_timer_fd();
    }

    const auto shutdown_now = std::chrono::steady_clock::now();
    while (!connections_.empty()) {
      close_connection(connections_.begin()->first, ConnectionState::FullyClosed, "proxy_shutdown",
                       shutdown_now);
    }
    write_metrics_file();
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

  // EPOLLERR and EPOLLHUP are delivered whether or not they are requested, so
  // they need not be named here beyond documenting the intent.
  //
  // EPOLLRDHUP is requested only alongside read interest. It is level-
  // triggered and stays asserted from the moment a peer half-closes, so
  // holding it while reads are paused for backpressure wakes the event loop
  // continuously for a condition we have deliberately chosen not to act on
  // yet — a busy loop that makes no progress. Dropping it with read interest
  // costs nothing: level-triggered delivery re-reports the half-close as soon
  // as reads resume, and a resumed read observes the EOF directly anyway.
  [[nodiscard]] static std::uint32_t to_epoll_mask(InterestSet interest) {
    std::uint32_t events = EPOLLERR | EPOLLHUP;
    if (interest.read) {
      events |= EPOLLIN | EPOLLRDHUP;
    }
    if (interest.write) {
      events |= EPOLLOUT;
    }
    return events;
  }

  void drain_timer_fd() {
    std::uint64_t expirations = 0;
    ssize_t bytes_read = -1;
    do {
      bytes_read = ::read(timer_fd_.get(), &expirations, sizeof(expirations));
    } while (bytes_read < 0 && errno == EINTR);
    // EAGAIN is fine: a rearm may have raced the expiration we were woken for.
  }

  void arm_timer_fd() {
    const auto deadline = timer_queue_.next_deadline();
    if (deadline == last_armed_deadline_) {
      return;  // avoid rearm churn; an unchanged armed deadline stays valid
    }
    itimerspec spec{};
    if (deadline) {
      auto since_epoch = deadline->time_since_epoch();
      if (since_epoch <= std::chrono::steady_clock::duration::zero()) {
        since_epoch = std::chrono::steady_clock::duration{1};
      }
      const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(since_epoch);
      const auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(since_epoch - seconds);
      spec.it_value.tv_sec = static_cast<time_t>(seconds.count());
      spec.it_value.tv_nsec = static_cast<long>(nanoseconds.count());
      if (spec.it_value.tv_sec == 0 && spec.it_value.tv_nsec == 0) {
        spec.it_value.tv_nsec = 1;  // all-zero disarms; a past deadline must still fire
      }
    }
    if (::timerfd_settime(timer_fd_.get(), TFD_TIMER_ABSTIME, &spec, nullptr) < 0) {
      logger_.event("timer_arm_failed", 0, "failed", std::strerror(errno));
      return;
    }
    last_armed_deadline_ = deadline;
    if (deadline) {
      const auto delta_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                *deadline - std::chrono::steady_clock::now())
                                .count();
      logger_.event("timer_armed", 0, "listening",
                    "delta_ms=" + std::to_string(delta_ms) +
                        ",pending=" + std::to_string(timer_queue_.size()));
    }
  }

  std::size_t process_due_timers(TimePoint now) {
    // The armed deadline is consumed by this firing (or superseded below), so
    // the next arm call must not be suppressed by the change-detection cache.
    last_armed_deadline_.reset();
    std::size_t processed = 0;
    while (auto event = timer_queue_.pop_due(now)) {
      ++processed;
      const auto iterator = connections_.find(event->connection_id);
      if (iterator == connections_.end()) {
        continue;  // stale entry for a removed connection; discarded lazily
      }
      Connection& connection = *iterator->second;
      switch (event->kind) {
        case TimerKind::Pump:
          if (connection.scheduled_wake && *connection.scheduled_wake <= now) {
            connection.scheduled_wake.reset();
          }
          pump_connection(connection.id, now);
          break;
        case TimerKind::ConnectTimeout:
          if (connection.relay.upstream_connecting()) {
            close_connection(connection.id, ConnectionState::TimedOut, "connect_timeout", now);
          }
          break;
        case TimerKind::IdleTimeout: {
          const auto idle_deadline = connection.relay.last_activity() + config_.idle_timeout;
          if (now >= idle_deadline) {
            close_connection(connection.id, ConnectionState::TimedOut, "idle_timeout", now);
          } else {
            timer_queue_.schedule(idle_deadline, connection.id, TimerKind::IdleTimeout);
          }
          break;
        }
      }
    }
    return processed;
  }

  // Builds the exported JSON document from event-loop-owned values. All
  // fields are numeric or fixed identifiers; payload bytes never appear.
  [[nodiscard]] std::string build_metrics_json() const {
    const auto now = std::chrono::steady_clock::now();
    const auto wall_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
    std::string json = "{";
    json += "\"timestamp_ms\":" + std::to_string(wall_ms);
    json += ",\"listen\":\"" + config_.listen.to_string() + "\"";
    json += ",\"upstream\":\"" + config_.upstream.to_string() + "\"";
    json += ",\"active_connections\":" + std::to_string(connections_.size());
    json += ",\"total_accepted\":" + std::to_string(next_connection_id_ - 1);
    json += ",\"pending_timers\":" + std::to_string(timer_queue_.size());
    json += ",\"dropped_log_events\":" + std::to_string(logger_.dropped_events());
    json += ",\"closes\":{";
    json += "\"fully_closed\":" + std::to_string(closes_fully_closed_);
    json += ",\"reset\":" + std::to_string(closes_reset_);
    json += ",\"failed\":" + std::to_string(closes_failed_);
    json += ",\"timed_out\":" + std::to_string(closes_timed_out_);
    json += "},\"connections\":[";
    bool first = true;
    for (const auto& [connection_id, connection] : connections_) {
      if (!first) {
        json += ",";
      }
      first = false;
      json += "{\"id\":" + std::to_string(connection_id) + ",";
      const auto body = connection->relay.metrics_json(now);
      json += body.substr(1);  // splice the relay object's fields after "id"
    }
    json += "]}";
    return json;
  }

  // Atomic export: write a sibling temporary file, then rename over the
  // target, so readers never observe a partially-written document.
  void write_metrics_file() const {
    if (config_.metrics_file.empty()) {
      return;
    }
    const auto temporary_path = config_.metrics_file + ".tmp";
    const auto json = build_metrics_json() + "\n";
    UniqueFd fd{::open(temporary_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644)};
    if (!fd) {
      logger_.event("metrics_file_failed", 0, "failed", "open=" + std::string{std::strerror(errno)});
      return;
    }
    std::size_t offset = 0;
    while (offset < json.size()) {
      const auto written = ::write(fd.get(), json.data() + offset, json.size() - offset);
      if (written < 0) {
        if (errno == EINTR) {
          continue;
        }
        logger_.event("metrics_file_failed", 0, "failed", "write=" + std::string{std::strerror(errno)});
        return;
      }
      offset += static_cast<std::size_t>(written);
    }
    fd.reset();
    if (std::rename(temporary_path.c_str(), config_.metrics_file.c_str()) != 0) {
      logger_.event("metrics_file_failed", 0, "failed", "rename=" + std::string{std::strerror(errno)});
      return;
    }
    logger_.event("metrics_file_written", 0, "listening", "bytes=" + std::to_string(json.size()));
  }

  // Copies event-loop-owned counters into log events on demand. No payload
  // bytes are ever included; only sizes, counts, and durations.
  void emit_metrics_snapshot() {
    const auto now = std::chrono::steady_clock::now();
    logger_.event("metrics_snapshot", 0, "listening",
                  "active_connections=" + std::to_string(connections_.size()) +
                      ",total_accepted=" + std::to_string(next_connection_id_ - 1) +
                      ",pending_timers=" + std::to_string(timer_queue_.size()) +
                      ",dropped_log_events=" + std::to_string(logger_.dropped_events()));
    for (const auto& [connection_id, connection] : connections_) {
      logger_.event("connection_snapshot", connection_id, state_name(connection->relay.state()),
                    "snapshot=1" + connection->relay.close_detail(now));
    }
  }

  void apply_socket_buffer_size(int fd) const {
    if (config_.socket_buffer_bytes == 0) {
      return;
    }
    const int size = static_cast<int>(config_.socket_buffer_bytes);
    if (::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size)) < 0 ||
        ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &size, sizeof(size)) < 0) {
      logger_.event("socket_buffer_resize_failed", 0, "failed", std::strerror(errno));
    }
  }

  // Decides once at accept whether this connection receives the fault plan,
  // sampling the derived connection seed so the choice is reproducible.
  [[nodiscard]] bool faults_apply(std::uint64_t connection_id) const {
    if (!config_.faults.any_enabled()) {
      return false;
    }
    if (config_.faults.apply_probability >= 1.0) {
      return true;
    }
    SplitMix64 rng{derive_connection_seed(config_.faults.master_seed, connection_id)};
    return sample_unit_interval(rng.next()) < config_.faults.apply_probability;
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
      apply_socket_buffer_size(upstream.get());

      const auto upstream_address = config_.upstream.to_sockaddr();
      const int connect_result = ::connect(upstream.get(), reinterpret_cast<const sockaddr*>(&upstream_address),
                                           sizeof(upstream_address));
      const bool connecting = connect_result < 0 && errno == EINPROGRESS;
      if (connect_result < 0 && !connecting) {
        logger_.event("upstream_connect_failed", 0, "failed", std::strerror(errno));
        continue;
      }

      const auto now = std::chrono::steady_clock::now();
      const auto connection_id = next_connection_id_++;
      const RelayConfig relay_config{
          .buffer_bytes_per_direction = config_.buffer_bytes_per_direction,
          .low_water_bytes = config_.low_water_bytes,
          .high_water_bytes = config_.high_water_bytes,
      };
      const bool apply_faults = faults_apply(connection_id);
      const FaultPlan& effective_faults = apply_faults ? config_.faults : disabled_faults_;
      auto connection = std::make_unique<Connection>(connection_id, std::move(client), std::move(upstream),
                                                     connecting, relay_config, effective_faults, socket_io_,
                                                     logger_, now);
      connection->client_token = next_token_++;
      connection->upstream_token = next_token_++;

      const int client_fd = connection->client_fd.get();
      const int upstream_fd = connection->upstream_fd.get();
      bindings_.emplace(connection->client_token, SocketBinding{connection_id, Side::Client});
      bindings_.emplace(connection->upstream_token, SocketBinding{connection_id, Side::Upstream});
      auto& inserted = *connections_.emplace(connection_id, std::move(connection)).first->second;
      inserted.client_mask = to_epoll_mask(inserted.relay.desired_interest(Side::Client, now));
      inserted.upstream_mask = to_epoll_mask(inserted.relay.desired_interest(Side::Upstream, now));
      add_epoll(client_fd, inserted.client_mask, inserted.client_token);
      add_epoll(upstream_fd, inserted.upstream_mask, inserted.upstream_token);
      logger_.event("connection_accepted", connection_id, state_name(inserted.relay.state()));
      if (config_.faults.any_enabled()) {
        logger_.event("fault_config", connection_id, state_name(inserted.relay.state()),
                      "applied=" + std::to_string(apply_faults ? 1 : 0) + ",seed=" +
                          std::to_string(derive_connection_seed(config_.faults.master_seed, connection_id)));
      }
      if (!connecting) {
        logger_.event("upstream_connected", connection_id, "active");
      } else if (config_.connect_timeout.count() > 0) {
        timer_queue_.schedule(now + config_.connect_timeout, connection_id, TimerKind::ConnectTimeout);
      }
      if (config_.idle_timeout.count() > 0) {
        timer_queue_.schedule(now + config_.idle_timeout, connection_id, TimerKind::IdleTimeout);
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
    const auto now = std::chrono::steady_clock::now();

    if (binding.side == Side::Upstream && connection.relay.upstream_connecting() && (events & EPOLLOUT) != 0U) {
      int socket_error = 0;
      socklen_t length = sizeof(socket_error);
      if (::getsockopt(connection.upstream_fd.get(), SOL_SOCKET, SO_ERROR, &socket_error, &length) < 0 ||
          socket_error != 0) {
        const int error = socket_error != 0 ? socket_error : errno;
        close_connection(connection.id, ConnectionState::Failed,
                         "upstream_connect=" + std::string{std::strerror(error)}, now);
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
                         std::strerror(socket_error), now);
        return;
      }
    }

    const bool readable_event = (events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) != 0U;
    const bool hangup_hint = (events & (EPOLLRDHUP | EPOLLHUP)) != 0U;
    if (readable_event && !(binding.side == Side::Upstream && connection.relay.upstream_connecting())) {
      const auto result = connection.relay.handle_readable(binding.side, hangup_hint, now);
      if (result.status == PumpStatus::CloseConnection) {
        close_connection(connection.id, result.final_state, result.reason, now);
        return;
      }
    }

    pump_connection(connection.id, now);
  }

  // Flushes both destinations, propagates half-closes, refreshes interests or
  // closes, and schedules a fault wake if the relay is blocked on time.
  void pump_connection(std::uint64_t connection_id, TimePoint now) {
    const auto iterator = connections_.find(connection_id);
    if (iterator == connections_.end()) {
      return;
    }
    Connection& connection = *iterator->second;
    if (!connection.relay.upstream_connecting()) {
      for (const Side destination : {Side::Upstream, Side::Client}) {
        const auto result = connection.relay.flush(destination, now);
        if (result.status == PumpStatus::CloseConnection) {
          close_connection(connection.id, result.final_state, result.reason, now);
          return;
        }
      }
    }
    connection.relay.propagate_half_closes();
    refresh_or_close(connection.id, now);
    schedule_relay_wake(connection_id, now);
  }

  void schedule_relay_wake(std::uint64_t connection_id, TimePoint now) {
    const auto iterator = connections_.find(connection_id);
    if (iterator == connections_.end()) {
      return;
    }
    Connection& connection = *iterator->second;
    std::optional<TimePoint> wake;
    for (const Side destination : {Side::Upstream, Side::Client}) {
      const auto candidate = connection.relay.next_wake(destination, now);
      if (candidate && (!wake || *candidate < *wake)) {
        wake = candidate;
      }
    }
    if (wake && (!connection.scheduled_wake || *wake < *connection.scheduled_wake)) {
      timer_queue_.schedule(*wake, connection.id, TimerKind::Pump);
      connection.scheduled_wake = *wake;
    }
  }

  void refresh_or_close(std::uint64_t connection_id, TimePoint now) {
    const auto iterator = connections_.find(connection_id);
    if (iterator == connections_.end()) {
      return;
    }
    Connection& connection = *iterator->second;
    if (connection.relay.fully_drained()) {
      close_connection(connection_id, ConnectionState::FullyClosed, "orderly_shutdown", now);
      return;
    }
    apply_interest(connection, Side::Client, now);
    apply_interest(connection, Side::Upstream, now);
  }

  // Re-registers a socket's epoll interest only when the mask actually
  // changes; the loop otherwise spends two epoll_ctl calls per iteration
  // rewriting registrations that are already correct.
  void apply_interest(Connection& connection, Side side, TimePoint now) {
    const auto mask = to_epoll_mask(connection.relay.desired_interest(side, now));
    auto& cached = side == Side::Client ? connection.client_mask : connection.upstream_mask;
    if (mask == cached) {
      return;
    }
    cached = mask;
    modify_epoll(side == Side::Client ? connection.client_fd.get() : connection.upstream_fd.get(), mask,
                 side == Side::Client ? connection.client_token : connection.upstream_token);
  }

  void close_connection(std::uint64_t connection_id, ConnectionState final_state, std::string detail,
                        TimePoint now) {
    const auto iterator = connections_.find(connection_id);
    if (iterator == connections_.end()) {
      return;
    }
    Connection& connection = *iterator->second;
    static_cast<void>(::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, connection.client_fd.get(), nullptr));
    static_cast<void>(::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, connection.upstream_fd.get(), nullptr));
    bindings_.erase(connection.client_token);
    bindings_.erase(connection.upstream_token);
    detail += connection.relay.close_detail(now);
    logger_.event("connection_closed", connection.id, state_name(final_state), detail);
    switch (final_state) {
      case ConnectionState::Reset:
        ++closes_reset_;
        break;
      case ConnectionState::Failed:
        ++closes_failed_;
        break;
      case ConnectionState::TimedOut:
        ++closes_timed_out_;
        break;
      default:
        ++closes_fully_closed_;
        break;
    }
    connections_.erase(iterator);
  }

  ProxyConfig config_;
  FaultPlan disabled_faults_{};
  Logger logger_;
  SystemSocketIo socket_io_;
  UniqueFd listener_;
  UniqueFd signal_fd_;
  UniqueFd timer_fd_;
  UniqueFd epoll_fd_;
  TimerQueue timer_queue_;
  std::optional<TimePoint> last_armed_deadline_;
  std::uint64_t next_connection_id_{1};
  std::uint64_t next_token_{4};
  std::uint64_t closes_fully_closed_{0};
  std::uint64_t closes_reset_{0};
  std::uint64_t closes_failed_{0};
  std::uint64_t closes_timed_out_{0};
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
  if (config_.socket_buffer_bytes != 0 &&
      (config_.socket_buffer_bytes < 1'024 || config_.socket_buffer_bytes > 1'024U * 1'024U)) {
    throw std::invalid_argument("socket_buffer_bytes must be 0 (kernel default) or between 1024 and 1048576");
  }
  if (config_.connect_timeout < std::chrono::milliseconds::zero() ||
      config_.idle_timeout < std::chrono::milliseconds::zero() ||
      config_.connect_timeout > std::chrono::minutes{10} || config_.idle_timeout > std::chrono::minutes{10}) {
    throw std::invalid_argument("timeouts must be between 0 (disabled) and 600000 ms");
  }
  config_.faults.validate();
  if (!config_.listen.is_loopback() && !config_.allow_non_loopback_listen) {
    throw std::invalid_argument("non-loopback listen requires --unsafe-allow-non-loopback-listen");
  }
  if (!config_.upstream.is_loopback() && !config_.allow_non_loopback_upstream) {
    throw std::invalid_argument("non-loopback upstream requires --unsafe-allow-non-loopback-upstream");
  }
}

int Proxy::run() { return ProxyRuntime{config_}.run(); }

}  // namespace netfault
