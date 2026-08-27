#include "netfault/relay.hpp"

#include <errno.h>

#include <algorithm>
#include <cstring>

namespace netfault {

Relay::Relay(const RelayConfig& config, int client_fd, int upstream_fd, bool upstream_connecting,
             SocketIo& io, RelayObserver& observer)
    : client_fd_(client_fd),
      upstream_fd_(upstream_fd),
      upstream_connecting_(upstream_connecting),
      state_(upstream_connecting ? ConnectionState::ConnectingUpstream : ConnectionState::Active),
      client_to_upstream_(config.buffer_bytes_per_direction, config.low_water_bytes, config.high_water_bytes),
      upstream_to_client_(config.buffer_bytes_per_direction, config.low_water_bytes, config.high_water_bytes),
      io_(io),
      observer_(observer) {}

void Relay::mark_upstream_connected() {
  upstream_connecting_ = false;
  refresh_state();
}

Relay::Direction& Relay::direction_from(Side source) noexcept {
  return source == Side::Client ? client_to_upstream_ : upstream_to_client_;
}

const Relay::Direction& Relay::direction_from(Side source) const noexcept {
  return source == Side::Client ? client_to_upstream_ : upstream_to_client_;
}

bool& Relay::read_open(Side side) noexcept {
  return side == Side::Client ? client_read_open_ : upstream_read_open_;
}

bool& Relay::write_open(Side side) noexcept {
  return side == Side::Client ? client_write_open_ : upstream_write_open_;
}

void Relay::note_half_closed_read(Side source) {
  read_open(source) = false;
  refresh_state();
  observer_.on_event("peer_half_closed", state_,
                     source == Side::Client ? "direction=client_read" : "direction=upstream_read");
}

PumpResult Relay::close_for_error(int error, std::string_view operation) {
  const bool reset = error == ECONNRESET || error == EPIPE;
  return {PumpStatus::CloseConnection, reset ? ConnectionState::Reset : ConnectionState::Failed,
          std::string{operation} + "=" + std::strerror(error)};
}

PumpResult Relay::handle_readable(Side source, bool hangup_hint) {
  Direction& direction = direction_from(source);

  while (read_open(source) && !direction.backpressure.read_paused()) {
    const auto available_to_high_water = direction.backpressure.high_water_bytes() - direction.queue.size();
    const auto full_writable_span = direction.queue.writable_span();
    const auto writable = full_writable_span.first(std::min(full_writable_span.size(), available_to_high_water));
    const auto result = io_.receive(fd_for(source), writable);
    if (result.status == IoStatus::Transferred) {
      direction.queue.commit_write(result.bytes);
      ++metrics_.read_operations;
      if (source == Side::Client) {
        metrics_.client_bytes_read += static_cast<std::uint64_t>(result.bytes);
      } else {
        metrics_.upstream_bytes_read += static_cast<std::uint64_t>(result.bytes);
      }
      observe_backpressure(source);
      continue;
    }
    if (result.status == IoStatus::PeerClosed) {
      note_half_closed_read(source);
      break;
    }
    if (result.status == IoStatus::WouldBlock) {
      ++metrics_.eagain_events;
      if (hangup_hint) {
        note_half_closed_read(source);
      }
      break;
    }
    return close_for_error(result.error, "read");
  }
  return {};
}

PumpResult Relay::flush(Side destination) {
  const Side source = destination == Side::Client ? Side::Upstream : Side::Client;
  Direction& direction = direction_from(source);
  if (!write_open(destination) || (destination == Side::Upstream && upstream_connecting_)) {
    return {};
  }

  while (!direction.queue.empty()) {
    const auto readable = direction.queue.readable_span();
    const auto result = io_.send(fd_for(destination), readable);
    if (result.status == IoStatus::Transferred) {
      if (result.bytes < readable.size()) {
        ++metrics_.partial_writes;
      }
      direction.queue.consume(result.bytes);
      observe_backpressure(source);
      ++metrics_.write_operations;
      if (destination == Side::Client) {
        metrics_.client_bytes_written += static_cast<std::uint64_t>(result.bytes);
      } else {
        metrics_.upstream_bytes_written += static_cast<std::uint64_t>(result.bytes);
      }
      continue;
    }
    if (result.status == IoStatus::WouldBlock) {
      ++metrics_.eagain_events;
      break;
    }
    return close_for_error(result.error, "write");
  }
  return {};
}

void Relay::observe_backpressure(Side source) {
  Direction& direction = direction_from(source);
  const auto transition = direction.backpressure.observe(direction.queue.size(), direction.queue.capacity(),
                                                         BackpressureTracker::Clock::now());
  if (transition == BackpressureTransition::None) {
    return;
  }
  const auto direction_label = source == Side::Client ? "client_to_upstream" : "upstream_to_client";
  const auto event = transition == BackpressureTransition::Paused ? "read_paused" : "read_resumed";
  observer_.on_event(event, state_,
                     "direction=" + std::string{direction_label} +
                         ",queue_bytes=" + std::to_string(direction.queue.size()) +
                         ",low_water_bytes=" + std::to_string(direction.backpressure.low_water_bytes()) +
                         ",high_water_bytes=" + std::to_string(direction.backpressure.high_water_bytes()));
}

void Relay::propagate_half_closes() {
  if (!client_read_open_ && client_to_upstream_.queue.empty() && upstream_write_open_ && !upstream_connecting_) {
    if (io_.shutdown_write(upstream_fd_)) {
      upstream_write_open_ = false;
      observer_.on_event("half_close_forwarded", state_, "direction=upstream_write");
    }
  }
  if (!upstream_read_open_ && upstream_to_client_.queue.empty() && client_write_open_) {
    if (io_.shutdown_write(client_fd_)) {
      client_write_open_ = false;
      observer_.on_event("half_close_forwarded", state_, "direction=client_write");
    }
  }
}

bool Relay::fully_drained() const noexcept {
  return !client_read_open_ && !upstream_read_open_ && client_to_upstream_.queue.empty() &&
         upstream_to_client_.queue.empty();
}

InterestSet Relay::desired_interest(Side side) const noexcept {
  if (side == Side::Upstream && upstream_connecting_) {
    return {.read = false, .write = true};
  }
  const Side inbound_source = side;
  const Side outbound_source = side == Side::Client ? Side::Upstream : Side::Client;
  const bool side_read_open = side == Side::Client ? client_read_open_ : upstream_read_open_;
  const bool side_write_open = side == Side::Client ? client_write_open_ : upstream_write_open_;
  return {
      .read = side_read_open && !direction_from(inbound_source).backpressure.read_paused(),
      .write = side_write_open && !direction_from(outbound_source).queue.empty(),
  };
}

void Relay::refresh_state() noexcept {
  state_ = derive_connection_state(ConnectionLifecycle{
      .upstream_connecting = upstream_connecting_,
      .client_read_open = client_read_open_,
      .upstream_read_open = upstream_read_open_,
  });
}

std::string Relay::close_detail() const {
  const auto now = BackpressureTracker::Clock::now();
  const auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - accepted_at_)
          .count();
  std::string detail;
  detail += ",duration_ms=" + std::to_string(duration);
  detail += ",client_read=" + std::to_string(metrics_.client_bytes_read);
  detail += ",upstream_read=" + std::to_string(metrics_.upstream_bytes_read);
  detail += ",client_written=" + std::to_string(metrics_.client_bytes_written);
  detail += ",upstream_written=" + std::to_string(metrics_.upstream_bytes_written);
  detail += ",read_operations=" + std::to_string(metrics_.read_operations);
  detail += ",write_operations=" + std::to_string(metrics_.write_operations);
  detail += ",partial_writes=" + std::to_string(metrics_.partial_writes);
  detail += ",eagain_events=" + std::to_string(metrics_.eagain_events);
  detail += ",rejected_bytes=0";
  detail += ",c2u_high_water=" + std::to_string(client_to_upstream_.queue.high_water_mark());
  detail += ",u2c_high_water=" + std::to_string(upstream_to_client_.queue.high_water_mark());
  const auto append_backpressure = [&](std::string_view prefix, const BackpressureTracker& tracker) {
    detail += "," + std::string{prefix} + "_pause_count=" + std::to_string(tracker.pause_count());
    detail += "," + std::string{prefix} + "_resume_count=" + std::to_string(tracker.resume_count());
    detail += "," + std::string{prefix} + "_saturation_count=" + std::to_string(tracker.saturation_count());
    detail += "," + std::string{prefix} + "_paused_us=" +
              std::to_string(
                  std::chrono::duration_cast<std::chrono::microseconds>(tracker.paused_duration(now)).count());
  };
  append_backpressure("c2u", client_to_upstream_.backpressure);
  append_backpressure("u2c", upstream_to_client_.backpressure);
  return detail;
}

}  // namespace netfault
