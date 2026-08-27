#include "netfault/relay.hpp"

#include <errno.h>

#include <algorithm>
#include <cstring>

namespace netfault {
namespace {

std::optional<Relay::TimePoint> earlier(std::optional<Relay::TimePoint> left,
                                        std::optional<Relay::TimePoint> right) {
  if (!left) {
    return right;
  }
  if (!right) {
    return left;
  }
  return std::min(*left, *right);
}

}  // namespace

Relay::Relay(const RelayConfig& config, const FaultPlan& faults, std::uint64_t connection_id,
             int client_fd, int upstream_fd, bool upstream_connecting, SocketIo& io,
             RelayObserver& observer, TimePoint now)
    : client_fd_(client_fd),
      upstream_fd_(upstream_fd),
      upstream_connecting_(upstream_connecting),
      state_(upstream_connecting ? ConnectionState::ConnectingUpstream : ConnectionState::Active),
      client_to_upstream_(config.buffer_bytes_per_direction, config.low_water_bytes, config.high_water_bytes),
      upstream_to_client_(config.buffer_bytes_per_direction, config.low_water_bytes, config.high_water_bytes),
      io_(io),
      observer_(observer),
      accepted_at_(now),
      last_activity_(now) {
  if (!faults.any_enabled()) {
    return;
  }
  faults_active_ = true;
  reset_after_bytes_ = faults.reset_after_bytes;
  half_close_after_bytes_ = faults.half_close_after_bytes;

  const auto connection_seed = derive_connection_seed(faults.master_seed, connection_id);
  const bool paced = faults.latency.count() > 0 || faults.jitter.count() > 0 ||
                     faults.rate_bytes_per_second > 0;
  const auto make_channel = [&](Direction& direction, std::uint64_t direction_index) {
    FaultChannel channel{.delay = std::nullopt,
                         .bucket = std::nullopt,
                         .rng = SplitMix64{derive_direction_seed(connection_seed, direction_index)},
                         .latency = std::chrono::duration_cast<std::chrono::nanoseconds>(faults.latency),
                         .jitter = std::chrono::duration_cast<std::chrono::nanoseconds>(faults.jitter),
                         .delayed_segments = 0,
                         .delay_budget = {}};
    if (faults.latency.count() > 0 || faults.jitter.count() > 0) {
      channel.delay.emplace();
    }
    if (faults.rate_bytes_per_second > 0) {
      channel.bucket.emplace(faults.rate_bytes_per_second, faults.effective_burst_bytes(), now);
    }
    direction.fault.emplace(std::move(channel));
  };
  if (paced && faults.applies_to_client_to_upstream()) {
    make_channel(client_to_upstream_, 0);
  }
  if (paced && faults.applies_to_upstream_to_client()) {
    make_channel(upstream_to_client_, 1);
  }
}

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

void Relay::stamp_ingress(Direction& direction, std::size_t bytes, TimePoint now) {
  if (!direction.fault || !direction.fault->delay) {
    return;
  }
  FaultChannel& channel = *direction.fault;
  auto delay = channel.latency + sample_jitter(channel.rng, channel.jitter);
  if (delay < std::chrono::nanoseconds::zero()) {
    delay = std::chrono::nanoseconds::zero();
  }
  channel.delay->on_commit(bytes, now + delay);
  ++channel.delayed_segments;
  channel.delay_budget += delay;
}

std::size_t Relay::fault_sendable(Direction& direction, std::size_t limit, TimePoint now) {
  if (!direction.fault) {
    return limit;
  }
  FaultChannel& channel = *direction.fault;
  if (channel.delay) {
    limit = std::min(limit, channel.delay->releasable(now));
  }
  if (channel.bucket) {
    limit = std::min(limit, channel.bucket->available(now));
  }
  return limit;
}

PumpResult Relay::handle_readable(Side source, bool hangup_hint, TimePoint now) {
  Direction& direction = direction_from(source);

  while (read_open(source) && !direction.backpressure.read_paused()) {
    const auto available_to_high_water = direction.backpressure.high_water_bytes() - direction.queue.size();
    const auto full_writable_span = direction.queue.writable_span();
    const auto writable = full_writable_span.first(std::min(full_writable_span.size(), available_to_high_water));
    const auto result = io_.receive(fd_for(source), writable);
    if (result.status == IoStatus::Transferred) {
      direction.queue.commit_write(result.bytes);
      stamp_ingress(direction, result.bytes, now);
      last_activity_ = now;
      ++metrics_.read_operations;
      if (source == Side::Client) {
        metrics_.client_bytes_read += static_cast<std::uint64_t>(result.bytes);
      } else {
        metrics_.upstream_bytes_read += static_cast<std::uint64_t>(result.bytes);
      }
      observe_backpressure(source, now);
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

PumpResult Relay::flush(Side destination, TimePoint now) {
  const Side source = destination == Side::Client ? Side::Upstream : Side::Client;
  Direction& direction = direction_from(source);
  if (!write_open(destination) || (destination == Side::Upstream && upstream_connecting_)) {
    return {};
  }

  while (!direction.queue.empty()) {
    const auto readable = direction.queue.readable_span();
    const auto budget = fault_sendable(direction, readable.size(), now);
    if (budget == 0) {
      break;
    }
    const auto result = io_.send(fd_for(destination), readable.first(budget));
    if (result.status == IoStatus::Transferred) {
      if (result.bytes < budget) {
        ++metrics_.partial_writes;
      }
      direction.queue.consume(result.bytes);
      if (direction.fault) {
        if (direction.fault->delay) {
          direction.fault->delay->on_consume(result.bytes);
        }
        if (direction.fault->bucket) {
          direction.fault->bucket->consume(result.bytes);
        }
      }
      observe_backpressure(source, now);
      last_activity_ = now;
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
  return check_lifecycle_faults();
}

PumpResult Relay::check_lifecycle_faults() {
  const auto total_written = metrics_.client_bytes_written + metrics_.upstream_bytes_written;
  if (half_close_after_bytes_ > 0 && !half_close_injected_ && client_write_open_ &&
      metrics_.client_bytes_written >= half_close_after_bytes_) {
    half_close_injected_ = true;
    if (io_.shutdown_write(client_fd_)) {
      client_write_open_ = false;
      observer_.on_event("fault_injected", state_,
                         "fault=half_close,direction=client_write,after_bytes=" +
                             std::to_string(metrics_.client_bytes_written));
    }
  }
  if (reset_after_bytes_ > 0 && total_written >= reset_after_bytes_) {
    observer_.on_event("fault_injected", state_,
                       "fault=reset,after_bytes=" + std::to_string(total_written));
    return {PumpStatus::CloseConnection, ConnectionState::Reset, "fault_reset"};
  }
  return {};
}

void Relay::observe_backpressure(Side source, TimePoint now) {
  Direction& direction = direction_from(source);
  const auto transition = direction.backpressure.observe(direction.queue.size(), direction.queue.capacity(), now);
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

std::optional<Relay::TimePoint> Relay::next_wake(Side destination, TimePoint now) {
  const Side source = destination == Side::Client ? Side::Upstream : Side::Client;
  Direction& direction = direction_from(source);
  if (!write_open(destination) || (destination == Side::Upstream && upstream_connecting_) ||
      direction.queue.empty() || !direction.fault) {
    return std::nullopt;
  }
  if (fault_sendable(direction, direction.queue.size(), now) > 0) {
    return std::nullopt;  // progress needs only EPOLLOUT
  }
  FaultChannel& channel = *direction.fault;
  std::optional<TimePoint> wake;
  std::size_t releasable = direction.queue.size();
  if (channel.delay) {
    releasable = channel.delay->releasable(now);
    if (releasable == 0) {
      wake = channel.delay->next_eligible(now);
    }
  }
  if (channel.bucket && releasable > 0) {
    wake = earlier(wake, channel.bucket->eligible_at(releasable, now));
  }
  return wake;
}

bool Relay::fully_drained() const noexcept {
  const bool client_to_upstream_done = client_to_upstream_.queue.empty() || !upstream_write_open_;
  const bool upstream_to_client_done = upstream_to_client_.queue.empty() || !client_write_open_;
  return !client_read_open_ && !upstream_read_open_ && client_to_upstream_done && upstream_to_client_done;
}

InterestSet Relay::desired_interest(Side side, TimePoint now) {
  if (side == Side::Upstream && upstream_connecting_) {
    return {.read = false, .write = true};
  }
  const Side inbound_source = side;
  const Side outbound_source = side == Side::Client ? Side::Upstream : Side::Client;
  Direction& outbound = direction_from(outbound_source);
  const bool side_read_open = side == Side::Client ? client_read_open_ : upstream_read_open_;
  const bool side_write_open = side == Side::Client ? client_write_open_ : upstream_write_open_;
  return {
      .read = side_read_open && !direction_from(inbound_source).backpressure.read_paused(),
      .write = side_write_open && !outbound.queue.empty() &&
               fault_sendable(outbound, outbound.queue.size(), now) > 0,
  };
}

void Relay::refresh_state() noexcept {
  state_ = derive_connection_state(ConnectionLifecycle{
      .upstream_connecting = upstream_connecting_,
      .client_read_open = client_read_open_,
      .upstream_read_open = upstream_read_open_,
  });
}

std::string Relay::close_detail(TimePoint now) const {
  const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - accepted_at_).count();
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
  detail += ",faults_applied=" + std::to_string(faults_active_ ? 1 : 0);
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
  const auto append_fault = [&](std::string_view prefix, const Direction& direction) {
    const auto segments = direction.fault ? direction.fault->delayed_segments : 0;
    const auto budget_us =
        direction.fault
            ? std::chrono::duration_cast<std::chrono::microseconds>(direction.fault->delay_budget).count()
            : 0;
    detail += "," + std::string{prefix} + "_delayed_segments=" + std::to_string(segments);
    detail += "," + std::string{prefix} + "_delay_budget_us=" + std::to_string(budget_us);
  };
  append_fault("c2u", client_to_upstream_);
  append_fault("u2c", upstream_to_client_);
  return detail;
}

std::string Relay::metrics_json(TimePoint now) const {
  const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - accepted_at_).count();
  std::string json = "{";
  const auto field = [&json](std::string_view name, std::uint64_t value, bool first = false) {
    if (!first) {
      json += ",";
    }
    json += "\"" + std::string{name} + "\":" + std::to_string(value);
  };
  json += "\"state\":\"" + std::string{state_name(state_)} + "\"";
  field("duration_ms", static_cast<std::uint64_t>(duration));
  field("client_bytes_read", metrics_.client_bytes_read);
  field("upstream_bytes_read", metrics_.upstream_bytes_read);
  field("client_bytes_written", metrics_.client_bytes_written);
  field("upstream_bytes_written", metrics_.upstream_bytes_written);
  field("read_operations", metrics_.read_operations);
  field("write_operations", metrics_.write_operations);
  field("partial_writes", metrics_.partial_writes);
  field("eagain_events", metrics_.eagain_events);
  field("faults_applied", faults_active_ ? 1 : 0);
  const auto append_direction = [&](std::string_view prefix, const Direction& direction) {
    const auto& tracker = direction.backpressure;
    field(std::string{prefix} + "_queue_bytes", direction.queue.size());
    field(std::string{prefix} + "_high_water", direction.queue.high_water_mark());
    field(std::string{prefix} + "_pause_count", tracker.pause_count());
    field(std::string{prefix} + "_resume_count", tracker.resume_count());
    field(std::string{prefix} + "_saturation_count", tracker.saturation_count());
    field(std::string{prefix} + "_paused_us",
          static_cast<std::uint64_t>(
              std::chrono::duration_cast<std::chrono::microseconds>(tracker.paused_duration(now)).count()));
    field(std::string{prefix} + "_delayed_segments",
          direction.fault ? direction.fault->delayed_segments : 0);
    field(std::string{prefix} + "_delay_budget_us",
          direction.fault
              ? static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                               direction.fault->delay_budget)
                                               .count())
              : 0);
  };
  append_direction("c2u", client_to_upstream_);
  append_direction("u2c", upstream_to_client_);
  json += "}";
  return json;
}

}  // namespace netfault
