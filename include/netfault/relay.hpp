#pragma once

#include "netfault/backpressure.hpp"
#include "netfault/byte_queue.hpp"
#include "netfault/connection_state.hpp"
#include "netfault/delay_line.hpp"
#include "netfault/fault_plan.hpp"
#include "netfault/fault_rng.hpp"
#include "netfault/socket_io.hpp"
#include "netfault/token_bucket.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace netfault {

enum class Side { Client, Upstream };

[[nodiscard]] constexpr std::string_view side_name(Side side) noexcept {
  return side == Side::Client ? "client" : "upstream";
}

struct RelayConfig {
  std::size_t buffer_bytes_per_direction{65'536};
  std::size_t low_water_bytes{32'768};
  std::size_t high_water_bytes{65'536};
};

// Receives relay lifecycle events for logging. Implementations must not block
// and must not call back into the relay.
class RelayObserver {
 public:
  RelayObserver() = default;
  RelayObserver(const RelayObserver&) = delete;
  RelayObserver& operator=(const RelayObserver&) = delete;
  virtual ~RelayObserver() = default;

  virtual void on_event(std::string_view event, ConnectionState state, std::string_view detail) = 0;
};

enum class PumpStatus { Continue, CloseConnection };

struct PumpResult {
  PumpStatus status{PumpStatus::Continue};
  ConnectionState final_state{ConnectionState::FullyClosed};
  std::string reason;
};

struct InterestSet {
  bool read{false};
  bool write{false};
};

struct RelayMetrics {
  std::uint64_t client_bytes_read{0};
  std::uint64_t upstream_bytes_read{0};
  std::uint64_t client_bytes_written{0};
  std::uint64_t upstream_bytes_written{0};
  std::uint64_t read_operations{0};
  std::uint64_t write_operations{0};
  std::uint64_t partial_writes{0};
  std::uint64_t eagain_events{0};
};

// Directional forwarding engine for one proxied connection: owns the two byte
// queues, backpressure trackers, half-close flags, byte accounting, and any
// configured fault state (latency/jitter delay lines, token buckets, lifecycle
// triggers). It never owns file descriptors, never touches epoll, and never
// reads a clock — every method takes the current time, so fault behavior is
// testable without sleeping. Socket access goes through SocketIo, and close
// decisions are returned to the caller instead of executed.
class Relay {
 public:
  using TimePoint = std::chrono::steady_clock::time_point;

  Relay(const RelayConfig& config, const FaultPlan& faults, std::uint64_t connection_id,
        int client_fd, int upstream_fd, bool upstream_connecting, SocketIo& io,
        RelayObserver& observer, TimePoint now);

  [[nodiscard]] ConnectionState state() const noexcept { return state_; }
  [[nodiscard]] bool upstream_connecting() const noexcept { return upstream_connecting_; }
  void mark_upstream_connected();

  // Drains the readable socket into its destination queue until EAGAIN, EOF,
  // the high-water mark, or an error. hangup_hint marks EPOLLRDHUP/EPOLLHUP
  // delivery so an EAGAIN after hangup is treated as end of stream. Ingress
  // bytes are stamped with their fault eligibility time.
  [[nodiscard]] PumpResult handle_readable(Side source, bool hangup_hint, TimePoint now);

  // Writes queued fault-eligible bytes toward the destination socket until the
  // queue empties, the fault budget runs out, EAGAIN, or an error. Partial
  // kernel writes are counted and retried. Lifecycle fault triggers (reset,
  // injected half-close) fire here because forwarded-byte counts change here.
  [[nodiscard]] PumpResult flush(Side destination, TimePoint now);

  // Forwards observed half-closes once the corresponding queue has drained.
  void propagate_half_closes();

  // Earliest time a currently fault-blocked destination could make progress.
  // nullopt when progress needs only socket writability (EPOLLOUT covers it).
  [[nodiscard]] std::optional<TimePoint> next_wake(Side destination, TimePoint now);

  [[nodiscard]] bool fully_drained() const noexcept;
  [[nodiscard]] InterestSet desired_interest(Side side, TimePoint now);
  [[nodiscard]] const RelayMetrics& metrics() const noexcept { return metrics_; }
  [[nodiscard]] TimePoint last_activity() const noexcept { return last_activity_; }
  [[nodiscard]] bool faults_active() const noexcept { return faults_active_; }

  // Key/value counters appended to the connection_closed log event.
  [[nodiscard]] std::string close_detail(TimePoint now) const;

  // The same counters as a JSON object body (without braces' surrounding
  // whitespace concerns): all values numeric or fixed identifiers, no payload
  // data, suitable for the exported metrics document.
  [[nodiscard]] std::string metrics_json(TimePoint now) const;

 private:
  struct FaultChannel {
    std::optional<DelayLine> delay;
    std::optional<TokenBucket> bucket;
    SplitMix64 rng;
    std::chrono::nanoseconds latency{};
    std::chrono::nanoseconds jitter{};
    std::uint64_t delayed_segments{0};
    std::chrono::nanoseconds delay_budget{};
  };

  struct Direction {
    Direction(std::size_t capacity, std::size_t low_water, std::size_t high_water)
        : queue(capacity), backpressure(low_water, high_water) {}
    ByteQueue queue;
    BackpressureTracker backpressure;
    std::optional<FaultChannel> fault;
  };

  [[nodiscard]] int fd_for(Side side) const noexcept { return side == Side::Client ? client_fd_ : upstream_fd_; }
  [[nodiscard]] Direction& direction_from(Side source) noexcept;
  [[nodiscard]] const Direction& direction_from(Side source) const noexcept;
  [[nodiscard]] bool& read_open(Side side) noexcept;
  [[nodiscard]] bool& write_open(Side side) noexcept;
  void note_half_closed_read(Side source);
  void observe_backpressure(Side source, TimePoint now);
  void stamp_ingress(Direction& direction, std::size_t bytes, TimePoint now);
  [[nodiscard]] std::size_t fault_sendable(Direction& direction, std::size_t limit, TimePoint now);
  [[nodiscard]] PumpResult check_lifecycle_faults();
  void refresh_state() noexcept;
  [[nodiscard]] static PumpResult close_for_error(int error, std::string_view operation);

  int client_fd_;
  int upstream_fd_;
  bool upstream_connecting_;
  bool client_read_open_{true};
  bool client_write_open_{true};
  bool upstream_read_open_{true};
  bool upstream_write_open_{true};
  bool faults_active_{false};
  std::uint64_t reset_after_bytes_{0};
  std::uint64_t half_close_after_bytes_{0};
  bool half_close_injected_{false};
  ConnectionState state_;
  Direction client_to_upstream_;
  Direction upstream_to_client_;
  RelayMetrics metrics_;
  SocketIo& io_;
  RelayObserver& observer_;
  TimePoint accepted_at_;
  TimePoint last_activity_;
};

}  // namespace netfault
