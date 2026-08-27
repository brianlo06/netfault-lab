#pragma once

#include <string_view>

namespace netfault {

enum class ConnectionState {
  ConnectingUpstream,
  Active,
  ClientReadClosed,
  UpstreamReadClosed,
  Draining,
  FullyClosed,
  Reset,
  Failed,
  TimedOut,
};

struct ConnectionLifecycle {
  bool upstream_connecting{false};
  bool client_read_open{true};
  bool upstream_read_open{true};
};

[[nodiscard]] constexpr ConnectionState derive_connection_state(ConnectionLifecycle lifecycle) noexcept {
  if (lifecycle.upstream_connecting) {
    return ConnectionState::ConnectingUpstream;
  }
  if (!lifecycle.client_read_open && !lifecycle.upstream_read_open) {
    return ConnectionState::Draining;
  }
  if (!lifecycle.client_read_open) {
    return ConnectionState::ClientReadClosed;
  }
  if (!lifecycle.upstream_read_open) {
    return ConnectionState::UpstreamReadClosed;
  }
  return ConnectionState::Active;
}

[[nodiscard]] constexpr std::string_view state_name(ConnectionState state) noexcept {
  switch (state) {
    case ConnectionState::ConnectingUpstream:
      return "connecting_upstream";
    case ConnectionState::Active:
      return "active";
    case ConnectionState::ClientReadClosed:
      return "client_read_closed";
    case ConnectionState::UpstreamReadClosed:
      return "upstream_read_closed";
    case ConnectionState::Draining:
      return "draining";
    case ConnectionState::FullyClosed:
      return "fully_closed";
    case ConnectionState::Reset:
      return "reset";
    case ConnectionState::Failed:
      return "failed";
    case ConnectionState::TimedOut:
      return "timed_out";
  }
  return "unknown";
}

}  // namespace netfault
