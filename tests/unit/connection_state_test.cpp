#include "netfault/connection_state.hpp"

#include <catch2/catch_test_macros.hpp>

TEST_CASE("Connection lifecycle derives explicit forwarding states") {
  using netfault::ConnectionLifecycle;
  using netfault::ConnectionState;
  using netfault::derive_connection_state;

  CHECK(derive_connection_state({.upstream_connecting = true}) == ConnectionState::ConnectingUpstream);
  CHECK(derive_connection_state({}) == ConnectionState::Active);
  CHECK(derive_connection_state({.client_read_open = false}) == ConnectionState::ClientReadClosed);
  CHECK(derive_connection_state({.upstream_read_open = false}) == ConnectionState::UpstreamReadClosed);
  CHECK(derive_connection_state({.client_read_open = false, .upstream_read_open = false}) ==
        ConnectionState::Draining);
}

TEST_CASE("Connection states have stable log names") {
  CHECK(netfault::state_name(netfault::ConnectionState::ConnectingUpstream) == "connecting_upstream");
  CHECK(netfault::state_name(netfault::ConnectionState::FullyClosed) == "fully_closed");
  CHECK(netfault::state_name(netfault::ConnectionState::Reset) == "reset");
  CHECK(netfault::state_name(netfault::ConnectionState::Failed) == "failed");
}
