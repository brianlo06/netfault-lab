#include "netfault/endpoint.hpp"

#include <catch2/catch_test_macros.hpp>

TEST_CASE("Endpoint parses numeric IPv4 and detects loopback range") {
  const auto endpoint = netfault::Endpoint::parse("127.20.30.40:8080");
  CHECK(endpoint.host == "127.20.30.40");
  CHECK(endpoint.port == 8080);
  CHECK(endpoint.is_loopback());
  CHECK(endpoint.to_string() == "127.20.30.40:8080");
}

TEST_CASE("Endpoint rejects DNS names and invalid ports") {
  CHECK_THROWS_AS(netfault::Endpoint::parse("localhost:8080"), std::invalid_argument);
  CHECK_THROWS_AS(netfault::Endpoint::parse("127.0.0.1:0"), std::invalid_argument);
  CHECK_THROWS_AS(netfault::Endpoint::parse("127.0.0.1:70000"), std::invalid_argument);
  CHECK_THROWS_AS(netfault::Endpoint::parse("127.0.0.1"), std::invalid_argument);
}
