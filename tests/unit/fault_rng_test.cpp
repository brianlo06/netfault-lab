#include "netfault/fault_rng.hpp"

#include "netfault/fault_plan.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <set>

namespace {

using namespace std::chrono_literals;

}  // namespace

TEST_CASE("seed derivation is deterministic and distinguishes inputs") {
  const auto seed_a = netfault::derive_connection_seed(42, 1);
  const auto seed_b = netfault::derive_connection_seed(42, 1);
  CHECK(seed_a == seed_b);
  CHECK(seed_a != netfault::derive_connection_seed(42, 2));
  CHECK(seed_a != netfault::derive_connection_seed(43, 1));

  const auto direction_0 = netfault::derive_direction_seed(seed_a, 0);
  const auto direction_1 = netfault::derive_direction_seed(seed_a, 1);
  CHECK(direction_0 != direction_1);
  CHECK(direction_0 == netfault::derive_direction_seed(seed_a, 0));
}

TEST_CASE("splitmix64 streams from equal seeds are identical") {
  netfault::SplitMix64 first{7};
  netfault::SplitMix64 second{7};
  for (int index = 0; index < 100; ++index) {
    REQUIRE(first.next() == second.next());
  }
}

TEST_CASE("unit interval samples stay in range") {
  netfault::SplitMix64 rng{123};
  for (int index = 0; index < 1'000; ++index) {
    const auto sample = netfault::sample_unit_interval(rng.next());
    REQUIRE(sample >= 0.0);
    REQUIRE(sample < 1.0);
  }
}

TEST_CASE("jitter samples are bounded and cover both signs") {
  netfault::SplitMix64 rng{99};
  const auto jitter = std::chrono::nanoseconds{5ms};
  bool saw_negative = false;
  bool saw_positive = false;
  for (int index = 0; index < 1'000; ++index) {
    const auto sample = netfault::sample_jitter(rng, jitter);
    REQUIRE(sample >= -jitter);
    REQUIRE(sample <= jitter);
    saw_negative = saw_negative || sample < std::chrono::nanoseconds::zero();
    saw_positive = saw_positive || sample > std::chrono::nanoseconds::zero();
  }
  CHECK(saw_negative);
  CHECK(saw_positive);
}

TEST_CASE("zero jitter always samples zero without consuming randomness") {
  netfault::SplitMix64 rng{5};
  netfault::SplitMix64 untouched{5};
  CHECK(netfault::sample_jitter(rng, std::chrono::nanoseconds::zero()) == std::chrono::nanoseconds::zero());
  CHECK(rng.next() == untouched.next());
}

TEST_CASE("fault plan validation enforces limits") {
  netfault::FaultPlan plan;
  CHECK_NOTHROW(plan.validate());
  CHECK_FALSE(plan.any_enabled());

  plan.latency = 20ms;
  plan.jitter = 5ms;
  plan.rate_bytes_per_second = 10'000;
  CHECK_NOTHROW(plan.validate());
  CHECK(plan.any_enabled());
  CHECK(plan.effective_burst_bytes() == 10'000);

  netfault::FaultPlan bad = plan;
  bad.apply_probability = 1.5;
  CHECK_THROWS(bad.validate());

  bad = plan;
  bad.rate_bytes_per_second = 0;
  bad.burst_bytes = 100;
  CHECK_THROWS(bad.validate());

  bad = plan;
  bad.latency = 59s;
  bad.jitter = 2s;
  CHECK_THROWS(bad.validate());
}

TEST_CASE("fault plan direction selection") {
  netfault::FaultPlan plan;
  CHECK(plan.applies_to_client_to_upstream());
  CHECK(plan.applies_to_upstream_to_client());

  plan.directions = netfault::FaultDirections::ClientToUpstream;
  CHECK(plan.applies_to_client_to_upstream());
  CHECK_FALSE(plan.applies_to_upstream_to_client());

  plan.directions = netfault::FaultDirections::UpstreamToClient;
  CHECK_FALSE(plan.applies_to_client_to_upstream());
  CHECK(plan.applies_to_upstream_to_client());
}
