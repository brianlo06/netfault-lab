#include "netfault/backpressure.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>

TEST_CASE("BackpressureTracker applies high-low watermark hysteresis") {
  using namespace std::chrono_literals;
  netfault::BackpressureTracker tracker{4, 8};
  const auto start = netfault::BackpressureTracker::TimePoint{};

  CHECK(tracker.observe(7, 10, start) == netfault::BackpressureTransition::None);
  CHECK(tracker.observe(8, 10, start + 1ms) == netfault::BackpressureTransition::Paused);
  CHECK(tracker.read_paused());
  CHECK(tracker.observe(5, 10, start + 2ms) == netfault::BackpressureTransition::None);
  CHECK(tracker.observe(4, 10, start + 3ms) == netfault::BackpressureTransition::Resumed);
  CHECK_FALSE(tracker.read_paused());
  CHECK(tracker.pause_count() == 1);
  CHECK(tracker.resume_count() == 1);
  CHECK(tracker.paused_duration(start + 4ms) == 2ms);
}

TEST_CASE("BackpressureTracker counts distinct capacity saturation episodes") {
  netfault::BackpressureTracker tracker{2, 5};
  const auto now = netfault::BackpressureTracker::TimePoint{};

  static_cast<void>(tracker.observe(5, 5, now));
  static_cast<void>(tracker.observe(5, 5, now));
  static_cast<void>(tracker.observe(4, 5, now));
  static_cast<void>(tracker.observe(5, 5, now));
  CHECK(tracker.saturation_count() == 2);
}

TEST_CASE("BackpressureTracker validates configuration and occupancy") {
  CHECK_THROWS_AS(netfault::BackpressureTracker(4, 4), std::invalid_argument);
  netfault::BackpressureTracker tracker{2, 4};
  CHECK_THROWS_AS(tracker.observe(5, 4, {}), std::out_of_range);
  CHECK_THROWS_AS(tracker.observe(3, 3, {}), std::out_of_range);
}
