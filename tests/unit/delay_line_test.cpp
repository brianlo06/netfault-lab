#include "netfault/delay_line.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>

namespace {

using namespace std::chrono_literals;

std::chrono::steady_clock::time_point base() {
  return std::chrono::steady_clock::time_point{} + 1000s;
}

}  // namespace

TEST_CASE("delay line releases bytes only after their eligibility time") {
  netfault::DelayLine line;
  line.on_commit(100, base() + 50ms);

  CHECK(line.releasable(base()) == 0);
  CHECK(line.releasable(base() + 49ms) == 0);
  CHECK(line.releasable(base() + 50ms) == 100);
  CHECK(line.pending() == 100);
  REQUIRE(line.next_eligible(base()) == base() + 50ms);
  CHECK_FALSE(line.next_eligible(base() + 50ms).has_value());
}

TEST_CASE("delay line releases segments front to back") {
  netfault::DelayLine line;
  line.on_commit(100, base() + 10ms);
  line.on_commit(200, base() + 20ms);
  line.on_commit(300, base() + 30ms);

  CHECK(line.releasable(base() + 15ms) == 100);
  CHECK(line.releasable(base() + 25ms) == 300);
  CHECK(line.releasable(base() + 30ms) == 600);

  line.on_consume(150);  // 100 from the first segment, 50 from the second
  CHECK(line.pending() == 450);
  CHECK(line.releasable(base() + 25ms) == 150);
  REQUIRE(line.next_eligible(base() + 25ms) == base() + 30ms);
}

TEST_CASE("delay line clamps eligibility monotonically so bytes cannot reorder") {
  netfault::DelayLine line;
  line.on_commit(100, base() + 50ms);
  // Negative jitter produced an earlier eligibility for later bytes; the line
  // must clamp it to the previous segment's time.
  line.on_commit(200, base() + 10ms);

  CHECK(line.releasable(base() + 20ms) == 0);
  CHECK(line.releasable(base() + 50ms) == 300);
}

TEST_CASE("delay line merges same-eligibility commits") {
  netfault::DelayLine line;
  line.on_commit(100, base() + 10ms);
  line.on_commit(200, base() + 10ms);
  CHECK(line.releasable(base() + 10ms) == 300);
  line.on_consume(300);
  CHECK(line.pending() == 0);
}

TEST_CASE("delay line rejects consuming more than pending") {
  netfault::DelayLine line;
  line.on_commit(10, base());
  CHECK_THROWS(line.on_consume(11));
}
