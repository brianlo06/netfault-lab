#include "netfault/timer_queue.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <vector>

namespace {

using namespace std::chrono_literals;

std::chrono::steady_clock::time_point base() {
  return std::chrono::steady_clock::time_point{} + 1000s;
}

}  // namespace

TEST_CASE("timer queue pops events in deadline order") {
  netfault::TimerQueue queue;
  queue.schedule(base() + 30ms, 3, netfault::TimerKind::Pump);
  queue.schedule(base() + 10ms, 1, netfault::TimerKind::IdleTimeout);
  queue.schedule(base() + 20ms, 2, netfault::TimerKind::ConnectTimeout);

  REQUIRE(queue.next_deadline() == base() + 10ms);

  std::vector<std::uint64_t> order;
  while (auto event = queue.pop_due(base() + 1s)) {
    order.push_back(event->connection_id);
  }
  CHECK(order == std::vector<std::uint64_t>{1, 2, 3});
  CHECK(queue.empty());
}

TEST_CASE("timer queue breaks equal deadlines by schedule order") {
  netfault::TimerQueue queue;
  for (std::uint64_t id = 1; id <= 5; ++id) {
    queue.schedule(base() + 50ms, id, netfault::TimerKind::Pump);
  }
  std::vector<std::uint64_t> order;
  while (auto event = queue.pop_due(base() + 50ms)) {
    order.push_back(event->connection_id);
  }
  CHECK(order == std::vector<std::uint64_t>{1, 2, 3, 4, 5});
}

TEST_CASE("timer queue does not pop future events") {
  netfault::TimerQueue queue;
  queue.schedule(base() + 100ms, 1, netfault::TimerKind::IdleTimeout);

  CHECK_FALSE(queue.pop_due(base() + 99ms).has_value());
  CHECK(queue.size() == 1);

  const auto event = queue.pop_due(base() + 100ms);
  REQUIRE(event.has_value());
  CHECK(event->connection_id == 1);
  CHECK(event->kind == netfault::TimerKind::IdleTimeout);
  CHECK_FALSE(queue.next_deadline().has_value());
}
