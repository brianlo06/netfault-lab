#include "netfault/relay.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace {

using namespace std::chrono_literals;

constexpr int kClientFd = 100;
constexpr int kUpstreamFd = 200;

std::chrono::steady_clock::time_point base() {
  return std::chrono::steady_clock::time_point{} + 1000s;
}

struct RecordedEvent {
  std::string event;
  netfault::ConnectionState state;
  std::string detail;
};

class RecordingObserver final : public netfault::RelayObserver {
 public:
  void on_event(std::string_view event, netfault::ConnectionState state, std::string_view detail) override {
    events.push_back({std::string{event}, state, std::string{detail}});
  }

  [[nodiscard]] std::size_t count(std::string_view event_name) const {
    return static_cast<std::size_t>(
        std::count_if(events.begin(), events.end(),
                      [&](const RecordedEvent& event) { return event.event == event_name; }));
  }

  std::vector<RecordedEvent> events;
};

// Scripted SocketIo: each fd has a queue of receive results and a rule for how
// many bytes a send may transfer per call, so partial writes and EAGAIN are
// forced deterministically.
class FakeSocketIo final : public netfault::SocketIo {
 public:
  struct ReceiveStep {
    netfault::IoStatus status{netfault::IoStatus::WouldBlock};
    std::vector<std::byte> payload;
    int error{0};
  };

  struct SendStep {
    netfault::IoStatus status{netfault::IoStatus::Transferred};
    std::size_t max_bytes{SIZE_MAX};
    int error{0};
  };

  netfault::IoResult receive(int fd, std::span<std::byte> buffer) override {
    auto& steps = receive_steps[fd];
    if (steps.empty()) {
      return {netfault::IoStatus::WouldBlock, 0, 0};
    }
    ReceiveStep& step = steps.front();
    if (step.status != netfault::IoStatus::Transferred) {
      const auto result = netfault::IoResult{step.status, 0, step.error};
      steps.pop_front();
      return result;
    }
    const auto bytes = std::min(step.payload.size(), buffer.size());
    std::memcpy(buffer.data(), step.payload.data(), bytes);
    if (bytes == step.payload.size()) {
      steps.pop_front();
    } else {
      step.payload.erase(step.payload.begin(), step.payload.begin() + static_cast<std::ptrdiff_t>(bytes));
    }
    return {netfault::IoStatus::Transferred, bytes, 0};
  }

  netfault::IoResult send(int fd, std::span<const std::byte> data) override {
    auto& steps = send_steps[fd];
    if (steps.empty()) {
      auto& sink = sent[fd];
      sink.insert(sink.end(), data.begin(), data.end());
      return {netfault::IoStatus::Transferred, data.size(), 0};
    }
    const SendStep step = steps.front();
    steps.pop_front();
    if (step.status != netfault::IoStatus::Transferred) {
      return {step.status, 0, step.error};
    }
    const auto bytes = std::min(step.max_bytes, data.size());
    auto& sink = sent[fd];
    sink.insert(sink.end(), data.begin(), data.begin() + static_cast<std::ptrdiff_t>(bytes));
    return {netfault::IoStatus::Transferred, bytes, 0};
  }

  bool shutdown_write(int fd) override {
    shutdowns.push_back(fd);
    return true;
  }

  void push_receive_bytes(int fd, std::size_t count, std::byte fill = std::byte{0x5A}) {
    receive_steps[fd].push_back({netfault::IoStatus::Transferred, std::vector<std::byte>(count, fill), 0});
  }

  std::map<int, std::deque<ReceiveStep>> receive_steps;
  std::map<int, std::deque<SendStep>> send_steps;
  std::map<int, std::vector<std::byte>> sent;
  std::vector<int> shutdowns;
};

netfault::RelayConfig small_config() {
  return {.buffer_bytes_per_direction = 4'096, .low_water_bytes = 1'024, .high_water_bytes = 4'096};
}

struct Fixture {
  explicit Fixture(const netfault::RelayConfig& config = small_config(),
                   const netfault::FaultPlan& plan = {}, std::uint64_t connection_id = 1,
                   bool connecting = false)
      : relay(config, plan, connection_id, kClientFd, kUpstreamFd, connecting, io, observer, base()) {}

  FakeSocketIo io;
  RecordingObserver observer;
  netfault::Relay relay;
};

}  // namespace

TEST_CASE("relay forwards client bytes upstream with exact accounting") {
  Fixture fixture;

  fixture.io.push_receive_bytes(kClientFd, 1'000);
  REQUIRE(fixture.relay.handle_readable(netfault::Side::Client, false, base()).status ==
          netfault::PumpStatus::Continue);
  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base()).status == netfault::PumpStatus::Continue);

  CHECK(fixture.relay.metrics().client_bytes_read == 1'000);
  CHECK(fixture.relay.metrics().upstream_bytes_written == 1'000);
  CHECK(fixture.io.sent[kUpstreamFd].size() == 1'000);
  CHECK(fixture.relay.metrics().eagain_events == 1);  // the drain-to-EAGAIN read after the payload
}

TEST_CASE("relay retries forced partial writes until the queue drains") {
  Fixture fixture;

  fixture.io.push_receive_bytes(kClientFd, 3'000);
  // Every send call transfers at most 700 bytes, forcing positive short writes.
  for (int step = 0; step < 5; ++step) {
    fixture.io.send_steps[kUpstreamFd].push_back({netfault::IoStatus::Transferred, 700, 0});
  }

  REQUIRE(fixture.relay.handle_readable(netfault::Side::Client, false, base()).status ==
          netfault::PumpStatus::Continue);
  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base()).status == netfault::PumpStatus::Continue);

  CHECK(fixture.io.sent[kUpstreamFd].size() == 3'000);
  CHECK(fixture.relay.metrics().upstream_bytes_written == 3'000);
  CHECK(fixture.relay.metrics().partial_writes >= 4);  // 700-byte slices of a 3000-byte queue
  CHECK(fixture.relay.metrics().write_operations >= 5);
}

TEST_CASE("relay preserves queued bytes across send EAGAIN and requests write interest") {
  Fixture fixture;

  fixture.io.push_receive_bytes(kClientFd, 2'000);
  fixture.io.send_steps[kUpstreamFd].push_back({netfault::IoStatus::Transferred, 500, 0});
  fixture.io.send_steps[kUpstreamFd].push_back({netfault::IoStatus::WouldBlock, 0, 0});

  REQUIRE(fixture.relay.handle_readable(netfault::Side::Client, false, base()).status ==
          netfault::PumpStatus::Continue);
  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base()).status == netfault::PumpStatus::Continue);

  CHECK(fixture.io.sent[kUpstreamFd].size() == 500);
  CHECK(fixture.relay.desired_interest(netfault::Side::Upstream, base()).write);
  CHECK(fixture.relay.metrics().eagain_events >= 1);

  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base()).status == netfault::PumpStatus::Continue);
  CHECK(fixture.io.sent[kUpstreamFd].size() == 2'000);
  CHECK_FALSE(fixture.relay.desired_interest(netfault::Side::Upstream, base()).write);
}

TEST_CASE("relay forwards a half-close only after the queue drains") {
  Fixture fixture;

  fixture.io.push_receive_bytes(kClientFd, 1'500);
  fixture.io.receive_steps[kClientFd].push_back({netfault::IoStatus::PeerClosed, {}, 0});
  fixture.io.send_steps[kUpstreamFd].push_back({netfault::IoStatus::Transferred, 400, 0});
  fixture.io.send_steps[kUpstreamFd].push_back({netfault::IoStatus::WouldBlock, 0, 0});

  REQUIRE(fixture.relay.handle_readable(netfault::Side::Client, false, base()).status ==
          netfault::PumpStatus::Continue);
  CHECK(fixture.observer.count("peer_half_closed") == 1);
  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base()).status == netfault::PumpStatus::Continue);

  // 1100 bytes are still queued, so the half-close must not be forwarded yet.
  fixture.relay.propagate_half_closes();
  CHECK(fixture.io.shutdowns.empty());

  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base()).status == netfault::PumpStatus::Continue);
  fixture.relay.propagate_half_closes();
  REQUIRE(fixture.io.shutdowns == std::vector<int>{kUpstreamFd});
  CHECK(fixture.observer.count("half_close_forwarded") == 1);
  CHECK(fixture.io.sent[kUpstreamFd].size() == 1'500);
}

TEST_CASE("relay reports fully drained after both directions half-close") {
  Fixture fixture;

  fixture.io.push_receive_bytes(kClientFd, 100);
  fixture.io.receive_steps[kClientFd].push_back({netfault::IoStatus::PeerClosed, {}, 0});
  fixture.io.push_receive_bytes(kUpstreamFd, 100);
  fixture.io.receive_steps[kUpstreamFd].push_back({netfault::IoStatus::PeerClosed, {}, 0});

  REQUIRE(fixture.relay.handle_readable(netfault::Side::Client, false, base()).status ==
          netfault::PumpStatus::Continue);
  REQUIRE(fixture.relay.handle_readable(netfault::Side::Upstream, false, base()).status ==
          netfault::PumpStatus::Continue);
  CHECK_FALSE(fixture.relay.fully_drained());
  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base()).status == netfault::PumpStatus::Continue);
  REQUIRE(fixture.relay.flush(netfault::Side::Client, base()).status == netfault::PumpStatus::Continue);
  fixture.relay.propagate_half_closes();

  CHECK(fixture.relay.fully_drained());
  CHECK(fixture.relay.state() == netfault::ConnectionState::Draining);
  CHECK(fixture.io.shutdowns.size() == 2);
}

TEST_CASE("relay pauses reads at the high-water mark and resumes after draining") {
  const netfault::RelayConfig config{
      .buffer_bytes_per_direction = 2'048, .low_water_bytes = 512, .high_water_bytes = 2'048};
  Fixture fixture{config};

  fixture.io.push_receive_bytes(kClientFd, 8'192);
  REQUIRE(fixture.relay.handle_readable(netfault::Side::Client, false, base()).status ==
          netfault::PumpStatus::Continue);

  CHECK(fixture.observer.count("read_paused") == 1);
  CHECK_FALSE(fixture.relay.desired_interest(netfault::Side::Client, base()).read);
  CHECK(fixture.relay.metrics().client_bytes_read == 2'048);

  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base()).status == netfault::PumpStatus::Continue);
  CHECK(fixture.observer.count("read_resumed") == 1);
  CHECK(fixture.relay.desired_interest(netfault::Side::Client, base()).read);

  // Reads continue with the remaining scripted payload after the resume.
  for (int round = 0; round < 3; ++round) {
    REQUIRE(fixture.relay.handle_readable(netfault::Side::Client, false, base()).status ==
            netfault::PumpStatus::Continue);
    REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base()).status == netfault::PumpStatus::Continue);
  }
  CHECK(fixture.io.sent[kUpstreamFd].size() == 8'192);
}

TEST_CASE("relay maps receive ECONNRESET to a reset close request") {
  Fixture fixture;

  fixture.io.receive_steps[kClientFd].push_back({netfault::IoStatus::Error, {}, ECONNRESET});
  const auto result = fixture.relay.handle_readable(netfault::Side::Client, false, base());

  REQUIRE(result.status == netfault::PumpStatus::CloseConnection);
  CHECK(result.final_state == netfault::ConnectionState::Reset);
  CHECK(result.reason.starts_with("read="));
}

TEST_CASE("relay maps send EPIPE to a reset close request") {
  Fixture fixture;

  fixture.io.push_receive_bytes(kClientFd, 64);
  fixture.io.send_steps[kUpstreamFd].push_back({netfault::IoStatus::Error, 0, EPIPE});

  REQUIRE(fixture.relay.handle_readable(netfault::Side::Client, false, base()).status ==
          netfault::PumpStatus::Continue);
  const auto result = fixture.relay.flush(netfault::Side::Upstream, base());

  REQUIRE(result.status == netfault::PumpStatus::CloseConnection);
  CHECK(result.final_state == netfault::ConnectionState::Reset);
  CHECK(result.reason.starts_with("write="));
}

TEST_CASE("relay treats EAGAIN after a hangup hint as end of stream") {
  Fixture fixture;

  const auto result = fixture.relay.handle_readable(netfault::Side::Client, true, base());
  REQUIRE(result.status == netfault::PumpStatus::Continue);
  CHECK(fixture.observer.count("peer_half_closed") == 1);
  CHECK_FALSE(fixture.relay.desired_interest(netfault::Side::Client, base()).read);
}

TEST_CASE("relay suppresses reads and flushes while the upstream connect is pending") {
  Fixture fixture{small_config(), {}, 1, true};

  CHECK(fixture.relay.state() == netfault::ConnectionState::ConnectingUpstream);
  const auto interest = fixture.relay.desired_interest(netfault::Side::Upstream, base());
  CHECK_FALSE(interest.read);
  CHECK(interest.write);

  fixture.io.push_receive_bytes(kClientFd, 256);
  REQUIRE(fixture.relay.handle_readable(netfault::Side::Client, false, base()).status ==
          netfault::PumpStatus::Continue);
  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base()).status == netfault::PumpStatus::Continue);
  CHECK(fixture.io.sent[kUpstreamFd].empty());

  fixture.relay.mark_upstream_connected();
  CHECK(fixture.relay.state() == netfault::ConnectionState::Active);
  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base()).status == netfault::PumpStatus::Continue);
  CHECK(fixture.io.sent[kUpstreamFd].size() == 256);
}

TEST_CASE("relay close detail reports byte and backpressure counters") {
  Fixture fixture;

  fixture.io.push_receive_bytes(kClientFd, 512);
  REQUIRE(fixture.relay.handle_readable(netfault::Side::Client, false, base()).status ==
          netfault::PumpStatus::Continue);
  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base()).status == netfault::PumpStatus::Continue);

  const auto detail = fixture.relay.close_detail(base());
  CHECK(detail.find(",client_read=512") != std::string::npos);
  CHECK(detail.find(",upstream_written=512") != std::string::npos);
  CHECK(detail.find(",rejected_bytes=0") != std::string::npos);
  CHECK(detail.find(",faults_applied=0") != std::string::npos);
  CHECK(detail.find(",c2u_pause_count=0") != std::string::npos);
  CHECK(detail.find(",u2c_paused_us=") != std::string::npos);
  CHECK(detail.find(",c2u_delayed_segments=0") != std::string::npos);
}

TEST_CASE("relay latency holds bytes until eligibility and reports the wake time") {
  netfault::FaultPlan plan;
  plan.latency = 50ms;
  Fixture fixture{small_config(), plan};

  fixture.io.push_receive_bytes(kClientFd, 1'000);
  REQUIRE(fixture.relay.handle_readable(netfault::Side::Client, false, base()).status ==
          netfault::PumpStatus::Continue);

  // Nothing eligible yet: flush sends nothing and EPOLLOUT must not be requested.
  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base()).status == netfault::PumpStatus::Continue);
  CHECK(fixture.io.sent[kUpstreamFd].empty());
  CHECK_FALSE(fixture.relay.desired_interest(netfault::Side::Upstream, base()).write);
  REQUIRE(fixture.relay.next_wake(netfault::Side::Upstream, base()) == base() + 50ms);

  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base() + 50ms).status ==
          netfault::PumpStatus::Continue);
  CHECK(fixture.io.sent[kUpstreamFd].size() == 1'000);
  CHECK_FALSE(fixture.relay.next_wake(netfault::Side::Upstream, base() + 50ms).has_value());
}

TEST_CASE("relay latency with jitter is deterministic for equal seeds") {
  netfault::FaultPlan plan;
  plan.latency = 20ms;
  plan.jitter = 10ms;
  plan.master_seed = 42;

  const auto run = [&](std::uint64_t connection_id) {
    Fixture fixture{small_config(), plan, connection_id};
    for (int chunk = 0; chunk < 5; ++chunk) {
      fixture.io.push_receive_bytes(kClientFd, 100);
      if (fixture.relay.handle_readable(netfault::Side::Client, false, base()).status !=
          netfault::PumpStatus::Continue) {
        return std::string{"close"};
      }
    }
    const auto detail = fixture.relay.close_detail(base());
    return detail.substr(detail.find("c2u_delay_budget_us="));
  };

  CHECK(run(1) == run(1));
  CHECK(run(1) != run(2));  // different connections draw different jitter
}

TEST_CASE("relay token bucket paces flushes and recovers over time") {
  netfault::FaultPlan plan;
  plan.rate_bytes_per_second = 1'000;
  plan.burst_bytes = 500;
  Fixture fixture{small_config(), plan};

  fixture.io.push_receive_bytes(kClientFd, 2'000);
  REQUIRE(fixture.relay.handle_readable(netfault::Side::Client, false, base()).status ==
          netfault::PumpStatus::Continue);

  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base()).status == netfault::PumpStatus::Continue);
  CHECK(fixture.io.sent[kUpstreamFd].size() == 500);  // burst only
  CHECK_FALSE(fixture.relay.desired_interest(netfault::Side::Upstream, base()).write);
  const auto wake = fixture.relay.next_wake(netfault::Side::Upstream, base());
  REQUIRE(wake.has_value());
  CHECK(*wake == base() + 500ms);  // when a full burst of tokens exists

  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, *wake).status == netfault::PumpStatus::Continue);
  CHECK(fixture.io.sent[kUpstreamFd].size() == 1'000);
  // Each wake releases at most one 500-byte burst, so the remaining 1000
  // bytes drain over two more paced flushes.
  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base() + 1500ms).status ==
          netfault::PumpStatus::Continue);
  CHECK(fixture.io.sent[kUpstreamFd].size() == 1'500);
  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base() + 2s).status ==
          netfault::PumpStatus::Continue);
  CHECK(fixture.io.sent[kUpstreamFd].size() == 2'000);
}

TEST_CASE("relay fault direction selection leaves the other direction unpaced") {
  netfault::FaultPlan plan;
  plan.latency = 50ms;
  plan.directions = netfault::FaultDirections::ClientToUpstream;
  Fixture fixture{small_config(), plan};

  fixture.io.push_receive_bytes(kClientFd, 100);
  fixture.io.push_receive_bytes(kUpstreamFd, 200);
  REQUIRE(fixture.relay.handle_readable(netfault::Side::Client, false, base()).status ==
          netfault::PumpStatus::Continue);
  REQUIRE(fixture.relay.handle_readable(netfault::Side::Upstream, false, base()).status ==
          netfault::PumpStatus::Continue);
  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base()).status == netfault::PumpStatus::Continue);
  REQUIRE(fixture.relay.flush(netfault::Side::Client, base()).status == netfault::PumpStatus::Continue);

  CHECK(fixture.io.sent[kUpstreamFd].empty());          // c2u delayed
  CHECK(fixture.io.sent[kClientFd].size() == 200);      // u2c unpaced
}

TEST_CASE("relay injects a reset after the configured forwarded bytes") {
  netfault::FaultPlan plan;
  plan.reset_after_bytes = 1'000;
  Fixture fixture{small_config(), plan};

  fixture.io.push_receive_bytes(kClientFd, 600);
  REQUIRE(fixture.relay.handle_readable(netfault::Side::Client, false, base()).status ==
          netfault::PumpStatus::Continue);
  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base()).status == netfault::PumpStatus::Continue);

  fixture.io.push_receive_bytes(kClientFd, 600);
  REQUIRE(fixture.relay.handle_readable(netfault::Side::Client, false, base()).status ==
          netfault::PumpStatus::Continue);
  const auto result = fixture.relay.flush(netfault::Side::Upstream, base());

  REQUIRE(result.status == netfault::PumpStatus::CloseConnection);
  CHECK(result.final_state == netfault::ConnectionState::Reset);
  CHECK(result.reason == "fault_reset");
  CHECK(fixture.observer.count("fault_injected") == 1);
}

TEST_CASE("relay injects a half-close toward the client and still drains") {
  netfault::FaultPlan plan;
  plan.half_close_after_bytes = 500;
  Fixture fixture{small_config(), plan};

  fixture.io.push_receive_bytes(kUpstreamFd, 800);
  REQUIRE(fixture.relay.handle_readable(netfault::Side::Upstream, false, base()).status ==
          netfault::PumpStatus::Continue);
  REQUIRE(fixture.relay.flush(netfault::Side::Client, base()).status == netfault::PumpStatus::Continue);

  CHECK(fixture.io.sent[kClientFd].size() == 800);
  REQUIRE(fixture.io.shutdowns == std::vector<int>{kClientFd});
  CHECK(fixture.observer.count("fault_injected") == 1);

  // Later upstream bytes can no longer reach the client; the connection must
  // still be able to drain once both read sides close.
  fixture.io.push_receive_bytes(kUpstreamFd, 100);
  fixture.io.receive_steps[kUpstreamFd].push_back({netfault::IoStatus::PeerClosed, {}, 0});
  fixture.io.receive_steps[kClientFd].push_back({netfault::IoStatus::PeerClosed, {}, 0});
  REQUIRE(fixture.relay.handle_readable(netfault::Side::Upstream, false, base()).status ==
          netfault::PumpStatus::Continue);
  REQUIRE(fixture.relay.handle_readable(netfault::Side::Client, false, base()).status ==
          netfault::PumpStatus::Continue);
  REQUIRE(fixture.relay.flush(netfault::Side::Client, base()).status == netfault::PumpStatus::Continue);
  fixture.relay.propagate_half_closes();
  CHECK(fixture.io.sent[kClientFd].size() == 800);  // no bytes after the injected half-close
  CHECK(fixture.relay.fully_drained());
}

TEST_CASE("relay tracks last activity for idle timeouts") {
  Fixture fixture;
  CHECK(fixture.relay.last_activity() == base());

  fixture.io.push_receive_bytes(kClientFd, 10);
  REQUIRE(fixture.relay.handle_readable(netfault::Side::Client, false, base() + 3s).status ==
          netfault::PumpStatus::Continue);
  CHECK(fixture.relay.last_activity() == base() + 3s);

  REQUIRE(fixture.relay.flush(netfault::Side::Upstream, base() + 5s).status == netfault::PumpStatus::Continue);
  CHECK(fixture.relay.last_activity() == base() + 5s);
}
