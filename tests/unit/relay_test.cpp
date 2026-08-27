#include "netfault/relay.hpp"

#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace {

constexpr int kClientFd = 100;
constexpr int kUpstreamFd = 200;

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

}  // namespace

TEST_CASE("relay forwards client bytes upstream with exact accounting") {
  FakeSocketIo io;
  RecordingObserver observer;
  netfault::Relay relay{small_config(), kClientFd, kUpstreamFd, false, io, observer};

  io.push_receive_bytes(kClientFd, 1'000);
  REQUIRE(relay.handle_readable(netfault::Side::Client, false).status == netfault::PumpStatus::Continue);
  REQUIRE(relay.flush(netfault::Side::Upstream).status == netfault::PumpStatus::Continue);

  CHECK(relay.metrics().client_bytes_read == 1'000);
  CHECK(relay.metrics().upstream_bytes_written == 1'000);
  CHECK(io.sent[kUpstreamFd].size() == 1'000);
  CHECK(relay.metrics().eagain_events == 1);  // the drain-to-EAGAIN read after the payload
}

TEST_CASE("relay retries forced partial writes until the queue drains") {
  FakeSocketIo io;
  RecordingObserver observer;
  netfault::Relay relay{small_config(), kClientFd, kUpstreamFd, false, io, observer};

  io.push_receive_bytes(kClientFd, 3'000);
  // Every send call transfers at most 700 bytes, forcing positive short writes.
  for (int step = 0; step < 5; ++step) {
    io.send_steps[kUpstreamFd].push_back({netfault::IoStatus::Transferred, 700, 0});
  }

  REQUIRE(relay.handle_readable(netfault::Side::Client, false).status == netfault::PumpStatus::Continue);
  REQUIRE(relay.flush(netfault::Side::Upstream).status == netfault::PumpStatus::Continue);

  CHECK(io.sent[kUpstreamFd].size() == 3'000);
  CHECK(relay.metrics().upstream_bytes_written == 3'000);
  CHECK(relay.metrics().partial_writes >= 4);  // 700-byte slices of a 3000-byte queue
  CHECK(relay.metrics().write_operations >= 5);
}

TEST_CASE("relay preserves queued bytes across send EAGAIN and requests write interest") {
  FakeSocketIo io;
  RecordingObserver observer;
  netfault::Relay relay{small_config(), kClientFd, kUpstreamFd, false, io, observer};

  io.push_receive_bytes(kClientFd, 2'000);
  io.send_steps[kUpstreamFd].push_back({netfault::IoStatus::Transferred, 500, 0});
  io.send_steps[kUpstreamFd].push_back({netfault::IoStatus::WouldBlock, 0, 0});

  REQUIRE(relay.handle_readable(netfault::Side::Client, false).status == netfault::PumpStatus::Continue);
  REQUIRE(relay.flush(netfault::Side::Upstream).status == netfault::PumpStatus::Continue);

  CHECK(io.sent[kUpstreamFd].size() == 500);
  CHECK(relay.desired_interest(netfault::Side::Upstream).write);
  CHECK(relay.metrics().eagain_events >= 1);

  REQUIRE(relay.flush(netfault::Side::Upstream).status == netfault::PumpStatus::Continue);
  CHECK(io.sent[kUpstreamFd].size() == 2'000);
  CHECK_FALSE(relay.desired_interest(netfault::Side::Upstream).write);
}

TEST_CASE("relay forwards a half-close only after the queue drains") {
  FakeSocketIo io;
  RecordingObserver observer;
  netfault::Relay relay{small_config(), kClientFd, kUpstreamFd, false, io, observer};

  io.push_receive_bytes(kClientFd, 1'500);
  io.receive_steps[kClientFd].push_back({netfault::IoStatus::PeerClosed, {}, 0});
  io.send_steps[kUpstreamFd].push_back({netfault::IoStatus::Transferred, 400, 0});
  io.send_steps[kUpstreamFd].push_back({netfault::IoStatus::WouldBlock, 0, 0});

  REQUIRE(relay.handle_readable(netfault::Side::Client, false).status == netfault::PumpStatus::Continue);
  CHECK(observer.count("peer_half_closed") == 1);
  REQUIRE(relay.flush(netfault::Side::Upstream).status == netfault::PumpStatus::Continue);

  // 1100 bytes are still queued, so the half-close must not be forwarded yet.
  relay.propagate_half_closes();
  CHECK(io.shutdowns.empty());

  REQUIRE(relay.flush(netfault::Side::Upstream).status == netfault::PumpStatus::Continue);
  relay.propagate_half_closes();
  REQUIRE(io.shutdowns == std::vector<int>{kUpstreamFd});
  CHECK(observer.count("half_close_forwarded") == 1);
  CHECK(io.sent[kUpstreamFd].size() == 1'500);
}

TEST_CASE("relay reports fully drained after both directions half-close") {
  FakeSocketIo io;
  RecordingObserver observer;
  netfault::Relay relay{small_config(), kClientFd, kUpstreamFd, false, io, observer};

  io.push_receive_bytes(kClientFd, 100);
  io.receive_steps[kClientFd].push_back({netfault::IoStatus::PeerClosed, {}, 0});
  io.push_receive_bytes(kUpstreamFd, 100);
  io.receive_steps[kUpstreamFd].push_back({netfault::IoStatus::PeerClosed, {}, 0});

  REQUIRE(relay.handle_readable(netfault::Side::Client, false).status == netfault::PumpStatus::Continue);
  REQUIRE(relay.handle_readable(netfault::Side::Upstream, false).status == netfault::PumpStatus::Continue);
  CHECK_FALSE(relay.fully_drained());
  REQUIRE(relay.flush(netfault::Side::Upstream).status == netfault::PumpStatus::Continue);
  REQUIRE(relay.flush(netfault::Side::Client).status == netfault::PumpStatus::Continue);
  relay.propagate_half_closes();

  CHECK(relay.fully_drained());
  CHECK(relay.state() == netfault::ConnectionState::Draining);
  CHECK(io.shutdowns.size() == 2);
}

TEST_CASE("relay pauses reads at the high-water mark and resumes after draining") {
  FakeSocketIo io;
  RecordingObserver observer;
  const netfault::RelayConfig config{
      .buffer_bytes_per_direction = 2'048, .low_water_bytes = 512, .high_water_bytes = 2'048};
  netfault::Relay relay{config, kClientFd, kUpstreamFd, false, io, observer};

  io.push_receive_bytes(kClientFd, 8'192);
  REQUIRE(relay.handle_readable(netfault::Side::Client, false).status == netfault::PumpStatus::Continue);

  CHECK(observer.count("read_paused") == 1);
  CHECK_FALSE(relay.desired_interest(netfault::Side::Client).read);
  CHECK(relay.metrics().client_bytes_read == 2'048);

  REQUIRE(relay.flush(netfault::Side::Upstream).status == netfault::PumpStatus::Continue);
  CHECK(observer.count("read_resumed") == 1);
  CHECK(relay.desired_interest(netfault::Side::Client).read);

  // Reads continue with the remaining scripted payload after the resume.
  REQUIRE(relay.handle_readable(netfault::Side::Client, false).status == netfault::PumpStatus::Continue);
  REQUIRE(relay.flush(netfault::Side::Upstream).status == netfault::PumpStatus::Continue);
  REQUIRE(relay.handle_readable(netfault::Side::Client, false).status == netfault::PumpStatus::Continue);
  REQUIRE(relay.flush(netfault::Side::Upstream).status == netfault::PumpStatus::Continue);
  REQUIRE(relay.handle_readable(netfault::Side::Client, false).status == netfault::PumpStatus::Continue);
  REQUIRE(relay.flush(netfault::Side::Upstream).status == netfault::PumpStatus::Continue);
  CHECK(io.sent[kUpstreamFd].size() == 8'192);
}

TEST_CASE("relay maps receive ECONNRESET to a reset close request") {
  FakeSocketIo io;
  RecordingObserver observer;
  netfault::Relay relay{small_config(), kClientFd, kUpstreamFd, false, io, observer};

  io.receive_steps[kClientFd].push_back({netfault::IoStatus::Error, {}, ECONNRESET});
  const auto result = relay.handle_readable(netfault::Side::Client, false);

  REQUIRE(result.status == netfault::PumpStatus::CloseConnection);
  CHECK(result.final_state == netfault::ConnectionState::Reset);
  CHECK(result.reason.starts_with("read="));
}

TEST_CASE("relay maps send EPIPE to a reset close request") {
  FakeSocketIo io;
  RecordingObserver observer;
  netfault::Relay relay{small_config(), kClientFd, kUpstreamFd, false, io, observer};

  io.push_receive_bytes(kClientFd, 64);
  io.send_steps[kUpstreamFd].push_back({netfault::IoStatus::Error, 0, EPIPE});

  REQUIRE(relay.handle_readable(netfault::Side::Client, false).status == netfault::PumpStatus::Continue);
  const auto result = relay.flush(netfault::Side::Upstream);

  REQUIRE(result.status == netfault::PumpStatus::CloseConnection);
  CHECK(result.final_state == netfault::ConnectionState::Reset);
  CHECK(result.reason.starts_with("write="));
}

TEST_CASE("relay treats EAGAIN after a hangup hint as end of stream") {
  FakeSocketIo io;
  RecordingObserver observer;
  netfault::Relay relay{small_config(), kClientFd, kUpstreamFd, false, io, observer};

  const auto result = relay.handle_readable(netfault::Side::Client, true);
  REQUIRE(result.status == netfault::PumpStatus::Continue);
  CHECK(observer.count("peer_half_closed") == 1);
  CHECK_FALSE(relay.desired_interest(netfault::Side::Client).read);
}

TEST_CASE("relay suppresses reads and flushes while the upstream connect is pending") {
  FakeSocketIo io;
  RecordingObserver observer;
  netfault::Relay relay{small_config(), kClientFd, kUpstreamFd, true, io, observer};

  CHECK(relay.state() == netfault::ConnectionState::ConnectingUpstream);
  const auto interest = relay.desired_interest(netfault::Side::Upstream);
  CHECK_FALSE(interest.read);
  CHECK(interest.write);

  io.push_receive_bytes(kClientFd, 256);
  REQUIRE(relay.handle_readable(netfault::Side::Client, false).status == netfault::PumpStatus::Continue);
  REQUIRE(relay.flush(netfault::Side::Upstream).status == netfault::PumpStatus::Continue);
  CHECK(io.sent[kUpstreamFd].empty());

  relay.mark_upstream_connected();
  CHECK(relay.state() == netfault::ConnectionState::Active);
  REQUIRE(relay.flush(netfault::Side::Upstream).status == netfault::PumpStatus::Continue);
  CHECK(io.sent[kUpstreamFd].size() == 256);
}

TEST_CASE("relay close detail reports byte and backpressure counters") {
  FakeSocketIo io;
  RecordingObserver observer;
  netfault::Relay relay{small_config(), kClientFd, kUpstreamFd, false, io, observer};

  io.push_receive_bytes(kClientFd, 512);
  REQUIRE(relay.handle_readable(netfault::Side::Client, false).status == netfault::PumpStatus::Continue);
  REQUIRE(relay.flush(netfault::Side::Upstream).status == netfault::PumpStatus::Continue);

  const auto detail = relay.close_detail();
  CHECK(detail.find(",client_read=512") != std::string::npos);
  CHECK(detail.find(",upstream_written=512") != std::string::npos);
  CHECK(detail.find(",rejected_bytes=0") != std::string::npos);
  CHECK(detail.find(",c2u_pause_count=0") != std::string::npos);
  CHECK(detail.find(",u2c_paused_us=") != std::string::npos);
}
