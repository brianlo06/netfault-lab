#include "netfault/endpoint.hpp"
#include "netfault/proxy.hpp"

#include <charconv>
#include <chrono>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

std::size_t parse_size(std::string_view text, std::string_view name) {
  std::size_t value = 0;
  const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size()) {
    throw std::invalid_argument(std::string{name} + " must be a positive integer");
  }
  return value;
}

double parse_probability(std::string_view text) {
  double value = 0.0;
  const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size() || value < 0.0 || value > 1.0) {
    throw std::invalid_argument("fault_probability must be a number between 0 and 1");
  }
  return value;
}

netfault::FaultDirections parse_directions(std::string_view text) {
  if (text == "both") {
    return netfault::FaultDirections::Both;
  }
  if (text == "c2u") {
    return netfault::FaultDirections::ClientToUpstream;
  }
  if (text == "u2c") {
    return netfault::FaultDirections::UpstreamToClient;
  }
  throw std::invalid_argument("fault_direction must be both, c2u, or u2c");
}

void print_help() {
  std::cout << R"(NetFault Lab proxy (Milestone 3)

Usage:
  netfault-proxy [options]

Options:
  --listen HOST:PORT       Numeric IPv4 listener (default 127.0.0.1:8080)
  --upstream HOST:PORT     Numeric IPv4 upstream (default 127.0.0.1:9000)
  --max-connections N      Connection limit (default 256)
  --buffer-bytes N         Fixed bytes per direction/connection (default 65536)
  --low-water-bytes N      Resume reads at or below this occupancy (default 32768)
  --high-water-bytes N     Pause reads at this occupancy (default 65536)
  --socket-buffer-bytes N  Set SO_SNDBUF/SO_RCVBUF on upstream sockets; small
                            values constrain the upstream path and force
                            partial writes for testing (default: kernel
                            default)
  --connect-timeout-ms N   Fail connections whose upstream connect exceeds N ms
                            (default 0 = disabled)
  --idle-timeout-ms N      Close connections with no transfer for N ms
                            (default 0 = disabled)
  --metrics-file PATH      Atomically write a JSON metrics document to PATH on
                            SIGUSR1 and at shutdown (default: disabled)

Fault injection (deterministic; reproducible from the same seed):
  --fault-seed N           Master seed for all fault randomness (default 0)
  --fault-probability P    Chance each connection receives faults (default 1)
  --fault-direction D      both, c2u, or u2c for latency/jitter/rate (default both)
  --fault-latency-ms N     Delay forwarded bytes by N ms
  --fault-jitter-ms N      Uniform per-chunk jitter in [-N, +N] ms, clamped so
                            total delay never goes negative
  --fault-rate-bytes-per-sec N
                            Token-bucket rate limit per direction
  --fault-burst-bytes N    Token-bucket burst (default: one second of tokens)
  --fault-reset-after-bytes N
                            Reset the connection after N total forwarded bytes
  --fault-half-close-after-bytes N
                            Half-close toward the client after N bytes reach it

  --unsafe-allow-non-loopback-listen
                            Permit a non-loopback listener after a warning
  --unsafe-allow-non-loopback-upstream
                            Permit a non-loopback upstream after a warning
  --help                    Show this help

Payload contents are never logged.
)";
}

}  // namespace

int main(int argc, char** argv) {
  try {
    netfault::ProxyConfig config;
    bool buffer_was_set = false;
    bool low_water_was_set = false;
    bool high_water_was_set = false;
    for (int index = 1; index < argc; ++index) {
      const std::string_view argument{argv[index]};
      const auto require_value = [&]() -> std::string_view {
        if (index + 1 >= argc) {
          throw std::invalid_argument(std::string{argument} + " requires a value");
        }
        ++index;
        return argv[index];
      };

      if (argument == "--help") {
        print_help();
        return EXIT_SUCCESS;
      }
      if (argument == "--listen") {
        config.listen = netfault::Endpoint::parse(require_value());
      } else if (argument == "--upstream") {
        config.upstream = netfault::Endpoint::parse(require_value());
      } else if (argument == "--max-connections") {
        config.max_connections = parse_size(require_value(), "max_connections");
      } else if (argument == "--buffer-bytes") {
        config.buffer_bytes_per_direction = parse_size(require_value(), "buffer_bytes");
        buffer_was_set = true;
      } else if (argument == "--low-water-bytes") {
        config.low_water_bytes = parse_size(require_value(), "low_water_bytes");
        low_water_was_set = true;
      } else if (argument == "--high-water-bytes") {
        config.high_water_bytes = parse_size(require_value(), "high_water_bytes");
        high_water_was_set = true;
      } else if (argument == "--socket-buffer-bytes") {
        config.socket_buffer_bytes = parse_size(require_value(), "socket_buffer_bytes");
      } else if (argument == "--connect-timeout-ms") {
        config.connect_timeout = std::chrono::milliseconds{parse_size(require_value(), "connect_timeout_ms")};
      } else if (argument == "--idle-timeout-ms") {
        config.idle_timeout = std::chrono::milliseconds{parse_size(require_value(), "idle_timeout_ms")};
      } else if (argument == "--metrics-file") {
        config.metrics_file = std::string{require_value()};
      } else if (argument == "--fault-seed") {
        config.faults.master_seed = parse_size(require_value(), "fault_seed");
      } else if (argument == "--fault-probability") {
        config.faults.apply_probability = parse_probability(require_value());
      } else if (argument == "--fault-direction") {
        config.faults.directions = parse_directions(require_value());
      } else if (argument == "--fault-latency-ms") {
        config.faults.latency = std::chrono::milliseconds{parse_size(require_value(), "fault_latency_ms")};
      } else if (argument == "--fault-jitter-ms") {
        config.faults.jitter = std::chrono::milliseconds{parse_size(require_value(), "fault_jitter_ms")};
      } else if (argument == "--fault-rate-bytes-per-sec") {
        config.faults.rate_bytes_per_second = parse_size(require_value(), "fault_rate_bytes_per_sec");
      } else if (argument == "--fault-burst-bytes") {
        config.faults.burst_bytes = parse_size(require_value(), "fault_burst_bytes");
      } else if (argument == "--fault-reset-after-bytes") {
        config.faults.reset_after_bytes = parse_size(require_value(), "fault_reset_after_bytes");
      } else if (argument == "--fault-half-close-after-bytes") {
        config.faults.half_close_after_bytes = parse_size(require_value(), "fault_half_close_after_bytes");
      } else if (argument == "--unsafe-allow-non-loopback-listen") {
        config.allow_non_loopback_listen = true;
      } else if (argument == "--unsafe-allow-non-loopback-upstream") {
        config.allow_non_loopback_upstream = true;
      } else {
        throw std::invalid_argument("unknown argument: " + std::string{argument});
      }
    }

    if (buffer_was_set && !high_water_was_set) {
      config.high_water_bytes = config.buffer_bytes_per_direction;
    }
    if ((buffer_was_set || high_water_was_set) && !low_water_was_set) {
      config.low_water_bytes = config.high_water_bytes / 2;
    }

    if (!config.listen.is_loopback()) {
      std::cerr << "WARNING: non-loopback listener requested; this can expose the proxy to other hosts.\n";
    }
    if (!config.upstream.is_loopback()) {
      std::cerr << "WARNING: non-loopback upstream requested; use only an explicitly authorized destination.\n";
    }
    return netfault::Proxy{config}.run();
  } catch (const std::exception& error) {
    std::cerr << "netfault-proxy: " << error.what() << '\n';
    return EXIT_FAILURE;
  }
}
