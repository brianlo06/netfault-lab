#include "netfault/endpoint.hpp"
#include "netfault/proxy.hpp"

#include <charconv>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
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

void print_help() {
  std::cout << R"(NetFault Lab proxy (Milestone 2)

Usage:
  netfault-proxy [options]

Options:
  --listen HOST:PORT       Numeric IPv4 listener (default 127.0.0.1:8080)
  --upstream HOST:PORT     Numeric IPv4 upstream (default 127.0.0.1:9000)
  --max-connections N      Connection limit (default 256)
  --buffer-bytes N         Fixed bytes per direction/connection (default 65536)
  --low-water-bytes N      Resume reads at or below this occupancy (default 32768)
  --high-water-bytes N     Pause reads at this occupancy (default 65536)
  --unsafe-allow-non-loopback-listen
                            Permit a non-loopback listener after a warning
  --unsafe-allow-non-loopback-upstream
                            Permit a non-loopback upstream after a warning
  --help                    Show this help

Payload contents are never logged. Fault injection is not implemented in Milestone 2.
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
