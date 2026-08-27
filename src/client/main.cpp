#include "netfault/endpoint.hpp"
#include "netfault/unique_fd.hpp"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

struct Result {
  bool success{false};
  std::string error;
  std::int64_t duration_us{0};
  std::size_t bytes{0};
};

std::size_t parse_size(std::string_view text, std::string_view name) {
  std::size_t value = 0;
  const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size() || value == 0) {
    throw std::invalid_argument(std::string{name} + " must be a positive integer");
  }
  return value;
}

std::uint64_t parse_seed(std::string_view text) {
  std::uint64_t value = 0;
  const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size()) {
    throw std::invalid_argument("seed must be an unsigned integer");
  }
  return value;
}

bool send_all(int fd, const std::vector<std::byte>& payload, std::string& error) {
  std::size_t offset = 0;
  while (offset < payload.size()) {
    const auto count = ::send(fd, payload.data() + offset, payload.size() - offset, MSG_NOSIGNAL);
    if (count > 0) {
      offset += static_cast<std::size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) {
      continue;
    }
    error = "send=" + std::string{std::strerror(errno)};
    return false;
  }
  return true;
}

Result run_connection(const netfault::Endpoint& endpoint, std::size_t payload_bytes, std::uint64_t seed) {
  Result result;
  result.bytes = payload_bytes;
  std::mt19937_64 generator{seed};
  std::vector<std::byte> payload(payload_bytes);
  for (auto& value : payload) {
    value = static_cast<std::byte>(generator() & 0xFFU);
  }

  netfault::UniqueFd socket{::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)};
  if (!socket) {
    result.error = "socket=" + std::string{std::strerror(errno)};
    return result;
  }
  timeval timeout{10, 0};
  static_cast<void>(::setsockopt(socket.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)));
  static_cast<void>(::setsockopt(socket.get(), SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)));

  const auto address = endpoint.to_sockaddr();
  const auto start = std::chrono::steady_clock::now();
  if (::connect(socket.get(), reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0) {
    result.error = "connect=" + std::string{std::strerror(errno)};
    return result;
  }

  std::vector<std::byte> received;
  received.reserve(payload_bytes);
  std::string receive_error;
  std::jthread reader{[&] {
    std::vector<std::byte> buffer(16'384);
    while (true) {
      const auto count = ::recv(socket.get(), buffer.data(), buffer.size(), 0);
      if (count > 0) {
        const auto bytes = static_cast<std::size_t>(count);
        received.insert(received.end(), buffer.begin(), buffer.begin() + static_cast<std::ptrdiff_t>(bytes));
        if (received.size() > payload.size()) {
          receive_error = "received_more_bytes_than_sent";
          return;
        }
        continue;
      }
      if (count == 0) {
        return;
      }
      if (errno == EINTR) {
        continue;
      }
      receive_error = "recv=" + std::string{std::strerror(errno)};
      return;
    }
  }};

  if (!send_all(socket.get(), payload, result.error)) {
    static_cast<void>(::shutdown(socket.get(), SHUT_RDWR));
    reader.join();
    return result;
  }
  if (::shutdown(socket.get(), SHUT_WR) < 0) {
    result.error = "shutdown=" + std::string{std::strerror(errno)};
    static_cast<void>(::shutdown(socket.get(), SHUT_RDWR));
    reader.join();
    return result;
  }
  reader.join();
  if (!receive_error.empty()) {
    result.error = receive_error;
    return result;
  }
  result.duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - start)
                           .count();
  if (received != payload) {
    result.error = "payload_mismatch";
    return result;
  }
  result.success = true;
  return result;
}

void print_help() {
  std::cout << R"(NetFault Lab deterministic client (Milestone 1)

Usage:
  netfault-client [--connect 127.0.0.1:8080] [--connections N]
                  [--payload-bytes N] [--seed N]

Output is one machine-readable JSON summary. Only loopback destinations are allowed.
)";
}

}  // namespace

int main(int argc, char** argv) {
  try {
    auto endpoint = netfault::Endpoint::parse("127.0.0.1:8080");
    std::size_t connection_count = 1;
    std::size_t payload_bytes = 4'096;
    std::uint64_t seed = 12'345;
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
      if (argument == "--connect") {
        endpoint = netfault::Endpoint::parse(require_value());
      } else if (argument == "--connections") {
        connection_count = parse_size(require_value(), "connections");
      } else if (argument == "--payload-bytes") {
        payload_bytes = parse_size(require_value(), "payload_bytes");
      } else if (argument == "--seed") {
        seed = parse_seed(require_value());
      } else {
        throw std::invalid_argument("unknown argument: " + std::string{argument});
      }
    }
    if (!endpoint.is_loopback()) {
      throw std::invalid_argument("the Milestone 1 client only permits loopback destinations");
    }
    if (connection_count > 1'024 || payload_bytes > 64U * 1'024U * 1'024U) {
      throw std::invalid_argument("requested workload exceeds the Milestone 1 safety limit");
    }

    std::vector<Result> results(connection_count);
    std::vector<std::jthread> workers;
    workers.reserve(connection_count);
    for (std::size_t index = 0; index < connection_count; ++index) {
      workers.emplace_back([&, index] {
        results[index] = run_connection(endpoint, payload_bytes, seed + static_cast<std::uint64_t>(index));
      });
    }
    workers.clear();

    std::size_t successful = 0;
    std::int64_t maximum_duration_us = 0;
    std::string first_error;
    for (const auto& result : results) {
      if (result.success) {
        ++successful;
        maximum_duration_us = std::max(maximum_duration_us, result.duration_us);
      } else if (first_error.empty()) {
        first_error = result.error;
      }
    }
    std::cout << "{\"connections\":" << connection_count << ",\"successful\":" << successful
              << ",\"payload_bytes_each\":" << payload_bytes << ",\"seed\":" << seed
              << ",\"max_duration_us\":" << maximum_duration_us;
    if (!first_error.empty()) {
      std::cout << ",\"first_error\":\"" << first_error << "\"";
    }
    std::cout << "}\n";
    return successful == connection_count ? EXIT_SUCCESS : EXIT_FAILURE;
  } catch (const std::exception& error) {
    std::cerr << "netfault-client: " << error.what() << '\n';
    return EXIT_FAILURE;
  }
}
