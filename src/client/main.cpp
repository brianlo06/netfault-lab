#include "netfault/endpoint.hpp"
#include "netfault/unique_fd.hpp"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/utsname.h>

#include <algorithm>
#include <cmath>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

enum class ClientMode { Bulk, RequestResponse };

struct Result {
  bool success{false};
  std::string error;
  std::int64_t duration_us{0};
  std::size_t bytes{0};
  std::vector<std::int64_t> latency_samples_us;  // request-response mode only
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

bool receive_exact(int fd, std::vector<std::byte>& buffer, std::size_t expected, std::string& error) {
  std::size_t received = 0;
  while (received < expected) {
    const auto count = ::recv(fd, buffer.data() + received, expected - received, 0);
    if (count > 0) {
      received += static_cast<std::size_t>(count);
      continue;
    }
    if (count == 0) {
      error = "unexpected_eof_mid_response";
      return false;
    }
    if (errno == EINTR) {
      continue;
    }
    error = "recv=" + std::string{std::strerror(errno)};
    return false;
  }
  return true;
}

// Benchmark mode: sequential request/response exchanges against an echoing
// peer. Each request sends request_bytes and reads exactly request_bytes
// back; the round-trip time of each post-warmup request is recorded.
Result run_request_response(const netfault::Endpoint& endpoint, std::size_t requests, std::size_t warmup,
                            std::size_t request_bytes, std::uint64_t seed) {
  Result result;
  std::mt19937_64 generator{seed};
  std::vector<std::byte> request(request_bytes);
  std::vector<std::byte> response(request_bytes);
  result.latency_samples_us.reserve(requests);

  netfault::UniqueFd socket{::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)};
  if (!socket) {
    result.error = "socket=" + std::string{std::strerror(errno)};
    return result;
  }
  timeval timeout{30, 0};
  static_cast<void>(::setsockopt(socket.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)));
  static_cast<void>(::setsockopt(socket.get(), SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)));
  int no_delay = 1;
  static_cast<void>(::setsockopt(socket.get(), IPPROTO_TCP, TCP_NODELAY, &no_delay, sizeof(no_delay)));

  const auto address = endpoint.to_sockaddr();
  const auto start = std::chrono::steady_clock::now();
  if (::connect(socket.get(), reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0) {
    result.error = "connect=" + std::string{std::strerror(errno)};
    return result;
  }

  for (std::size_t index = 0; index < warmup + requests; ++index) {
    for (auto& value : request) {
      value = static_cast<std::byte>(generator() & 0xFFU);
    }
    const auto request_start = std::chrono::steady_clock::now();
    if (!send_all(socket.get(), request, result.error)) {
      return result;
    }
    if (!receive_exact(socket.get(), response, request_bytes, result.error)) {
      return result;
    }
    if (response != request) {
      result.error = "response_mismatch";
      return result;
    }
    if (index >= warmup) {
      result.latency_samples_us.push_back(std::chrono::duration_cast<std::chrono::microseconds>(
                                              std::chrono::steady_clock::now() - request_start)
                                              .count());
    }
  }

  static_cast<void>(::shutdown(socket.get(), SHUT_WR));
  std::vector<std::byte> drain(4'096);
  while (true) {
    const auto count = ::recv(socket.get(), drain.data(), drain.size(), 0);
    if (count > 0) {
      result.error = "unexpected_data_after_final_response";
      return result;
    }
    if (count == 0) {
      break;
    }
    if (errno == EINTR) {
      continue;
    }
    result.error = "drain_recv=" + std::string{std::strerror(errno)};
    return result;
  }
  result.duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::steady_clock::now() - start)
                           .count();
  result.bytes = (warmup + requests) * request_bytes;
  result.success = true;
  return result;
}

// Nearest-rank percentile over an already-sorted sample vector.
std::int64_t percentile(const std::vector<std::int64_t>& sorted, double p) {
  const auto rank = static_cast<std::size_t>(std::max(
      1.0, std::ceil(p / 100.0 * static_cast<double>(sorted.size()))));
  return sorted[rank - 1];
}

void print_help() {
  std::cout << R"(NetFault Lab deterministic client (Milestone 4)

Usage:
  netfault-client [--connect 127.0.0.1:8080] [--connections N]
                  [--payload-bytes N] [--seed N]
                  [--mode bulk|request-response]
                  [--requests N] [--warmup N] [--request-bytes N]

bulk (default): each connection streams --payload-bytes full duplex and
verifies the echo. request-response: each connection performs --warmup
unrecorded then --requests recorded sequential exchanges of --request-bytes
and reports pooled nearest-rank latency percentiles.

Output is one machine-readable JSON summary recording the configuration,
seed, and environment so runs are reproducible. Only loopback destinations
are allowed.
)";
}

}  // namespace

int main(int argc, char** argv) {
  try {
    auto endpoint = netfault::Endpoint::parse("127.0.0.1:8080");
    ClientMode mode = ClientMode::Bulk;
    std::size_t connection_count = 1;
    std::size_t payload_bytes = 4'096;
    std::size_t requests = 100;
    std::size_t warmup = 5;
    std::size_t request_bytes = 1'024;
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
      } else if (argument == "--mode") {
        const auto value = require_value();
        if (value == "bulk") {
          mode = ClientMode::Bulk;
        } else if (value == "request-response") {
          mode = ClientMode::RequestResponse;
        } else {
          throw std::invalid_argument("mode must be bulk or request-response");
        }
      } else if (argument == "--requests") {
        requests = parse_size(require_value(), "requests");
      } else if (argument == "--warmup") {
        warmup = parse_size(require_value(), "warmup");
      } else if (argument == "--request-bytes") {
        request_bytes = parse_size(require_value(), "request_bytes");
      } else if (argument == "--seed") {
        seed = parse_seed(require_value());
      } else {
        throw std::invalid_argument("unknown argument: " + std::string{argument});
      }
    }
    if (!endpoint.is_loopback()) {
      throw std::invalid_argument("the client only permits loopback destinations");
    }
    if (connection_count > 1'024 || payload_bytes > 64U * 1'024U * 1'024U) {
      throw std::invalid_argument("requested workload exceeds the safety limit");
    }
    if (requests > 100'000 || warmup > 1'000 || request_bytes > 1U * 1'024U * 1'024U ||
        connection_count * requests > 1'000'000) {
      throw std::invalid_argument("requested benchmark exceeds the sample safety limit");
    }

    std::vector<Result> results(connection_count);
    std::vector<std::jthread> workers;
    workers.reserve(connection_count);
    for (std::size_t index = 0; index < connection_count; ++index) {
      workers.emplace_back([&, index] {
        const auto connection_seed = seed + static_cast<std::uint64_t>(index);
        results[index] = mode == ClientMode::Bulk
                             ? run_connection(endpoint, payload_bytes, connection_seed)
                             : run_request_response(endpoint, requests, warmup, request_bytes,
                                                    connection_seed);
      });
    }
    workers.clear();

    std::size_t successful = 0;
    std::int64_t maximum_duration_us = 0;
    std::string first_error;
    std::vector<std::int64_t> pooled_samples;
    for (const auto& result : results) {
      if (result.success) {
        ++successful;
        maximum_duration_us = std::max(maximum_duration_us, result.duration_us);
        pooled_samples.insert(pooled_samples.end(), result.latency_samples_us.begin(),
                              result.latency_samples_us.end());
      } else if (first_error.empty()) {
        first_error = result.error;
      }
    }

    std::cout << "{\"mode\":\"" << (mode == ClientMode::Bulk ? "bulk" : "request-response") << "\""
              << ",\"connections\":" << connection_count << ",\"successful\":" << successful
              << ",\"seed\":" << seed;
    if (mode == ClientMode::Bulk) {
      std::cout << ",\"payload_bytes_each\":" << payload_bytes;
    } else {
      std::cout << ",\"requests_per_connection\":" << requests << ",\"warmup_per_connection\":" << warmup
                << ",\"request_bytes\":" << request_bytes;
    }
    utsname system_info{};
    if (::uname(&system_info) == 0) {
      std::cout << ",\"environment\":{\"sysname\":\"" << system_info.sysname << "\",\"release\":\""
                << system_info.release << "\",\"machine\":\"" << system_info.machine << "\"}";
    }
    std::cout << ",\"max_duration_us\":" << maximum_duration_us;
    if (mode == ClientMode::RequestResponse && !pooled_samples.empty()) {
      std::sort(pooled_samples.begin(), pooled_samples.end());
      const auto sum = std::accumulate(pooled_samples.begin(), pooled_samples.end(), std::int64_t{0});
      std::cout << ",\"latency_us\":{\"count\":" << pooled_samples.size()
                << ",\"min\":" << pooled_samples.front()
                << ",\"mean\":" << sum / static_cast<std::int64_t>(pooled_samples.size())
                << ",\"p50\":" << percentile(pooled_samples, 50.0)
                << ",\"p90\":" << percentile(pooled_samples, 90.0)
                << ",\"p99\":" << percentile(pooled_samples, 99.0)
                << ",\"max\":" << pooled_samples.back() << "}";
    }
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
