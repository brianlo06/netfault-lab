#include "netfault/endpoint.hpp"
#include "netfault/unique_fd.hpp"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>

#include <algorithm>
#include <chrono>
#include <charconv>
#include <span>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

volatile sig_atomic_t stop_requested = 0;

enum class ServerMode { Echo, SlowReader, SlowWriter, ReadUntilEof, SendThenHalfClose };

struct ServerConfig {
  ServerMode mode{ServerMode::Echo};
  std::chrono::milliseconds delay{0};
  std::size_t chunk_bytes{16'384};
  std::size_t send_bytes{65'536};
};

extern "C" void handle_signal(int /*signal*/) { stop_requested = 1; }

bool interruptible_delay(std::stop_token stop_token, std::chrono::milliseconds delay) {
  constexpr auto quantum = std::chrono::milliseconds{10};
  auto remaining = delay;
  while (remaining > std::chrono::milliseconds::zero() && !stop_token.stop_requested()) {
    const auto current = std::min(remaining, quantum);
    std::this_thread::sleep_for(current);
    remaining -= current;
  }
  return !stop_token.stop_requested();
}

bool send_all(int fd, const std::byte* data, std::size_t size, std::stop_token stop_token,
              const ServerConfig& config) {
  std::size_t offset = 0;
  while (offset < size && !stop_token.stop_requested()) {
    if (config.mode == ServerMode::SlowWriter && !interruptible_delay(stop_token, config.delay)) {
      return false;
    }
    const auto request_bytes = std::min(size - offset, config.chunk_bytes);
    const auto count = ::send(fd, data + offset, request_bytes, MSG_NOSIGNAL);
    if (count > 0) {
      offset += static_cast<std::size_t>(count);
      continue;
    }
    if (count < 0 && errno == EINTR) {
      continue;
    }
    return false;
  }
  return offset == size;
}

// Reads and discards until EOF, then closes without ever echoing. Exercises the
// client-initiated half-close path end to end.
void drain_until_eof(std::stop_token stop_token, int fd, std::span<std::byte> buffer) {
  while (!stop_token.stop_requested()) {
    const auto count = ::recv(fd, buffer.data(), buffer.size(), 0);
    if (count > 0) {
      continue;
    }
    if (count == 0 || (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)) {
      return;
    }
  }
}

// Sends a deterministic payload, half-closes the write side first, then drains
// whatever the peer still sends. Exercises the upstream-initiated half-close path.
void send_then_half_close(std::stop_token stop_token, int fd, std::span<std::byte> buffer,
                          const ServerConfig& config) {
  std::size_t remaining = config.send_bytes;
  for (std::size_t index = 0; index < buffer.size(); ++index) {
    buffer[index] = static_cast<std::byte>(index & 0xFFU);
  }
  while (remaining > 0 && !stop_token.stop_requested()) {
    const auto request_bytes = std::min(remaining, buffer.size());
    if (!send_all(fd, buffer.data(), request_bytes, stop_token, config)) {
      return;
    }
    remaining -= request_bytes;
  }
  static_cast<void>(::shutdown(fd, SHUT_WR));
  drain_until_eof(stop_token, fd, buffer);
}

void serve_client(std::stop_token stop_token, netfault::UniqueFd client, ServerConfig config) {
  timeval timeout{1, 0};
  static_cast<void>(::setsockopt(client.get(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)));
  static_cast<void>(::setsockopt(client.get(), SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)));
  std::vector<std::byte> buffer(config.chunk_bytes);
  if (config.mode == ServerMode::ReadUntilEof) {
    drain_until_eof(stop_token, client.get(), buffer);
    static_cast<void>(::shutdown(client.get(), SHUT_WR));
    return;
  }
  if (config.mode == ServerMode::SendThenHalfClose) {
    send_then_half_close(stop_token, client.get(), buffer, config);
    return;
  }
  while (!stop_token.stop_requested()) {
    if (config.mode == ServerMode::SlowReader && !interruptible_delay(stop_token, config.delay)) {
      return;
    }
    const auto count = ::recv(client.get(), buffer.data(), buffer.size(), 0);
    if (count > 0) {
      if (!send_all(client.get(), buffer.data(), static_cast<std::size_t>(count), stop_token, config)) {
        return;
      }
      continue;
    }
    if (count == 0) {
      static_cast<void>(::shutdown(client.get(), SHUT_WR));
      return;
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      continue;
    }
    return;
  }
}

void print_help() {
  std::cout << R"(NetFault Lab controlled server (Milestone 2)

Usage:
  netfault-server [--listen 127.0.0.1:9000]
                  [--mode echo|slow-reader|slow-writer|read-until-eof|send-then-half-close]
                  [--delay-ms N] [--chunk-bytes N] [--send-bytes N]

Slow modes delay every read or write operation. read-until-eof discards input and
closes after the peer half-closes. send-then-half-close sends --send-bytes, half-closes
its write side first, then drains input. Only loopback listeners are allowed.
)";
}

std::size_t parse_size(std::string_view text, std::string_view name, bool allow_zero = false) {
  std::size_t value = 0;
  const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
  if (error != std::errc{} || end != text.data() + text.size() || (!allow_zero && value == 0)) {
    throw std::invalid_argument(std::string{name} + " must be a valid integer");
  }
  return value;
}

ServerMode parse_mode(std::string_view mode) {
  if (mode == "echo") {
    return ServerMode::Echo;
  }
  if (mode == "slow-reader") {
    return ServerMode::SlowReader;
  }
  if (mode == "slow-writer") {
    return ServerMode::SlowWriter;
  }
  if (mode == "read-until-eof") {
    return ServerMode::ReadUntilEof;
  }
  if (mode == "send-then-half-close") {
    return ServerMode::SendThenHalfClose;
  }
  throw std::invalid_argument(
      "mode must be echo, slow-reader, slow-writer, read-until-eof, or send-then-half-close");
}

std::string_view mode_name(ServerMode mode) {
  switch (mode) {
    case ServerMode::Echo:
      return "echo";
    case ServerMode::SlowReader:
      return "slow-reader";
    case ServerMode::SlowWriter:
      return "slow-writer";
    case ServerMode::ReadUntilEof:
      return "read-until-eof";
    case ServerMode::SendThenHalfClose:
      return "send-then-half-close";
  }
  return "unknown";
}

}  // namespace

int main(int argc, char** argv) {
  try {
    auto listen_endpoint = netfault::Endpoint::parse("127.0.0.1:9000");
    ServerConfig config;
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
        listen_endpoint = netfault::Endpoint::parse(require_value());
      } else if (argument == "--mode") {
        config.mode = parse_mode(require_value());
      } else if (argument == "--delay-ms") {
        config.delay = std::chrono::milliseconds{parse_size(require_value(), "delay_ms", true)};
      } else if (argument == "--chunk-bytes") {
        config.chunk_bytes = parse_size(require_value(), "chunk_bytes");
      } else if (argument == "--send-bytes") {
        config.send_bytes = parse_size(require_value(), "send_bytes");
      } else {
        throw std::invalid_argument("unknown argument: " + std::string{argument});
      }
    }
    if (!listen_endpoint.is_loopback()) {
      throw std::invalid_argument("the Milestone 1 server only permits loopback listeners");
    }
    if (config.delay > std::chrono::seconds{60} || config.chunk_bytes > 1U * 1'024U * 1'024U) {
      throw std::invalid_argument("delay must be at most 60000 ms and chunk size at most 1048576 bytes");
    }
    if (config.send_bytes > 16U * 1'024U * 1'024U) {
      throw std::invalid_argument("send_bytes must be at most 16777216");
    }

    ::signal(SIGINT, handle_signal);
    ::signal(SIGTERM, handle_signal);
    ::signal(SIGPIPE, SIG_IGN);

    netfault::UniqueFd listener{::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)};
    if (!listener) {
      throw std::runtime_error("socket: " + std::string{std::strerror(errno)});
    }
    int enabled = 1;
    if (::setsockopt(listener.get(), SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) < 0) {
      throw std::runtime_error("setsockopt: " + std::string{std::strerror(errno)});
    }
    const auto address = listen_endpoint.to_sockaddr();
    if (::bind(listener.get(), reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0 ||
        ::listen(listener.get(), 128) < 0) {
      throw std::runtime_error("bind/listen: " + std::string{std::strerror(errno)});
    }

    std::cout << "{\"event\":\"server_started\",\"listen\":\"" << listen_endpoint.to_string()
              << "\",\"mode\":\"" << mode_name(config.mode) << "\",\"delay_ms\":"
              << config.delay.count() << ",\"chunk_bytes\":" << config.chunk_bytes << "}\n" << std::flush;
    std::vector<std::jthread> workers;
    while (stop_requested == 0) {
      netfault::UniqueFd client{::accept4(listener.get(), nullptr, nullptr, SOCK_CLOEXEC)};
      if (client) {
        workers.emplace_back(serve_client, std::move(client), config);
        continue;
      }
      if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
        continue;
      }
      throw std::runtime_error("accept: " + std::string{std::strerror(errno)});
    }
    for (auto& worker : workers) {
      worker.request_stop();
    }
    workers.clear();
    std::cout << "{\"event\":\"server_stopped\"}\n" << std::flush;
    return EXIT_SUCCESS;
  } catch (const std::exception& error) {
    std::cerr << "netfault-server: " << error.what() << '\n';
    return EXIT_FAILURE;
  }
}
