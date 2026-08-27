#pragma once

#include <cstddef>
#include <span>

namespace netfault {

enum class IoStatus { Transferred, WouldBlock, PeerClosed, Error };

struct IoResult {
  IoStatus status{IoStatus::WouldBlock};
  std::size_t bytes{0};
  int error{0};
};

// Seam between connection forwarding logic and the kernel socket calls so tests
// can force partial transfers, EAGAIN, EOF, and resets deterministically.
class SocketIo {
 public:
  SocketIo() = default;
  SocketIo(const SocketIo&) = delete;
  SocketIo& operator=(const SocketIo&) = delete;
  virtual ~SocketIo() = default;

  [[nodiscard]] virtual IoResult receive(int fd, std::span<std::byte> buffer) = 0;
  [[nodiscard]] virtual IoResult send(int fd, std::span<const std::byte> data) = 0;
  [[nodiscard]] virtual bool shutdown_write(int fd) = 0;
};

// Nonblocking recv/send/shutdown with EINTR retries; never raises SIGPIPE.
class SystemSocketIo final : public SocketIo {
 public:
  [[nodiscard]] IoResult receive(int fd, std::span<std::byte> buffer) override;
  [[nodiscard]] IoResult send(int fd, std::span<const std::byte> data) override;
  [[nodiscard]] bool shutdown_write(int fd) override;
};

}  // namespace netfault
