#include "netfault/socket_io.hpp"

#include <errno.h>
#include <sys/socket.h>

namespace netfault {

IoResult SystemSocketIo::receive(int fd, std::span<std::byte> buffer) {
  while (true) {
    const auto count = ::recv(fd, buffer.data(), buffer.size(), 0);
    if (count > 0) {
      return {IoStatus::Transferred, static_cast<std::size_t>(count), 0};
    }
    if (count == 0) {
      return {IoStatus::PeerClosed, 0, 0};
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return {IoStatus::WouldBlock, 0, 0};
    }
    return {IoStatus::Error, 0, errno};
  }
}

IoResult SystemSocketIo::send(int fd, std::span<const std::byte> data) {
  while (true) {
    const auto count = ::send(fd, data.data(), data.size(), MSG_NOSIGNAL);
    if (count > 0) {
      return {IoStatus::Transferred, static_cast<std::size_t>(count), 0};
    }
    if (count < 0 && errno == EINTR) {
      continue;
    }
    if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return {IoStatus::WouldBlock, 0, 0};
    }
    return {IoStatus::Error, 0, count < 0 ? errno : EPIPE};
  }
}

bool SystemSocketIo::shutdown_write(int fd) {
  return ::shutdown(fd, SHUT_WR) == 0 || errno == ENOTCONN;
}

}  // namespace netfault
