#pragma once

#include <netinet/in.h>

#include <cstdint>
#include <string>
#include <string_view>

namespace netfault {

struct Endpoint {
  std::string host;
  std::uint16_t port;

  [[nodiscard]] static Endpoint parse(std::string_view text);
  [[nodiscard]] sockaddr_in to_sockaddr() const;
  [[nodiscard]] bool is_loopback() const;
  [[nodiscard]] std::string to_string() const;
};

}  // namespace netfault
