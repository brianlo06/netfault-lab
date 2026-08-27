#include "netfault/endpoint.hpp"

#include <arpa/inet.h>

#include <charconv>
#include <stdexcept>

namespace netfault {

Endpoint Endpoint::parse(std::string_view text) {
  const auto separator = text.rfind(':');
  if (separator == std::string_view::npos || separator == 0 || separator + 1 >= text.size()) {
    throw std::invalid_argument("endpoint must use numeric IPv4 HOST:PORT syntax");
  }

  Endpoint endpoint{std::string{text.substr(0, separator)}, 0};
  unsigned int parsed_port = 0;
  const auto port_text = text.substr(separator + 1);
  const auto [end, error] = std::from_chars(port_text.data(), port_text.data() + port_text.size(), parsed_port);
  if (error != std::errc{} || end != port_text.data() + port_text.size() || parsed_port == 0 ||
      parsed_port > 65'535U) {
    throw std::invalid_argument("endpoint port must be an integer from 1 to 65535");
  }

  in_addr address{};
  if (::inet_pton(AF_INET, endpoint.host.c_str(), &address) != 1) {
    throw std::invalid_argument("endpoint host must be a numeric IPv4 address");
  }
  endpoint.port = static_cast<std::uint16_t>(parsed_port);
  return endpoint;
}

sockaddr_in Endpoint::to_sockaddr() const {
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  if (::inet_pton(AF_INET, host.c_str(), &address.sin_addr) != 1) {
    throw std::invalid_argument("invalid numeric IPv4 address");
  }
  return address;
}

bool Endpoint::is_loopback() const {
  const auto address = to_sockaddr();
  const auto host_order = ntohl(address.sin_addr.s_addr);
  return (host_order & 0xFF00'0000U) == 0x7F00'0000U;
}

std::string Endpoint::to_string() const { return host + ":" + std::to_string(port); }

}  // namespace netfault
