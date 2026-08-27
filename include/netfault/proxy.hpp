#pragma once

#include "netfault/endpoint.hpp"

#include <cstddef>
#include <cstdint>

namespace netfault {

struct ProxyConfig {
  Endpoint listen{"127.0.0.1", 8080};
  Endpoint upstream{"127.0.0.1", 9000};
  std::size_t max_connections{256};
  std::size_t buffer_bytes_per_direction{65'536};
  std::size_t low_water_bytes{32'768};
  std::size_t high_water_bytes{65'536};
  bool allow_non_loopback_listen{false};
  bool allow_non_loopback_upstream{false};
};

class Proxy {
 public:
  explicit Proxy(ProxyConfig config);
  int run();

 private:
  ProxyConfig config_;
};

}  // namespace netfault
