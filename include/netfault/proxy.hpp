#pragma once

#include "netfault/endpoint.hpp"
#include "netfault/fault_plan.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>

namespace netfault {

struct ProxyConfig {
  Endpoint listen{"127.0.0.1", 8080};
  Endpoint upstream{"127.0.0.1", 9000};
  std::size_t max_connections{256};
  std::size_t buffer_bytes_per_direction{65'536};
  std::size_t low_water_bytes{32'768};
  std::size_t high_water_bytes{65'536};
  std::size_t socket_buffer_bytes{0};  // 0 keeps the kernel default; small values force partial writes
  std::chrono::milliseconds connect_timeout{0};  // 0 disables
  std::chrono::milliseconds idle_timeout{0};     // 0 disables
  std::string metrics_file;  // empty disables; written atomically on SIGUSR1 and at shutdown
  FaultPlan faults;
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
