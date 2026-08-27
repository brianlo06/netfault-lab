#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stdexcept>

namespace netfault {

enum class FaultDirections { Both, ClientToUpstream, UpstreamToClient };

// Validated, immutable fault configuration shared by every connection. Whether
// a given connection applies it is decided once at accept time from the master
// seed and apply_probability; per-direction jitter streams derive from the same
// seed, so a run is fully reproducible from (configuration, master seed).
struct FaultPlan {
  std::uint64_t master_seed{0};
  double apply_probability{1.0};
  FaultDirections directions{FaultDirections::Both};
  std::chrono::milliseconds latency{0};
  std::chrono::milliseconds jitter{0};
  std::size_t rate_bytes_per_second{0};  // 0 disables the token bucket
  std::size_t burst_bytes{0};            // 0 with a rate defaults to one second of tokens
  std::uint64_t reset_after_bytes{0};       // total forwarded bytes, both directions; 0 disables
  std::uint64_t half_close_after_bytes{0};  // bytes written to the client; 0 disables

  [[nodiscard]] bool any_enabled() const noexcept {
    return latency.count() > 0 || jitter.count() > 0 || rate_bytes_per_second > 0 ||
           reset_after_bytes > 0 || half_close_after_bytes > 0;
  }

  [[nodiscard]] bool applies_to_client_to_upstream() const noexcept {
    return directions != FaultDirections::UpstreamToClient;
  }

  [[nodiscard]] bool applies_to_upstream_to_client() const noexcept {
    return directions != FaultDirections::ClientToUpstream;
  }

  [[nodiscard]] std::size_t effective_burst_bytes() const noexcept {
    return burst_bytes != 0 ? burst_bytes : rate_bytes_per_second;
  }

  void validate() const {
    if (apply_probability < 0.0 || apply_probability > 1.0) {
      throw std::invalid_argument("fault probability must be between 0 and 1");
    }
    if (latency < std::chrono::milliseconds::zero() || jitter < std::chrono::milliseconds::zero()) {
      throw std::invalid_argument("fault latency and jitter must be non-negative");
    }
    if (jitter > latency + std::chrono::seconds{10}) {
      throw std::invalid_argument("fault jitter must not exceed latency by more than 10 seconds");
    }
    if (latency + jitter > std::chrono::seconds{60}) {
      throw std::invalid_argument("fault latency plus jitter must be at most 60 seconds");
    }
    if (burst_bytes != 0 && rate_bytes_per_second == 0) {
      throw std::invalid_argument("fault burst requires a fault rate");
    }
    if (rate_bytes_per_second > 1'000'000'000ULL) {
      throw std::invalid_argument("fault rate must be at most 1000000000 bytes per second");
    }
  }
};

}  // namespace netfault
