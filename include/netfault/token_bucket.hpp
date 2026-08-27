#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stdexcept>

namespace netfault {

// Byte-rate token bucket parameterized on explicit time points so unit tests
// advance time without sleeping. Integer arithmetic only: refill carries the
// sub-byte nanosecond remainder forward, so long-run throughput equals the
// configured rate exactly regardless of refill cadence.
class TokenBucket {
 public:
  using TimePoint = std::chrono::steady_clock::time_point;

  TokenBucket(std::size_t rate_bytes_per_second, std::size_t burst_bytes, TimePoint start)
      : rate_(rate_bytes_per_second), burst_(burst_bytes), tokens_(burst_bytes), last_refill_(start) {
    if (rate_ == 0 || burst_ == 0) {
      throw std::invalid_argument("token bucket rate and burst must be positive");
    }
  }

  [[nodiscard]] std::size_t available(TimePoint now) {
    refill(now);
    return tokens_;
  }

  void consume(std::size_t bytes) {
    if (bytes > tokens_) {
      throw std::out_of_range("token bucket consume exceeds available tokens");
    }
    tokens_ -= bytes;
  }

  // Earliest time at least min(bytes, burst) tokens will exist. May be
  // slightly early from truncation; callers recompute availability on wake.
  [[nodiscard]] TimePoint eligible_at(std::size_t bytes, TimePoint now) {
    refill(now);
    const auto target = std::min(bytes, burst_);
    if (tokens_ >= target) {
      return now;
    }
    const auto needed = static_cast<std::uint64_t>(target - tokens_);
    const auto wait_ns = (needed * 1'000'000'000ULL + rate_ - 1) / rate_;
    return now + std::chrono::nanoseconds{wait_ns};
  }

  [[nodiscard]] std::size_t rate_bytes_per_second() const noexcept { return rate_; }
  [[nodiscard]] std::size_t burst_bytes() const noexcept { return burst_; }

 private:
  void refill(TimePoint now) {
    if (now <= last_refill_) {
      return;
    }
    const auto elapsed_ns =
        static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now - last_refill_).count());
    last_refill_ = now;

    // Cap the elapsed window at the time needed to fill the bucket so the
    // product below cannot overflow after long idle periods.
    const auto missing = static_cast<std::uint64_t>(burst_ - tokens_);
    const auto full_ns = (missing * 1'000'000'000ULL + rate_ - 1) / rate_;
    if (elapsed_ns >= full_ns + 1'000'000'000ULL / rate_ + 1) {
      tokens_ = burst_;
      carry_product_ = 0;
      return;
    }

    // Track sub-byte progress in "byte * 1e9" product space for exactness.
    const auto product = elapsed_ns * rate_ + carry_product_;
    const auto earned = product / 1'000'000'000ULL;
    carry_product_ = product % 1'000'000'000ULL;
    tokens_ = static_cast<std::size_t>(
        std::min<std::uint64_t>(static_cast<std::uint64_t>(tokens_) + earned, burst_));
    if (tokens_ == burst_) {
      carry_product_ = 0;
    }
  }

  std::size_t rate_;
  std::size_t burst_;
  std::size_t tokens_;
  TimePoint last_refill_;
  std::uint64_t carry_product_{0};
};

}  // namespace netfault
