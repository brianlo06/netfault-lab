#pragma once

#include <chrono>
#include <cstdint>

namespace netfault {

// Deterministic fault randomness. The documented mixing function is SplitMix64
// (Steele/Lea/Flood): every per-connection/per-direction stream is derived from
// the master seed by pure arithmetic, so results never depend on container
// iteration order or wall-clock time.
[[nodiscard]] constexpr std::uint64_t splitmix64_mix(std::uint64_t value) noexcept {
  value += 0x9E3779B97F4A7C15ULL;
  value = (value ^ (value >> 30U)) * 0xBF58476D1CE4E5B9ULL;
  value = (value ^ (value >> 27U)) * 0x94D049BB133111EBULL;
  return value ^ (value >> 31U);
}

class SplitMix64 {
 public:
  explicit constexpr SplitMix64(std::uint64_t seed) noexcept : state_(seed) {}

  constexpr std::uint64_t next() noexcept {
    state_ += 0x9E3779B97F4A7C15ULL;
    std::uint64_t value = state_;
    value = (value ^ (value >> 30U)) * 0xBF58476D1CE4E5B9ULL;
    value = (value ^ (value >> 27U)) * 0x94D049BB133111EBULL;
    return value ^ (value >> 31U);
  }

 private:
  std::uint64_t state_;
};

// Seed derivation: master -> connection -> direction, one mix step per level.
[[nodiscard]] constexpr std::uint64_t derive_connection_seed(std::uint64_t master_seed,
                                                             std::uint64_t connection_id) noexcept {
  return splitmix64_mix(splitmix64_mix(master_seed) ^ connection_id);
}

[[nodiscard]] constexpr std::uint64_t derive_direction_seed(std::uint64_t connection_seed,
                                                            std::uint64_t direction_index) noexcept {
  return splitmix64_mix(connection_seed ^ (direction_index + 1));
}

// Uniform double in [0, 1) from the top 53 bits.
[[nodiscard]] constexpr double sample_unit_interval(std::uint64_t random_bits) noexcept {
  return static_cast<double>(random_bits >> 11U) * 0x1.0p-53;
}

// Uniform jitter in [-jitter, +jitter], the documented v1 distribution. Modulo
// bias is accepted and documented: determinism matters here, not statistical
// perfection, and the bias is negligible for millisecond-scale ranges.
[[nodiscard]] constexpr std::chrono::nanoseconds sample_jitter(SplitMix64& rng,
                                                               std::chrono::nanoseconds jitter) noexcept {
  if (jitter <= std::chrono::nanoseconds::zero()) {
    return std::chrono::nanoseconds::zero();
  }
  const auto range = static_cast<std::uint64_t>(jitter.count()) * 2 + 1;
  const auto offset = static_cast<std::int64_t>(rng.next() % range);
  return std::chrono::nanoseconds{offset - jitter.count()};
}

}  // namespace netfault
