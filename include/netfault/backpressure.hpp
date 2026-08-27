#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stdexcept>

namespace netfault {

enum class BackpressureTransition { None, Paused, Resumed };

class BackpressureTracker {
 public:
  using Clock = std::chrono::steady_clock;
  using TimePoint = Clock::time_point;

  BackpressureTracker(std::size_t low_water_bytes, std::size_t high_water_bytes)
      : low_water_bytes_(low_water_bytes), high_water_bytes_(high_water_bytes) {
    if (high_water_bytes == 0 || low_water_bytes >= high_water_bytes) {
      throw std::invalid_argument("backpressure watermarks require 0 <= low < high");
    }
  }

  [[nodiscard]] bool read_paused() const noexcept { return read_paused_; }
  [[nodiscard]] std::size_t low_water_bytes() const noexcept { return low_water_bytes_; }
  [[nodiscard]] std::size_t high_water_bytes() const noexcept { return high_water_bytes_; }
  [[nodiscard]] std::uint64_t pause_count() const noexcept { return pause_count_; }
  [[nodiscard]] std::uint64_t resume_count() const noexcept { return resume_count_; }
  [[nodiscard]] std::uint64_t saturation_count() const noexcept { return saturation_count_; }

  BackpressureTransition observe(std::size_t occupancy, std::size_t capacity, TimePoint now) {
    if (occupancy > capacity || high_water_bytes_ > capacity) {
      throw std::out_of_range("backpressure occupancy or high-water mark exceeds capacity");
    }

    if (occupancy == capacity) {
      if (!at_capacity_) {
        ++saturation_count_;
        at_capacity_ = true;
      }
    } else {
      at_capacity_ = false;
    }

    if (!read_paused_ && occupancy >= high_water_bytes_) {
      read_paused_ = true;
      pause_started_at_ = now;
      ++pause_count_;
      return BackpressureTransition::Paused;
    }
    if (read_paused_ && occupancy <= low_water_bytes_) {
      read_paused_ = false;
      total_paused_ += now - pause_started_at_;
      ++resume_count_;
      return BackpressureTransition::Resumed;
    }
    return BackpressureTransition::None;
  }

  [[nodiscard]] std::chrono::nanoseconds paused_duration(TimePoint now) const noexcept {
    const auto current_pause = read_paused_ ? now - pause_started_at_ : Clock::duration::zero();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(total_paused_ + current_pause);
  }

 private:
  std::size_t low_water_bytes_;
  std::size_t high_water_bytes_;
  bool read_paused_{false};
  bool at_capacity_{false};
  TimePoint pause_started_at_{};
  Clock::duration total_paused_{};
  std::uint64_t pause_count_{0};
  std::uint64_t resume_count_{0};
  std::uint64_t saturation_count_{0};
};

}  // namespace netfault
