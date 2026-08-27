#pragma once

#include <chrono>
#include <cstddef>
#include <deque>
#include <optional>
#include <stdexcept>

namespace netfault {

// Latency injection bookkeeping for one direction. Bytes stay in the existing
// bounded ByteQueue — this records only (byte count, eligible_at) segments, so
// delay never creates hidden storage and never blocks the event loop. Segment
// eligibility is clamped monotonically non-decreasing: jitter can stretch or
// shrink a segment's delay but can never reorder the byte stream.
class DelayLine {
 public:
  using TimePoint = std::chrono::steady_clock::time_point;

  void on_commit(std::size_t bytes, TimePoint eligible_at) {
    if (bytes == 0) {
      return;
    }
    if (eligible_at < last_eligible_) {
      eligible_at = last_eligible_;
    }
    last_eligible_ = eligible_at;
    if (!segments_.empty() && segments_.back().eligible_at == eligible_at) {
      segments_.back().bytes += bytes;
    } else {
      segments_.push_back({bytes, eligible_at});
    }
    pending_ += bytes;
  }

  // Bytes at the front of the stream whose eligibility has passed.
  [[nodiscard]] std::size_t releasable(TimePoint now) const {
    std::size_t total = 0;
    for (const auto& segment : segments_) {
      if (segment.eligible_at > now) {
        break;
      }
      total += segment.bytes;
    }
    return total;
  }

  // The caller must only consume bytes reported releasable.
  void on_consume(std::size_t bytes) {
    if (bytes > pending_) {
      throw std::out_of_range("delay line consume exceeds pending bytes");
    }
    pending_ -= bytes;
    while (bytes > 0) {
      auto& front = segments_.front();
      const auto taken = bytes < front.bytes ? bytes : front.bytes;
      front.bytes -= taken;
      bytes -= taken;
      if (front.bytes == 0) {
        segments_.pop_front();
      }
    }
  }

  // Eligibility time of the first still-blocked segment, if any.
  [[nodiscard]] std::optional<TimePoint> next_eligible(TimePoint now) const {
    for (const auto& segment : segments_) {
      if (segment.eligible_at > now) {
        return segment.eligible_at;
      }
    }
    return std::nullopt;
  }

  [[nodiscard]] std::size_t pending() const noexcept { return pending_; }

 private:
  struct Segment {
    std::size_t bytes;
    TimePoint eligible_at;
  };

  std::deque<Segment> segments_;
  std::size_t pending_{0};
  TimePoint last_eligible_{};
};

}  // namespace netfault
