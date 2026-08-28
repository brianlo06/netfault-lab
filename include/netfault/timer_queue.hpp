#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <queue>
#include <vector>

namespace netfault {

enum class TimerKind { Pump, IdleTimeout, ConnectTimeout };

struct TimerEvent {
  std::chrono::steady_clock::time_point deadline;
  std::uint64_t sequence;  // stable tie-break for equal deadlines
  std::uint64_t connection_id;
  TimerKind kind;
};

// Min-heap of deadlines keyed by (deadline, sequence). The queue never sleeps
// and never touches file descriptors; the caller arms one timerfd from
// next_deadline() and validates popped events against live connections, so
// stale entries for removed connections are discarded lazily and can never
// reach freed memory.
class TimerQueue {
 public:
  void schedule(std::chrono::steady_clock::time_point deadline, std::uint64_t connection_id,
                TimerKind kind) {
    heap_.push(TimerEvent{deadline, next_sequence_++, connection_id, kind});
  }

  // GCC 13 at -O3 raises a false-positive -Wstringop-overflow inside the
  // libstdc++ heap routines inlined from priority_queue::pop (the analyzer
  // loses track of the vector's real size); suppressed narrowly here.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstringop-overflow"
  [[nodiscard]] std::optional<TimerEvent> pop_due(std::chrono::steady_clock::time_point now) {
    if (heap_.empty() || heap_.top().deadline > now) {
      return std::nullopt;
    }
    TimerEvent event = heap_.top();
    heap_.pop();
    return event;
  }
#pragma GCC diagnostic pop

  [[nodiscard]] std::optional<std::chrono::steady_clock::time_point> next_deadline() const {
    if (heap_.empty()) {
      return std::nullopt;
    }
    return heap_.top().deadline;
  }

  [[nodiscard]] std::size_t size() const noexcept { return heap_.size(); }
  [[nodiscard]] bool empty() const noexcept { return heap_.empty(); }

 private:
  struct Later {
    [[nodiscard]] bool operator()(const TimerEvent& left, const TimerEvent& right) const noexcept {
      if (left.deadline != right.deadline) {
        return left.deadline > right.deadline;
      }
      return left.sequence > right.sequence;
    }
  };

  std::priority_queue<TimerEvent, std::vector<TimerEvent>, Later> heap_;
  std::uint64_t next_sequence_{1};
};

}  // namespace netfault
