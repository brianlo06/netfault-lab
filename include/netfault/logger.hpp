#pragma once

#include <cstdint>
#include <string_view>

namespace netfault {

class Logger {
 public:
  Logger();

  void event(std::string_view event_name, std::uint64_t connection_id,
             std::string_view state, std::string_view detail = {}) const;

  [[nodiscard]] std::uint64_t dropped_events() const noexcept { return dropped_events_; }

 private:
  mutable std::uint64_t dropped_events_{0};
};

}  // namespace netfault
