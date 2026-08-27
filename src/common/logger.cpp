#include "netfault/logger.hpp"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <string>

namespace netfault {
namespace {

std::string escape_json(std::string_view value) {
  std::string escaped;
  escaped.reserve(value.size());
  for (const char character : value) {
    switch (character) {
      case '\\':
        escaped += "\\\\";
        break;
      case '"':
        escaped += "\\\"";
        break;
      case '\n':
        escaped += "\\n";
        break;
      case '\r':
        escaped += "\\r";
        break;
      case '\t':
        escaped += "\\t";
        break;
      default:
        if (static_cast<unsigned char>(character) >= 0x20U) {
          escaped += character;
        }
        break;
    }
  }
  return escaped;
}

}  // namespace

Logger::Logger() {
  const int flags = ::fcntl(STDOUT_FILENO, F_GETFL, 0);
  if (flags >= 0) {
    static_cast<void>(::fcntl(STDOUT_FILENO, F_SETFL, flags | O_NONBLOCK));
  }
}

void Logger::event(std::string_view event_name, std::uint64_t connection_id,
                   std::string_view state, std::string_view detail) const {
  const auto now = std::chrono::system_clock::now().time_since_epoch();
  const auto timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
  std::string line = "{\"timestamp_ms\":" + std::to_string(timestamp_ms) + ",\"event\":\"" +
                     escape_json(event_name) + "\",\"connection_id\":" +
                     std::to_string(connection_id) + ",\"state\":\"" + escape_json(state) + "\"";
  if (!detail.empty()) {
    line += ",\"detail\":\"" + escape_json(detail) + "\"";
  }
  line += "}\n";

  ssize_t result = -1;
  do {
    result = ::write(STDOUT_FILENO, line.data(), line.size());
  } while (result < 0 && errno == EINTR);
  if (result < 0 || static_cast<std::size_t>(result) != line.size()) {
    ++dropped_events_;
  }
}

}  // namespace netfault
