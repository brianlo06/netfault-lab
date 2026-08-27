#pragma once

#include <algorithm>
#include <cstddef>
#include <span>
#include <stdexcept>
#include <vector>

namespace netfault {

class ByteQueue {
 public:
  explicit ByteQueue(std::size_t capacity) : storage_(capacity) {
    if (capacity == 0) {
      throw std::invalid_argument("ByteQueue capacity must be positive");
    }
  }

  [[nodiscard]] std::size_t size() const noexcept { return size_; }
  [[nodiscard]] std::size_t capacity() const noexcept { return storage_.size(); }
  [[nodiscard]] std::size_t available() const noexcept { return capacity() - size_; }
  [[nodiscard]] bool empty() const noexcept { return size_ == 0; }
  [[nodiscard]] bool full() const noexcept { return size_ == capacity(); }

  [[nodiscard]] std::span<std::byte> writable_span() noexcept {
    if (full()) {
      return {};
    }
    const auto tail = (head_ + size_) % capacity();
    const auto contiguous = std::min(available(), capacity() - tail);
    return {storage_.data() + tail, contiguous};
  }

  [[nodiscard]] std::span<const std::byte> readable_span() const noexcept {
    if (empty()) {
      return {};
    }
    const auto contiguous = std::min(size_, capacity() - head_);
    return {storage_.data() + head_, contiguous};
  }

  void commit_write(std::size_t count) {
    if (count > available()) {
      throw std::out_of_range("ByteQueue write exceeds available capacity");
    }
    size_ += count;
    high_water_mark_ = std::max(high_water_mark_, size_);
  }

  void consume(std::size_t count) {
    if (count > size_) {
      throw std::out_of_range("ByteQueue consume exceeds queued bytes");
    }
    head_ = (head_ + count) % capacity();
    size_ -= count;
    if (size_ == 0) {
      head_ = 0;
    }
  }

  [[nodiscard]] std::size_t high_water_mark() const noexcept { return high_water_mark_; }

 private:
  std::vector<std::byte> storage_;
  std::size_t head_{0};
  std::size_t size_{0};
  std::size_t high_water_mark_{0};
};

}  // namespace netfault
