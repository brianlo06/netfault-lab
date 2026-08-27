#include "netfault/byte_queue.hpp"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <cstddef>

TEST_CASE("ByteQueue enforces capacity and preserves wrapped byte order") {
  netfault::ByteQueue queue{5};

  auto first_write = queue.writable_span();
  REQUIRE(first_write.size() == 5);
  first_write[0] = std::byte{1};
  first_write[1] = std::byte{2};
  first_write[2] = std::byte{3};
  queue.commit_write(3);
  queue.consume(2);

  auto second_write = queue.writable_span();
  REQUIRE(second_write.size() == 2);
  second_write[0] = std::byte{4};
  second_write[1] = std::byte{5};
  queue.commit_write(2);

  auto first_read = queue.readable_span();
  REQUIRE(first_read.size() == 3);
  CHECK(first_read[0] == std::byte{3});
  CHECK(first_read[1] == std::byte{4});
  CHECK(first_read[2] == std::byte{5});
  CHECK(queue.high_water_mark() == 3);
}

TEST_CASE("ByteQueue rejects invalid accounting") {
  netfault::ByteQueue queue{4};
  CHECK_THROWS_AS(queue.commit_write(5), std::out_of_range);
  CHECK_THROWS_AS(queue.consume(1), std::out_of_range);
}
