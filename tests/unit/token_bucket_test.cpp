#include "netfault/token_bucket.hpp"

#include <catch2/catch_test_macros.hpp>

#include <chrono>

namespace {

using namespace std::chrono_literals;

std::chrono::steady_clock::time_point base() {
  return std::chrono::steady_clock::time_point{} + 1000s;
}

}  // namespace

TEST_CASE("token bucket starts full and consumes tokens") {
  netfault::TokenBucket bucket{10'000, 4'096, base()};
  CHECK(bucket.available(base()) == 4'096);
  bucket.consume(4'096);
  CHECK(bucket.available(base()) == 0);
}

TEST_CASE("token bucket refills at the configured rate") {
  netfault::TokenBucket bucket{10'000, 8'192, base()};
  bucket.consume(8'192);

  CHECK(bucket.available(base() + 100ms) == 1'000);
  CHECK(bucket.available(base() + 500ms) == 5'000);
  CHECK(bucket.available(base() + 819200us + 1s) == 8'192);  // never above burst
}

TEST_CASE("token bucket carries sub-byte remainders exactly") {
  // 3 bytes/second: after 1s, exactly 1 token would be wrong; remainder math
  // must accumulate so 10 seconds yields exactly 30 tokens.
  netfault::TokenBucket bucket{3, 1'000, base()};
  bucket.consume(1'000);
  auto now = base();
  std::size_t total = 0;
  for (int step = 0; step < 30; ++step) {
    now += 333ms;  // deliberately misaligned with byte boundaries
    total = bucket.available(now);
  }
  // 30 * 333ms = 9.99s at 3 bytes/s = 29.97 bytes -> floor 29 with exact carry
  CHECK(total == 29);
  CHECK(bucket.available(now + 10ms) == 30);
}

TEST_CASE("token bucket eligible_at reports when tokens will exist") {
  netfault::TokenBucket bucket{1'000, 2'000, base()};
  bucket.consume(2'000);

  const auto when = bucket.eligible_at(500, base());
  CHECK(when == base() + 500ms);
  CHECK(bucket.available(when) == 500);

  // Requests above burst are clamped to burst (fresh bucket: the available()
  // call above already refilled this one).
  netfault::TokenBucket drained{1'000, 2'000, base()};
  drained.consume(2'000);
  CHECK(drained.eligible_at(1'000'000, base()) == base() + 2s);
}

TEST_CASE("token bucket eligible_at is immediate when tokens suffice") {
  netfault::TokenBucket bucket{1'000, 2'000, base()};
  CHECK(bucket.eligible_at(2'000, base()) == base());
}

TEST_CASE("token bucket survives long idle without overflow") {
  netfault::TokenBucket bucket{1'000'000'000, 16U * 1024U * 1024U, base()};
  bucket.consume(1024);
  CHECK(bucket.available(base() + 24h) == 16U * 1024U * 1024U);
}

TEST_CASE("token bucket rejects invalid configuration and overdraw") {
  CHECK_THROWS(netfault::TokenBucket(0, 100, base()));
  CHECK_THROWS(netfault::TokenBucket(100, 0, base()));
  netfault::TokenBucket bucket{100, 100, base()};
  CHECK_THROWS(bucket.consume(101));
}
