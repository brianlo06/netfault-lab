# Milestone 1 Report — Basic TCP Forwarding

Date: August 26, 2026

## Outcome

Milestone 1 now provides a safe, Linux-only C++20 forwarding baseline:

- Numeric IPv4 listener and upstream parsing.
- Loopback defaults and explicit, separate unsafe flags for non-loopback proxy listening/upstream access.
- Nonblocking listener, accepted client sockets, and upstream sockets.
- Level-triggered `epoll` with reads/writes drained to `EAGAIN` and dynamic `EPOLLIN`/`EPOLLOUT` interests.
- Nonblocking upstream `connect()` completion checked through `SO_ERROR`.
- One connection object owning client/upstream file descriptors and both directional queues.
- Fixed-capacity circular queues allocated per direction and read pausing when full.
- Orderly EOF and half-close propagation after buffered data drains.
- Opaque epoll tokens that make stale events safe after connection removal.
- RAII file descriptor ownership and `signalfd`-driven `SIGINT`/`SIGTERM` shutdown.
- Nonblocking JSON Lines connection logging that cannot stall the event loop.
- Controlled loopback echo server and deterministic full-duplex binary test client.
- Catch2 unit tests plus a process-level Python integration test.

This milestone does **not** implement configurable faults, timer scheduling, benchmark statistics, a metrics endpoint, or the dashboard.

## Files created or modified

### Build and project controls

- `CMakeLists.txt`
- `CMakePresets.json`
- `containers/Dockerfile`
- `.dockerignore`
- `.gitignore`
- `LICENSE`
- `SECURITY.md`
- `CONTRIBUTING.md`

### Public interfaces

- `include/netfault/unique_fd.hpp`
- `include/netfault/byte_queue.hpp`
- `include/netfault/endpoint.hpp`
- `include/netfault/logger.hpp`
- `include/netfault/proxy.hpp`

### Implementations

- `src/common/endpoint.cpp`
- `src/common/logger.cpp`
- `src/proxy/proxy.cpp`
- `src/proxy/main.cpp`
- `src/server/main.cpp`
- `src/client/main.cpp`

### Tests and documentation

- `tests/unit/byte_queue_test.cpp`
- `tests/unit/endpoint_test.cpp`
- `tests/integration/milestone1.py`
- `README.md`
- `docs/milestone-0-design.md`
- `docs/milestone-1-report.md`

## Build and test commands

Authoritative Linux container build:

```bash
docker build -f containers/Dockerfile -t netfault-lab:m1 .
```

Default container test (ASan+UBSan):

```bash
docker run --rm netfault-lab:m1
```

Complete final matrix used in this milestone:

```bash
docker run --rm netfault-lab:m1 bash -lc '
  ctest --preset asan-ubsan &&
  cmake --preset release &&
  cmake --build --preset release --parallel 2 &&
  ctest --test-dir build/release --output-on-failure &&
  cmake --preset tsan &&
  cmake --build --preset tsan --parallel 2 &&
  ctest --preset tsan
'
```

Repeatability check:

```bash
docker run --rm netfault-lab:m1 bash -lc '
  for run in 1 2 3 4 5 6 7 8 9 10; do
    ctest --test-dir build/asan-ubsan -R milestone1-integration --output-on-failure >/dev/null || exit 1
  done
'
```

## Actual final test results

The final matrix was run successfully in the Ubuntu 24.04 `aarch64` container with GCC 13.3.0 and Catch2 3.4.0.

| Build/test mode | Result | Actual final CTest time |
|---|---|---:|
| ASan + UBSan | 2/2 tests passed | 0.52 s |
| Release (`-O3`, strict warnings as errors) | 2/2 tests passed | 0.21 s |
| TSan | 2/2 tests passed | 0.57 s |
| ASan+UBSan integration repeatability | 10/10 consecutive runs passed | individual output suppressed after failure gating |

The integration test starts separate server/proxy/client processes, sends eight concurrent deterministic 1 MiB arbitrary binary payloads through the proxy, validates exact echo equality, performs orderly shutdown, and checks `/proc/<proxy-pid>/fd` until the proxy returns to its pre-workload file-descriptor baseline. All subprocesses have timeouts.

These are correctness/test-run results, not performance benchmarks. The measured CTest times are not presented as proxy performance.

## Safety checks actually run

The built CLIs were executed with non-loopback values and behaved as intended:

- Proxy non-loopback listener failed without `--unsafe-allow-non-loopback-listen` and printed a warning/error.
- Proxy non-loopback upstream failed without `--unsafe-allow-non-loopback-upstream` and printed a warning/error.
- Controlled server rejected a non-loopback listener.
- Deterministic client rejected a non-loopback destination.
- `--help` completed successfully for all three executables.

No privileged command, firewall change, external destination, payload capture, or `tc netem` operation was used.

## Failures found during implementation

1. **Strict compiler error:** the first Linux build used `std::signal` while including the C signal header. It was corrected, and the server now uses `sig_atomic_t` in the minimal signal handler.
2. **Full-duplex test-client deadlock risk:** the first client sent an entire payload before reading its echo. Bounded proxy/server/client buffers can legitimately create a circular wait. The client now reads and writes concurrently, matching TCP full-duplex semantics.
3. **Event-loop logging stall:** integration tests initially completed only six of eight transfers because the proxy synchronously wrote JSON logs to an undrained pipe. Logging now uses nonblocking writes and counts dropped events instead of blocking forwarding.
4. **Release-only warning:** the optimized build caught an ignored fortified `read(signalfd)` result. The result is now handled explicitly.

These failures are retained here because they are useful interview evidence about backpressure, event-loop discipline, and why multiple build modes matter.

## Acceptance criteria status

| Criterion | Status | Evidence |
|---|---|---|
| Arbitrary binary forwarding | Passed for tested scope | Eight concurrent deterministic 1 MiB exact round trips |
| Large payload spans multiple queue/socket operations | Passed | 1 MiB per connection through 64 KiB user queues |
| Multiple simultaneous connections | Passed for tested scope | Eight concurrent connections; ten repeated runs |
| Memory errors/leaks in executed paths | No findings | ASan+UBSan final suite passed |
| Thread data races in executed paths | No findings | TSan final suite passed |
| File-descriptor cleanup | Passed for tested scope | `/proc` FD count returns to baseline after workload |
| Clean `SIGTERM` shutdown | Passed | Integration harness terminates and waits for proxy/server without forced kill |
| Loopback-safe defaults | Passed | CLI safety checks and default configuration |
| Strict warning-free Release build | Passed | GCC 13.3.0, `-Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion -Wshadow -Werror` |
| Forced OS-level partial-write path | **Not yet proven** | Code handles returned byte counts, but the current test does not deterministically force `send()` to return a positive short write |

Because the final item remains unproven, public documentation should say “correct partial-I/O loops are implemented” rather than claiming exhaustive partial-write validation. Milestone 2 should add a controlled small socket-buffer/slow-reader test and assert partial-write or `EAGAIN` metrics.

## Known limitations

- Only numeric IPv4 is supported.
- The proxy event loop remains in one 561-line implementation file; split connection I/O/state transitions from runtime orchestration during Milestone 2 after tests define the boundaries.
- The controlled server supports only echo mode.
- Per-connection logs contain useful final counters, but no queryable metrics snapshot exists.
- Nonblocking logs may be dropped when the sink stalls; the final event reports the number only if that event itself can be written.
- Read resumption occurs whenever any queue capacity is available. Configurable high/low watermarks and read-pause duration metrics are not implemented.
- No connect, idle, drain, or workload timeouts exist inside the proxy; only the test harness and controlled endpoints have timeouts.
- No deterministic forced partial-write, reset, refused-upstream, client-only half-close, or server-only half-close integration case exists yet.
- No benchmark or performance claim has been made.

## Recommended next milestone

Proceed to **Milestone 2: bounded buffers and backpressure hardening**, without adding fault injection yet.

Recommended order:

1. Extract connection lifecycle/directional I/O from `proxy.cpp` behind testable interfaces.
2. Add configurable high/low watermarks and explicit read-pause/resume events/counters.
3. Add server modes for slow reader, slow writer, read-until-EOF, and send-then-half-close.
4. Add integration tests for both half-close directions, queue saturation, upstream refusal, abrupt endpoint termination, and deterministic forced partial writes/`EAGAIN`.
5. Add a JSON metrics snapshot mechanism that copies event-loop-owned values without exposing payloads.
6. Measure FD and RSS plateau over repeated connection creation/destruction before accepting Milestone 2.

Do not add latency, jitter, token buckets, YAML, or a dashboard until those tests pass.
