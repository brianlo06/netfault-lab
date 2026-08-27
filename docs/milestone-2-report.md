# Milestone 2 Report — Bounded Buffers and Backpressure Hardening

Date: August 27, 2026

## Outcome

Milestone 2 hardened the forwarding path around bounded buffers and made its failure behavior testable:

- Per-connection forwarding logic extracted from the epoll event loop into a `Relay` class with directional queues, backpressure trackers, half-close flags, and byte accounting behind one interface.
- A `SocketIo` seam between the relay and the kernel, so unit tests can deterministically force partial transfers, `EAGAIN`, EOF, and resets that Milestone 1 could not produce on demand.
- Configurable high/low watermarks with `read_paused`/`read_resumed` events and pause/resume/saturation counters plus cumulative paused duration per direction.
- Controlled-server modes `slow-reader`, `slow-writer`, `read-until-eof`, and `send-then-half-close` for driving both half-close directions and sustained saturation.
- A `--socket-buffer-bytes` proxy option that constrains the upstream socket's `SO_SNDBUF`/`SO_RCVBUF`, so the upstream path chokes while client-side inflow stays at kernel defaults and proxy queues genuinely fill.
- A `SIGUSR1` metrics snapshot that copies event-loop-owned counters into log events for the listener and every live connection. No payload bytes are ever included.
- Integration tests for both half-close directions, refused upstream, abrupt endpoint termination, OS-level forced partial writes, backpressure under a slow reader, and a five-round connection churn test asserting file descriptors return to baseline and resident memory plateaus.

This milestone does **not** implement fault injection, timer scheduling, configuration files, benchmark statistics, or the dashboard.

## Design notes

### Relay extraction

`src/proxy/proxy.cpp` previously held connection I/O, state transitions, and the epoll runtime in one 561-line file. The split is now:

- `include/netfault/relay.hpp` / `src/common/relay.cpp` — the per-connection forwarding engine. It never owns file descriptors and never touches epoll; socket access goes through `SocketIo`, close decisions are returned as `PumpResult` values instead of being executed, and desired epoll interests are reported as an I/O-free `InterestSet`.
- `include/netfault/socket_io.hpp` / `src/common/socket_io.cpp` — `SystemSocketIo` wraps nonblocking `recv`/`send`/`shutdown` with `EINTR` retries and `MSG_NOSIGNAL`.
- `src/proxy/proxy.cpp` — retains listener setup, `signalfd`, epoll registration/token mapping, accept, upstream-connect completion, and connection teardown.

Log events, counters, and close-detail formats are unchanged, so Milestone 1 integration assertions still pass against the refactored proxy.

### Forced partial writes

The Milestone 1 report left one verification item unproven: a deterministic positive short `send()`. Two mechanisms now cover it:

1. **Unit level (deterministic):** `tests/unit/relay_test.cpp` scripts a `FakeSocketIo` whose send steps transfer at most N bytes per call, proving the flush loop retries short writes until the queue drains, preserves queued bytes across `EAGAIN`, and counts `partial_writes` correctly.
2. **OS level:** loopback TCP copies up to its ~64 KiB size goal per `send()` before checking send-buffer memory, so a span at or below that either fully copies or returns `EAGAIN` — this is why Milestone 1 never observed a kernel short write. The `forced_partial_writes` scenario constrains the upstream socket to a 4 KiB `SO_SNDBUF` while giving the proxy a 256 KiB queue; unthrottled client inflow fills the queue past the size goal and `send()` returns positive short counts, asserted via the `partial_writes` counter.

### Metrics snapshot

`SIGUSR1` was added to the existing `signalfd` mask, so snapshots are serialized with all other event-loop work and copy values only the event loop owns — no locks, no cross-thread access, no payload data. `metrics_snapshot` reports active connections, total accepted, and dropped log events; one `connection_snapshot` per live connection reuses the close-detail counter format with `snapshot=1`.

## Verification

All runs used the Ubuntu 24.04 container (GCC 13.3.0, `-Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion -Wshadow -Werror`).

| Property | Result | Evidence |
|---|---|---|
| Positive short writes retried until drained | Passed | `relay_test.cpp` scripted 700-byte send caps over a 3000-byte queue; OS-level scenario recorded `partial_writes >= 1` with a 4 KiB upstream `SO_SNDBUF` |
| Queued bytes preserved across send `EAGAIN` | Passed | Unit test asserts byte-exact delivery after `WouldBlock`, plus write-interest reporting |
| Pause at high water, resume at low water | Passed | Unit tests observe `read_paused`/`read_resumed` transitions; slow-reader integration records pause/resume/saturation counts and paused duration |
| Client-initiated half-close ordering | Passed | `read-until-eof` scenario asserts `upstream_write` shutdown precedes `client_write` |
| Upstream-initiated half-close ordering | Passed | `send-then-half-close` scenario asserts `client_write` shutdown precedes `upstream_write`, with post-half-close reply bytes still forwarded |
| Refused upstream | Passed | Connect to an unbound port produces logged refusal evidence and client EOF/reset, never a hang |
| Abrupt endpoint termination | Passed | `SIGKILL` of the server mid-transfer closes the connection as `reset`/`failed`; client unblocks |
| Metrics snapshot during transfer | Passed | `SIGUSR1` mid-transfer emits aggregate and per-connection counters bounded by the payload size; the transfer still completes byte-exact |
| FD plateau over churn | Passed | Five rounds of 16 connections; proxy FD count returns to baseline after every round |
| RSS plateau over churn | Passed | First-to-last round VmRSS growth bounded under the ASan allocator |
| Memory errors/leaks in executed paths | No findings | ASan+UBSan full suite passed three consecutive runs |
| Thread data races in executed paths | No findings | TSan full suite passed |

### Flakes found and fixed during this milestone

- Polling the proxy log while the proxy is mid-`write()` can observe a partially-completed line; the shared harness now skips unparseable lines while polling (complete once the process exits).
- The harness's port-readiness probe creates a real zero-byte proxied connection; assertions now identify workload connections by their byte counters instead of assuming a single close event.
- Constraining both proxy sockets' buffers throttled inflow to match outflow, so queues equilibrated near 32 KiB and kernel partial writes never occurred; `--socket-buffer-bytes` therefore applies to the upstream socket only.

## Known limitations

- Only numeric IPv4 is supported.
- Watermarks are global per proxy, not per connection or per direction.
- The snapshot is delivered via log events; there is no queryable endpoint or on-disk metrics file.
- Snapshot delivery requires `SIGUSR1`; multiple rapid signals coalesce under `signalfd` semantics.
- The churn test's RSS bound is generous (16 MiB) because sanitizer allocators are noisy; a Release-build plateau measurement has not been recorded.
- Nonblocking logs may still be dropped when the sink stalls; only the drop count is reported.
- No connect, idle, drain, or workload timeouts exist inside the proxy.
- No benchmark or performance claim has been made.

## Recommended next milestone

Proceed to **Milestone 3: fault policies and `timerfd` scheduling** per the design document:

1. One nonblocking `timerfd` registered with epoll plus a min-heap of deadlines keyed by `(deadline, sequence)`.
2. Latency injection holding bytes in bounded queue segments with an `eligible_at` time; the budget must include scheduled bytes so delayed data cannot grow unbounded.
3. Fault policy configuration on the CLI first; YAML only after the policy shape stabilizes.
4. Fake-clock unit tests so every fault is reproducible without wall-clock sleeps.
5. Connect/idle/drain timeouts as the first consumers of the timer wheel.

Do not add benchmark claims, YAML, or the dashboard until those tests pass.
