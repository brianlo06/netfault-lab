# Milestone 3 Report — Fault Policies and Timer Scheduling

Date: August 27, 2026

## Outcome

Milestone 3 added deterministic fault injection and timer-driven lifecycle policy without ever letting the event loop sleep:

- One nonblocking `CLOCK_MONOTONIC` `timerfd` registered with epoll, armed in absolute time from a min-heap of deadlines keyed `(deadline, sequence)` with a stable tie-break. Stale heap entries for removed connections are discarded lazily by connection-id lookup and can never reach freed memory.
- Latency and jitter injection: forwarded bytes stay in the existing bounded queues and a per-direction `DelayLine` records `(bytes, eligible_at)` segments, so delay creates no hidden storage. Jitter is uniform in `[-jitter, +jitter]`, clamped so total delay never goes negative, and segment eligibility is clamped monotonically so jitter can never reorder the byte stream.
- Token-bucket rate limiting per direction with integer-only refill that carries sub-byte remainders in product space, making long-run throughput exactly the configured rate.
- Lifecycle fault injection: connection reset after a total forwarded-byte budget, and half-close toward the client after a client-delivered-byte budget.
- Connect and idle timeouts as the first timer consumers, closing connections in a distinct `timed_out` state.
- Deterministic seeding: a documented SplitMix64 mixing chain derives per-connection and per-direction streams from one master seed. Whether each connection receives faults is sampled once at accept and logged as a `fault_config` event, so a run is reproducible from configuration plus seed.
- CLI configuration for all of the above (`--fault-*`, `--connect-timeout-ms`, `--idle-timeout-ms`). YAML remains deferred until the policy shape stabilizes, per the design document.

This milestone does **not** implement stall/starvation faults, YAML configuration, benchmark statistics, latency percentiles, or the dashboard.

## Design notes

### No clocks below the event loop

The `Relay` no longer reads a clock anywhere: `handle_readable`, `flush`, `desired_interest`, `next_wake`, and `close_detail` all take the current time as a parameter, and the runtime captures one timestamp per event batch. `DelayLine`, `TokenBucket`, and `TimerQueue` are likewise pure functions of explicit time points. This is the "controllable clock" the design called for, achieved without a clock interface: unit tests simply pass fabricated time points, so every fault behavior — a 50 ms hold, a 500 ms token refill, a same-seed jitter stream — is asserted without one real sleep.

One consequence is semantic: a backpressure pause and resume that complete within a single event batch now record zero paused duration, because both observations carry the batch timestamp. The Milestone 2 assertion was updated accordingly; pause/resume/saturation counts remain the evidence.

### Blocked-on-time versus blocked-on-socket

A destination can now be unwritable for two different reasons, and they use different wake mechanisms:

- Blocked on the socket (`EAGAIN`): `EPOLLOUT` interest is requested, as before.
- Blocked purely on time (delay not yet eligible, bucket empty): `EPOLLOUT` is suppressed — a writable socket the proxy refuses to write would busy-loop level-triggered epoll — and `Relay::next_wake()` reports the earliest useful time. The runtime schedules a `Pump` timer entry, deduplicated per connection so repeated pumps cannot flood the heap.

### Fault-application decision

`fault_config` is logged for every accepted connection when faults are configured, recording the derived connection seed and whether the plan applied. The decision samples the first SplitMix64 output of the derived seed against `--fault-probability`, so it depends only on the master seed and the connection id — verified across full process restarts in integration.

## Verification

All runs used the Ubuntu 24.04 container (GCC 13.3.0, full warning set with `-Werror`).

| Property | Result | Evidence |
|---|---|---|
| Heap orders by deadline with stable tie-break | Passed | `timer_queue_test.cpp` equal-deadline ordering |
| Token refill exact under misaligned cadence | Passed | 3 B/s bucket sampled at 333 ms steps yields floor-exact totals; long-idle overflow guard test |
| Delay releases at eligibility, never reorders | Passed | `delay_line_test.cpp` including negative-jitter clamp case |
| Seed derivation deterministic and distinguishing | Passed | `fault_rng_test.cpp` |
| Latency holds bytes and reports wake times | Passed | Relay unit test at fabricated times; integration: 200 ms/direction dominates a 64 KiB echo (≥ 380 ms observed) |
| Same-seed jitter streams identical | Passed | Relay unit test comparing delay budgets across equal and different connection ids |
| Token bucket paces and recovers | Passed | Relay unit test; integration: 150 KB at 100 KB/s takes ≥ 1.3 s |
| Reset injection at byte budget | Passed | Client observes reset/EOF; log shows `fault_injected fault=reset` and a `reset` close ≥ the budget |
| Half-close injection | Passed | Client sees mid-stream EOF between threshold and payload size; connection still drains to an orderly close |
| Fault application reproducible from seed | Passed | 16 sequential connections at probability 0.5: identical applied bitmaps across runs for the same seed, different for a different seed |
| Idle timeout | Passed | Quiet connection closes as `timed_out`/`idle_timeout` near the configured time; the client's blocking read unblocks |
| Connect timeout | Passed | SYN-queue-saturated upstream trips `timed_out`/`connect_timeout` |
| Event loop never sleeps | Passed by construction | No sleep/poll timeout in the proxy; all waiting is epoll on sockets, signalfd, and timerfd |
| Memory errors/leaks in executed paths | No findings | Six consecutive full ASan+UBSan suites passed |
| Thread data races in executed paths | No findings | TSan suite passed |

### Flake found and fixed during this milestone

The stability sweep surfaced a harness bug affecting every polling integration test since Milestone 2: tests poll the proxy log through the same Python file object whose descriptor the proxy inherited as stdout. Both handles share one file description — one offset — so the harness's `seek(0)` rewound the proxy's write position and the next log line overwrote the start of the file, producing phantom event reorderings (a later-timestamped event physically earlier in the file, `proxy_started` overwritten). `read_events` now opens an independent file description over the same inode (`/proc/self/fd` reopen) and never touches the writer's offset. Reproduced roughly 1-in-10 runs before the fix; zero in 40 targeted attempts and six consecutive full suites after.

## Known limitations

- Faults are proxy-global: every fault-applied connection gets the same plan. Per-connection policy variation beyond the probability gate is future work.
- Jitter chunking follows kernel read chunk boundaries, so the *number* of jitter samples per connection is timing-dependent even though the sample stream is deterministic. Cross-run delay-budget equality is therefore not asserted at the integration level (the unit level asserts it with fixed chunking).
- No stall (indefinite hold) fault, no scheduled fault expiry, and no mid-connection policy changes.
- Timeouts are two fixed values; there is no drain deadline distinct from idle timeout during shutdown.
- `--fault-probability` gates the whole plan; individual faults cannot be sampled independently.
- Still numeric IPv4 only, level-triggered epoll only, log-event metrics only.
- No benchmark or performance claim has been made.

## Recommended next milestone

Proceed to **Milestone 4: metrics export and reproducible benchmark client** per the design document:

1. A machine-readable metrics snapshot DTO (JSON file or endpoint) copied from event-loop-owned values, replacing log-scraping for tooling.
2. Benchmark mode in the client: request/response latency distributions with bounded histograms, recorded environment/config/seed, and repeatable output.
3. Rate/latency verification runs comparing configured fault parameters against measured distributions.
4. Only after methodology and reproducibility are documented may performance numbers be published.
