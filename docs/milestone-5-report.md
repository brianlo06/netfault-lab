# Milestone 5 Report — Reliability, Stress, and Presentation

Date: August 28, 2026

## Outcome

Milestone 5 hardened the evidence rather than the features:

- **Payload sweep** (`milestone5-sweep`): byte-exact forwarding verified across off-by-one payload sizes around every boundary in the system — client/server chunk (16 KiB ± 1), watermark defaults (32/64 KiB ± 1), page-ish sizes (4 KiB ± 1), 128 KiB ± 1 — plus twelve seeded random sizes up to 512 KiB. The sweep seed is printed on success and embedded in any failure message, so a failing combination replays exactly; `NETFAULT_SWEEP_SEED` overrides it.
- **Soak** (`milestone5-soak`): duration-based rounds (default 45 s, `NETFAULT_SOAK_SECONDS` for long manual runs) of randomly sized transfers against one long-lived proxy, interleaved with `SO_LINGER`-zero abrupt client aborts so reset cleanup churns alongside orderly closes. File descriptors must return to baseline after every round.
- **Packet-capture comparison** (`milestone5-pcap`): a reset-injection run captured on loopback with tcpdump and decoded by a minimal hand-rolled classic-pcap parser (Ethernet/IPv4/TCP, ~50 lines, no dependencies). The wire must corroborate the log: handshakes on both hops, forwarded payload within one socket buffer of the log's written totals, and a real RST after the logged `fault_injected`. The scenario self-skips without capture privilege.
- **Release lane**: a `release` test preset, a third CI job building and testing `-O3` with the full warning set, and tcpdump in the container image.
- **Demo**: [docs/demo.md](demo.md), a captured walkthrough in which the benchmark client measures the proxy's own fault injection — baseline 51 µs median round trip becoming 84.9 ms through a 40 ms ± 10 ms per-direction fault, with the connection's logged delay budget reconciling against its lifetime.

## Allocator-aware RSS discipline

The soak's memory assertion depends on what allocator sits under the proxy, and the first ASan run demonstrated why: RSS climbed ~1.5 MiB per round, linearly, for 96 rounds. That is not a leak — LSan exits clean — it is ASan's quarantine deliberately holding freed memory (each round retires ~12 connections × 128 KiB of queue buffers, matching the slope) up to its default 256 MiB cap. The soak now:

- asserts a strict 24 MiB post-warmup plateau in plain (Release/Debug) builds;
- bounds the quarantine (`quarantine_size_mb=16`) and allows 64 MiB under ASan, keeping a real assertion in the sanitizer lane;
- reports without asserting under TSan, whose history and shadow state make RSS non-indicative.

The Release-build soak recorded **1,672 rounds, 11,925 connections, and 2,502 abrupt aborts in 45 seconds with 128 KiB of RSS drift** (4,744 → 4,872 KiB) and file descriptors at baseline after every round.

## Wire-versus-log reconciliation

Two subtleties surfaced while making the pcap comparison honest, both now encoded in the test:

1. The reset budget counts proxy-written bytes in **both** directions, so payload toward the upstream alone legitimately undershoots it — the comparison sums the upstream hop and the proxy-to-client hop.
2. The wire can show slightly less than the log even then: the proxy counts bytes handed to the kernel, and the injected RST discards whatever still sits unsent in a socket buffer. The assertion allows one client-chunk of slack per direction and otherwise requires wire ≥ logged totals.

## Verification

| Property | Result | Evidence |
|---|---|---|
| Byte-exact forwarding at every boundary size | Passed | 17 boundary sizes + 12 seeded random sizes, 2 connections each, echo-verified |
| Failing seeds replayable | Passed by construction | Sweep/soak seeds printed in success output and embedded in failure messages |
| FD plateau under churn with aborts | Passed | Baseline re-reached after each of 1,672 Release rounds (and every sanitizer round) |
| RSS plateau in Release | Passed | 128 KiB drift over 11,925 connections against a 24 MiB limit |
| ASan RSS bounded with quarantine capped | Passed | 64 MiB limit with `quarantine_size_mb=16` |
| Wire corroborates the fault log | Passed | Handshakes on both hops, forwarded payload within slack of logged totals, RST present after `fault_injected` |
| pcap parser correctness | Passed indirectly | Assertions above are computed solely from the hand-parsed capture |
| Full suite on all three lanes | Passed | 13/13 under Release, ASan/UBSan, and TSan in the container; CI runs all three |

### Issues found and fixed during this milestone

- **A real timer race, caught by CI.** The event loop re-armed the timerfd at the end of every epoll iteration, even when the earliest deadline was unchanged — and `timerfd_settime` clears any pending expiration by design. If the timer expired in the window between `epoll_wait` returning (woken by an unrelated socket event) and the end-of-iteration rearm, the pending readable state could be consumed and the expiration lost. On Azure's 6.17 kernel this reproducibly left connect-timeout connections hanging forever while the idle scenario (whose traffic kept generating events) worked. The fix skips `timerfd_settime` entirely when the armed deadline is unchanged, and the arm cache is invalidated when a firing is consumed. The proxy now also logs `timer_armed` (with delta) and `timer_fired` (entries processed) so any recurrence carries evidence. Two-of-three CI jobs failed before the fix; three consecutive fully-green three-lane runs followed it.
- The ASan RSS climb described above — initially read as a failure, actually the quarantine working as designed; the fix was measuring what the allocator can promise rather than relaxing blindly.
- The pcap byte-accounting subtleties above.
- The controlled server held every finished connection thread joinable (stacks unreleased) until shutdown; the accept loop now reaps completed workers.
- GCC 13 at `-O3` raises a false-positive `-Wstringop-overflow` inside libstdc++ heap code inlined from `priority_queue::pop`; suppressed with a narrowly scoped, documented pragma rather than weakening the global warning set.
- One ASan soak round observed a single `payload_mismatch` on CI before the timer fix; it has not recurred across subsequent runs, and the soak now dumps the proxy's recent events in its failure message so any recurrence is diagnosable.

## Known limitations

- The soak's default 45 s is CI-scale; multi-minute or overnight soaks are manual (`NETFAULT_SOAK_SECONDS`).
- The pcap parser handles classic pcap with Ethernet-framed IPv4 TCP only — exactly what tcpdump produces on Linux loopback — and the scenario skips elsewhere.
- The pcap comparison covers the reset fault; latency and rate faults are corroborated by measurement (benchmark) rather than capture.
- Demo media is a captured terminal walkthrough; a screen recording for LinkedIn Featured remains to be produced by hand.
- Still numeric IPv4 only, level-triggered epoll only.

## Addendum — a busy loop found after the milestone closed

Building the [project site](https://netfault.jarvisworlds.com), which replays captured runs, surfaced a
defect none of the tests could see: a single 1 MiB transfer through a slow upstream logged **721,806
`EAGAIN` results**, and the proxy burned most of a CPU while waiting on a peer that was, by construction,
slow.

**Root cause.** `EPOLLRDHUP` was requested unconditionally. It is level-triggered and stays asserted from
the moment a peer half-closes. The deterministic client sends its payload and then half-closes, so from
that point the flag was permanently ready. When backpressure had paused reads on that socket, the loop woke
on `EPOLLRDHUP`, correctly declined to read (the queue was above the low-water mark), performed a futile
flush, and immediately woke again — a hot loop on a condition the proxy had deliberately decided not to act
on yet.

An earlier reading of the evidence blamed spurious writability — the kernel reporting a socket writable
while `send()` still returns `EAGAIN`. That theory fitted the `EAGAIN` counter but not the measurements: it
failed to explain why an *unconstrained* upstream, which produced only 131 `EAGAIN` results, still burned
196 CPU ticks. The `EPOLLRDHUP` explanation covers both, because a spin with empty queues never reaches a
`send()` at all and so never moves the `EAGAIN` counter.

**Fix.** Request `EPOLLRDHUP` only alongside read interest. This costs nothing: level-triggered delivery
re-reports the half-close the moment reads resume, and a resumed read observes the EOF directly. Redundant
`EPOLL_CTL_MOD` calls were removed at the same time — the loop had been rewriting both registrations every
iteration, about two `epoll_ctl` calls per wakeup.

| Workload (1 MiB through a slow reader) | Before | After |
|---|---|---|
| `EAGAIN`, constrained upstream buffer | 721,806 | 485 |
| CPU ticks, constrained upstream buffer | 90 | 3 |
| `EAGAIN`, default upstream buffer | 131 | 107 |
| CPU ticks, default upstream buffer | 196 | 10 |

## Addendum — a rate limiter that dribbled

Re-measuring the captured scenarios after that fix exposed a second, unrelated inefficiency in the same
family. Moving 200 kB through a 100 kB/s token bucket took **144,936 read and write operations** — about
1.4 bytes per syscall.

**Root cause.** The bucket released as soon as a single byte's worth of tokens had accrued, so the loop woke
constantly to forward a byte or two. Nothing was incorrect: the rate was enforced exactly and every byte
arrived in order. It was simply the least efficient possible way to honour the limit.

**Fix.** A direction now waits until it can release a worthwhile quantum — 4 KiB, bounded by the bytes
actually queued and by the configured burst, so a small tail or a burst smaller than the quantum still
drains rather than stalling. The wake-up calculation targets that same quantum, so the timer sleeps until a
useful chunk is affordable instead of until the next single byte.

| Workload (200 kB through a 100 kB/s bucket) | Before | After |
|---|---|---|
| Read + write operations | 144,936 | 149 |
| Bytes per operation | 1.4 | 1,342 |
| `EAGAIN` | 48,302 | 47 |
| Proxy CPU as a fraction of wall time | 39% | 0.5% |
| Wall time | 1,929 ms | 1,922 ms |

Wall time is unchanged, which is the point: the rate limit is still enforced to the same accuracy, using
three orders of magnitude fewer syscalls.

**Regression guard.** `milestone5-spin` asserts three independent signals across three workloads, because
these defects have three distinct shapes: a queue-backed spin burns one `EAGAIN` per futile iteration; an
empty-queue spin burns CPU without moving the `EAGAIN` counter at all, since a flush with nothing queued
never reaches a `send()`; and a dribbling rate limiter moves bytes efficiently by neither measure while
issuing a syscall per byte. The guard runs in plain builds only — sanitizers slow each iteration enough
that a spinning build reaches just 1.3 `EAGAIN` per operation while the honest build's own CPU fraction
rises to 0.31, leaving the two barely distinguishable. A performance guard belongs in the build that
represents performance. It was verified against the pre-fix build, where it fails at 919 `EAGAIN` per I/O
operation against a limit of 60.

## Remaining roadmap

Milestone 6 is optional per the design document: a dashboard/API consuming the metrics DTO, a thread-per-connection comparison, or flamegraph work. The core workbench — forwarding, backpressure, deterministic faults, timeouts, metrics, benchmark, stress evidence, CI — is complete.
