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

- The ASan RSS climb described above — initially read as a failure, actually the quarantine working as designed; the fix was measuring what the allocator can promise rather than relaxing blindly.
- The pcap byte-accounting subtleties above.
- GCC 13 at `-O3` raises a false-positive `-Wstringop-overflow` inside libstdc++ heap code inlined from `priority_queue::pop`; suppressed with a narrowly scoped, documented pragma rather than weakening the global warning set.

## Known limitations

- The soak's default 45 s is CI-scale; multi-minute or overnight soaks are manual (`NETFAULT_SOAK_SECONDS`).
- The pcap parser handles classic pcap with Ethernet-framed IPv4 TCP only — exactly what tcpdump produces on Linux loopback — and the scenario skips elsewhere.
- The pcap comparison covers the reset fault; latency and rate faults are corroborated by measurement (benchmark) rather than capture.
- Demo media is a captured terminal walkthrough; a screen recording for LinkedIn Featured remains to be produced by hand.
- Still numeric IPv4 only, level-triggered epoll only.

## Remaining roadmap

Milestone 6 is optional per the design document: a dashboard/API consuming the metrics DTO, a thread-per-connection comparison, or flamegraph work. The core workbench — forwarding, backpressure, deterministic faults, timeouts, metrics, benchmark, stress evidence, CI — is complete.
