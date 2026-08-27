# Milestone 4 Report — Metrics Export and Benchmark Client

Date: August 27, 2026

## Outcome

Milestone 4 gave the workbench machine-readable observability and a reproducible measurement mode:

- `--metrics-file PATH`: the proxy exports a JSON metrics document on `SIGUSR1` and at shutdown. The write is atomic — a sibling temporary file renamed over the target — so readers never observe a partial document. Content is copied from event-loop-owned values only: aggregate counters, a closes breakdown by final state (`fully_closed`/`reset`/`failed`/`timed_out`), and one structured object per live connection (bytes, operations, backpressure counters, fault counters). Tooling no longer needs to scrape log events.
- `netfault-client --mode request-response`: a benchmark mode measuring request/response round-trip latency. Each connection performs `--warmup` unrecorded exchanges then `--requests` recorded sequential exchanges of `--request-bytes` against an echoing peer, with `TCP_NODELAY`, verifying every response byte-exact against the seeded request. The summary pools samples across connections and reports nearest-rank percentiles (`min`/`mean`/`p50`/`p90`/`p99`/`max`) together with the full configuration, seed, and `uname` environment, so a run is reproducible and self-describing.
- GitHub Actions CI now runs the ASan/UBSan and TSan suites on every push and pull request, mirroring the container test path.

This milestone does **not** implement a queryable metrics endpoint, Prometheus text, latency histo­gram buckets (recorded samples are used, as the design permits for benchmark mode), YAML configuration, or the dashboard.

## Benchmark methodology

Documented before any number is published, per the design rule:

- **What is measured:** application-observed round-trip time of one `send()` of N bytes followed by reads until exactly N bytes return, over loopback TCP through the proxy, sequential per connection. This includes client scheduling, both proxy directions, and the echo peer — it is an end-to-end application latency, not a proxy-forwarding latency.
- **Sampling:** warmup exchanges are excluded; every recorded exchange contributes one sample; samples are pooled across connections; percentiles are nearest-rank over the sorted pool (no interpolation).
- **Reproducibility:** the summary embeds mode, connection count, request count, warmup count, request size, master seed, and `uname` (sysname/release/machine). Payloads derive deterministically from the seed. Timings themselves remain machine- and load-dependent; identical configuration is the comparable unit, not identical timings.
- **Bounds:** requests ≤ 100,000 per connection, total recorded samples ≤ 1,000,000, request size ≤ 1 MiB. Recorded samples are exact; no bucketing error.
- **No absolute claims:** this repository publishes no absolute throughput or latency numbers. The verified claim is relational: injected fault latency shifts the measured distribution by the configured amount (see below).

## Verification

All runs used the Ubuntu 24.04 container (GCC 13.3.0, full warning set with `-Werror`), plus GitHub Actions on ubuntu-24.04.

| Property | Result | Evidence |
|---|---|---|
| Live export mid-transfer | Passed | `SIGUSR1` during a slow-reader transfer produces a parseable document with one active connection whose byte counters are bounded by the payload |
| Export atomicity | Passed by construction | Temp-file-plus-rename; the integration reader polls the target and never observes partial JSON |
| Shutdown export reconciles | Passed | Final document shows zero active connections, an empty connection list, and close counts summing to total accepted |
| Closes breakdown by state | Passed | `fully_closed`/`reset`/`failed`/`timed_out` counters present and consistent |
| Benchmark summary self-describing | Passed | Configuration, seed, and environment fields asserted present |
| Percentiles monotone and complete | Passed | 2 connections × 20 recorded requests yield exactly 40 pooled samples with min ≤ p50 ≤ p90 ≤ p99 ≤ max |
| Response integrity under benchmark | Passed | Every response compared byte-exact against the seeded request |
| Fault latency visible in the distribution | Passed | 50 ms/direction injected latency shifts measured p50 by ≥ 90 ms over baseline through the same proxy path |
| Memory errors/leaks in executed paths | No findings | Three consecutive full ASan+UBSan suites (10 tests) passed |
| Thread data races in executed paths | No findings | TSan suite passed |

### Flakes found and fixed during this milestone

Bringing the suite to GitHub-hosted runners surfaced two environment dependencies the local container had masked:

1. **Timing margins.** The Milestone 2 snapshot test's 512 KiB transfer through the 3 ms/4 KiB slow reader has a ~384 ms duration floor, shorter than its 500 ms `SIGUSR1` sample point — it had passed locally only because sanitizer-container overhead stretched the transfer. The payload is now 2 MiB (~1.5 s floor on any machine), and the same analysis was applied to the new metrics test.
2. **Kernel TCP overflow behavior.** The connect-timeout scenario hangs the proxy's upstream connect by saturating a never-accepting listener's queue, relying on the kernel dropping SYNs to a full accept queue. Runners exhibited two other legitimate behaviors: with `tcp_syncookies=1` the handshake can complete against the full queue while the server silently drops the child (the proxy sees a connected upstream), and the queue can answer with RST so the connect fails instantly (never accepted at all). The scenario now watches the workload connection's fate in the log — asserting the strict `timed_out` path when the connect hangs and reporting itself environment-skipped for the other two outcomes — and CI pins `tcp_syncookies=0` and `tcp_abort_on_overflow=0` so the strict path runs there.

## Known limitations

- The metrics document is a file written on demand; there is no HTTP endpoint, no periodic export, and no Prometheus text format.
- Process-level metrics (CPU time, RSS, FD count) are not yet included in the export.
- The benchmark measures sequential request/response only — no pipelining, no concurrency within a connection, no open-loop arrival process.
- Latency samples are pooled across connections; per-connection distributions are not reported.
- The benchmark client and proxy share a host in all current runs; cross-host measurement is out of scope while the loopback-only safety posture stands.
- Still numeric IPv4 only, level-triggered epoll only.

## Recommended next milestone

Proceed to **Milestone 5: reliability, stress, CI depth, and documentation polish** per the design document:

1. Long-duration soak (minutes, hundreds of connection cycles) with FD/RSS plateau assertions in Release as well as sanitizer builds.
2. Randomized deterministic payload-size sweeps around socket and queue boundary sizes, with failing seeds printed and retained.
3. Packet-capture comparison of one representative fault scenario against the logged event sequence.
4. A recorded demo and README media for portfolio presentation.
5. Only then consider Milestone 6 experiments (dashboard/API, thread-model comparison).
