# Five-Minute Demo

A captured walkthrough of the workbench measuring its own fault injection. All output below is real, recorded in the Ubuntu 24.04 container (ASan/UBSan build) on 2026-08-28; timings are machine-dependent and illustrate relationships, not performance claims.

## Setup: three processes

```bash
netfault-server --listen 127.0.0.1:9000 --mode echo

netfault-proxy --listen 127.0.0.1:8080 --upstream 127.0.0.1:9000 \
  --fault-latency-ms 40 --fault-jitter-ms 10 --fault-seed 42 \
  --metrics-file /tmp/metrics.json
```

The proxy will delay every forwarded chunk by 40 ms ± 10 ms of seeded uniform jitter, in both directions.

## 1. Baseline: benchmark the echo server directly

```bash
netfault-client --connect 127.0.0.1:9000 --mode request-response \
  --connections 2 --requests 20 --request-bytes 1024 --seed 7
```

```json
{"mode":"request-response","connections":2,"successful":2,"seed":7,
 "requests_per_connection":20,"warmup_per_connection":5,"request_bytes":1024,
 "environment":{"sysname":"Linux","release":"6.12.76-linuxkit","machine":"aarch64"},
 "max_duration_us":5086,
 "latency_us":{"count":40,"min":31,"mean":58,"p50":51,"p90":97,"p99":114,"max":114}}
```

Loopback echo round trips sit around 51 µs at the median.

## 2. Through the fault proxy

Same command, pointed at the proxy:

```json
{"mode":"request-response","connections":2,"successful":2,"seed":7,
 "requests_per_connection":20,"warmup_per_connection":5,"request_bytes":1024,
 "environment":{"sysname":"Linux","release":"6.12.76-linuxkit","machine":"aarch64"},
 "max_duration_us":2212394,
 "latency_us":{"count":40,"min":62091,"mean":85515,"p50":84941,"p90":97582,"p99":105115,"max":105115}}
```

The median jumps from 51 µs to 84.9 ms — two traversals of a 40 ms ± 10 ms delay, exactly as configured. The spread (min 62.1 ms, max 105.1 ms) is the jitter distribution: each direction samples uniformly from [30 ms, 50 ms], so a round trip spans [60 ms, 100 ms] plus the loopback baseline.

## 3. The proxy's own accounting

The close event for one benchmark connection (from the proxy's JSON Lines log):

```json
{"event":"connection_closed","connection_id":2,"state":"fully_closed",
 "detail":"orderly_shutdown,duration_ms=2208,client_read=25600,upstream_read=25600,
 client_written=25600,upstream_written=25600,read_operations=50,write_operations=50,
 partial_writes=0,eagain_events=50,rejected_bytes=0,faults_applied=1,
 c2u_delayed_segments=25,c2u_delay_budget_us=1013425,
 u2c_delayed_segments=25,u2c_delay_budget_us=1047240,..."}
```

Cross-checking the story: 25 requests (5 warmup + 20 recorded) × 1024 bytes = 25,600 bytes each way, 25 delayed segments per direction, and a delay budget of ~1.01 s + ~1.05 s ≈ 2.06 s of injected latency — matching the connection's 2,208 ms lifetime on a link whose raw round trip is microseconds. The budgets differ between directions because each direction draws its own seeded jitter stream.

## 4. Machine-readable metrics on demand

```bash
kill -USR1 <proxy pid>
```

```json
{
  "timestamp_ms": 1787895722370,
  "listen": "127.0.0.1:8080",
  "upstream": "127.0.0.1:9000",
  "active_connections": 0,
  "total_accepted": 2,
  "pending_timers": 0,
  "dropped_log_events": 0,
  "closes": { "fully_closed": 2, "reset": 0, "failed": 0, "timed_out": 0 },
  "connections": []
}
```

The document is written atomically (temp file + rename), so a reader polling the path never sees partial JSON. During a live transfer the `connections` array carries per-connection byte, backpressure, and fault counters.

## Where to go next

- Reproduce everything: `docker build -f containers/Dockerfile -t netfault-lab . && docker run --rm netfault-lab`
- Break things on purpose: `--fault-reset-after-bytes`, `--fault-half-close-after-bytes`, `--fault-rate-bytes-per-sec`, `--idle-timeout-ms`
- Watch the wire agree with the log: `tests/integration/milestone5_pcap.py` captures a fault run and corroborates handshakes, forwarded bytes, and the injected RST against the event log.
