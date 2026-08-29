#!/usr/bin/env python3
"""Milestone 5 addendum — the event loop must idle, not spin.

A proxy waiting on a slow upstream should consume almost no CPU: the work is
bounded by what the upstream accepts, not by how fast the loop can iterate.
Two independent signals are asserted, because the busy loop this guards
against has appeared in two forms:

  * a queue-backed spin, which burns one `EAGAIN` per futile iteration, and
  * an empty-queue spin, which burns CPU without moving the `EAGAIN` counter
    at all because a flush with nothing queued returns immediately.

The cause in both cases was requesting EPOLLRDHUP while reads were paused:
the flag is level-triggered and stays asserted once a peer half-closes, so
the loop woke continuously for a condition it had deliberately declined to
act on. Both variants are covered here.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import (  # noqa: E402
    default_environment,
    events_named,
    free_port,
    parse_detail,
    read_events,
    stop_process,
    wait_for_port,
)

# Thresholds chosen from measurements of both the fixed and the pre-fix build.
# In a plain build the separation is stark — 0.0 versus 0.37 CPU fraction, and
# 0.7 versus 919 EAGAIN per I/O operation — so both signals carry wide margins.
#
# This runs in plain builds only. Sanitizers distort precisely the quantity
# being measured: they slow each loop iteration enough that a spinning build
# reaches only 1.3 EAGAIN per operation, while the honest build's own CPU
# fraction rises to ~0.31, leaving the two barely distinguishable. A
# performance guard belongs in the build that represents performance.
MAX_CPU_FRACTION = 0.35
MAX_EAGAIN_PER_OPERATION = 60

# A third failure shape: not spinning, but dribbling. A rate limiter that
# releases the instant one byte's worth of tokens exists moved 200 kB in
# 144,936 operations of roughly 1.4 bytes each; releasing in quanta does it in
# 149, about 1,342 bytes each. Healthy runs sit far above this floor.
MIN_BYTES_PER_OPERATION = 100

CLOCK_TICKS_PER_SECOND = os.sysconf("SC_CLK_TCK")


def proxy_cpu_seconds(pid: int) -> float:
    with open(f"/proc/{pid}/stat", encoding="ascii") as handle:
        fields = handle.read().rsplit(") ", 1)[1].split()
    utime, stime = int(fields[11]), int(fields[12])
    return (utime + stime) / CLOCK_TICKS_PER_SECOND


def run_case(args, label: str, proxy_extra: list[str], payload_bytes: int,
             server_extra: list[str] | None = None) -> dict:
    environment = default_environment()
    if server_extra is None:
        server_extra = ["--mode", "slow-reader", "--delay-ms", "5", "--chunk-bytes", "4096"]
    with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
        server_port = free_port()
        proxy_port = free_port()
        server = subprocess.Popen(
            [args.server, "--listen", f"127.0.0.1:{server_port}"] + server_extra,
            stdout=server_log, stderr=server_log, text=True, env=environment,
        )
        proxy = None
        try:
            wait_for_port(server_port, server)
            proxy = subprocess.Popen(
                [
                    args.proxy, "--listen", f"127.0.0.1:{proxy_port}",
                    "--upstream", f"127.0.0.1:{server_port}",
                ] + proxy_extra,
                stdout=proxy_log, stderr=proxy_log, text=True, env=environment,
            )
            wait_for_port(proxy_port, proxy)

            cpu_before = proxy_cpu_seconds(proxy.pid)
            started = time.monotonic()
            client = subprocess.run(
                [
                    args.client, "--connect", f"127.0.0.1:{proxy_port}",
                    "--connections", "1", "--payload-bytes", str(payload_bytes), "--seed", "101",
                ],
                capture_output=True, text=True, timeout=120, env=environment, check=False,
            )
            wall = time.monotonic() - started
            cpu = proxy_cpu_seconds(proxy.pid) - cpu_before
            if client.returncode != 0 or json.loads(client.stdout)["successful"] != 1:
                raise RuntimeError(f"{label}: transfer failed: {client.stdout!r} {client.stderr!r}")
        finally:
            if proxy is not None:
                stop_process(proxy)
            stop_process(server)

        closed = [
            event for event in events_named(read_events(proxy_log), "connection_closed")
            if parse_detail(str(event.get("detail", ""))).get("client_read", 0) == payload_bytes
        ]
        if len(closed) != 1:
            raise AssertionError(f"{label}: expected one workload close, got {len(closed)}")
        metrics = parse_detail(str(closed[0]["detail"]))

    operations = metrics["read_operations"] + metrics["write_operations"]
    result = {
        "case": label,
        "wall_s": round(wall, 3),
        "proxy_cpu_s": round(cpu, 3),
        "cpu_fraction": round(cpu / wall, 4) if wall > 0 else 0,
        "eagain_events": metrics["eagain_events"],
        "io_operations": operations,
        "eagain_per_operation": round(metrics["eagain_events"] / max(operations, 1), 1),
        "bytes_per_operation": round(2 * payload_bytes / max(operations, 1), 1),
    }

    if metrics["client_read"] != payload_bytes or metrics["client_written"] != payload_bytes:
        raise AssertionError(f"{label}: byte accounting mismatch: {metrics}")
    if result["cpu_fraction"] > MAX_CPU_FRACTION:
        raise AssertionError(
            f"{label}: proxy burned {result['cpu_fraction']:.0%} of wall time on CPU while waiting on a "
            f"slow upstream (limit {MAX_CPU_FRACTION:.0%}) — the event loop is spinning: {result}"
        )
    if result["eagain_per_operation"] > MAX_EAGAIN_PER_OPERATION:
        raise AssertionError(
            f"{label}: {result['eagain_per_operation']} EAGAIN results per I/O operation "
            f"(limit {MAX_EAGAIN_PER_OPERATION}) — the event loop is spinning: {result}"
        )
    if result["bytes_per_operation"] < MIN_BYTES_PER_OPERATION:
        raise AssertionError(
            f"{label}: only {result['bytes_per_operation']} bytes per I/O operation "
            f"(floor {MIN_BYTES_PER_OPERATION}) — the proxy is dribbling: {result}"
        )
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--client", required=True)
    args = parser.parse_args()

    sanitizer = next((name for name in ("ASAN_OPTIONS", "TSAN_OPTIONS") if name in os.environ), None)
    if sanitizer:
        print(json.dumps({
            "milestone": 5,
            "scenario": "event_loop_idles",
            "status": "skipped_sanitizer_build",
            "reason": f"{sanitizer} is set; sanitizers distort loop-iteration cost, "
                      "so this guard runs in plain builds only",
        }))
        return 0

    watermarks = ["--buffer-bytes", "16384", "--low-water-bytes", "4096", "--high-water-bytes", "16384"]
    results = [
        # Queue-backed: a constrained upstream keeps bytes queued for the whole
        # transfer, so a spin shows up in the EAGAIN counter.
        run_case(args, "constrained upstream buffer",
                 watermarks + ["--socket-buffer-bytes", "4096"], 512 * 1024),
        # Empty-queue: kernel buffers absorb the payload, so a spin burns CPU
        # without touching the EAGAIN counter.
        run_case(args, "default upstream buffer", watermarks, 512 * 1024),
        # Rate-limited: guards against dribbling — releasing a byte at a time
        # rather than in quanta — which neither signal above detects.
        run_case(args, "rate-limited direction",
                 ["--fault-rate-bytes-per-sec", "100000", "--fault-burst-bytes", "10000",
                  "--fault-direction", "c2u"],
                 100_000, server_extra=["--mode", "echo"]),
    ]
    print(json.dumps({"milestone": 5, "scenario": "event_loop_idles", "cases": results, "status": "passed"}))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"milestone5 spin failure: {error}", file=sys.stderr)
        raise
