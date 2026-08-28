#!/usr/bin/env python3
"""Milestone 5 soak: repeated mixed-size transfer rounds, interleaved abrupt
client aborts, against one long-lived proxy. File descriptors must return to
baseline after every round and resident memory must plateau, not climb.

Duration is controlled by NETFAULT_SOAK_SECONDS (default 45; raise it for
long manual soaks). The seed is printed so a failing sequence can be replayed.
"""

import argparse
import json
import os
import random
import socket
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import (  # noqa: E402
    default_environment,
    free_port,
    read_events,
    rss_kib,
    stop_process,
    wait_for_fd_count_at_most,
    wait_for_port,
    wait_for_stable_fd_count,
)

# RSS discipline depends on the allocator underneath:
# - plain builds must plateau tightly;
# - ASan quarantines freed memory, so the soak bounds the quarantine and
#   allows headroom;
# - TSan retains history/shadow state that makes RSS non-indicative, so only
#   the FD plateau is asserted there.
RSS_LIMIT_PLAIN_KIB = 24 * 1024
RSS_LIMIT_ASAN_KIB = 64 * 1024


def abrupt_client(port: int, payload: bytes) -> None:
    """Connect, push bytes, then abort with a RST (SO_LINGER 0) mid-stream."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.settimeout(5)
        sock.connect(("127.0.0.1", port))
        sock.sendall(payload)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b"\x01\x00\x00\x00\x00\x00\x00\x00")
    finally:
        sock.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--client", required=True)
    args = parser.parse_args()

    seed = int(os.environ.get("NETFAULT_SOAK_SEED", "573"))
    soak_seconds = float(os.environ.get("NETFAULT_SOAK_SECONDS", "45"))
    rng = random.Random(seed)
    under_asan = "ASAN_OPTIONS" in os.environ
    under_tsan = "TSAN_OPTIONS" in os.environ
    environment = default_environment()
    if under_asan:
        environment["ASAN_OPTIONS"] += ":quarantine_size_mb=16"
    rss_limit_kib = None if under_tsan else (RSS_LIMIT_ASAN_KIB if under_asan else RSS_LIMIT_PLAIN_KIB)

    with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
        server_port = free_port()
        proxy_port = free_port()
        server = subprocess.Popen(
            [args.server, "--listen", f"127.0.0.1:{server_port}", "--mode", "echo"],
            stdout=server_log,
            stderr=server_log,
            text=True,
            env=environment,
        )
        proxy = None
        rounds = 0
        connections_total = 0
        aborts_total = 0
        rss_samples_kib: list[int] = []
        try:
            wait_for_port(server_port, server)
            proxy = subprocess.Popen(
                [
                    args.proxy,
                    "--listen",
                    f"127.0.0.1:{proxy_port}",
                    "--upstream",
                    f"127.0.0.1:{server_port}",
                    "--max-connections",
                    "64",
                    "--idle-timeout-ms",
                    "5000",
                ],
                stdout=proxy_log,
                stderr=proxy_log,
                text=True,
                env=environment,
            )
            wait_for_port(proxy_port, proxy)
            baseline_fds = wait_for_stable_fd_count(proxy)

            deadline = time.monotonic() + soak_seconds
            while time.monotonic() < deadline:
                connections = rng.randint(2, 12)
                payload_bytes = rng.randint(1, 256 * 1024)
                client = subprocess.run(
                    [
                        args.client,
                        "--connect",
                        f"127.0.0.1:{proxy_port}",
                        "--connections",
                        str(connections),
                        "--payload-bytes",
                        str(payload_bytes),
                        "--seed",
                        str(seed + rounds),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=60,
                    env=environment,
                    check=False,
                )
                summary = json.loads(client.stdout) if client.returncode == 0 else None
                if summary is None or summary["successful"] != connections:
                    recent_events = json.dumps(read_events(proxy_log)[-12:])
                    raise AssertionError(
                        f"soak round failed: soak_seed={seed} round={rounds} connections={connections} "
                        f"payload_bytes={payload_bytes} stdout={client.stdout!r} stderr={client.stderr!r} "
                        f"recent_proxy_events={recent_events}"
                    )
                connections_total += connections

                # Interleave abrupt aborts so reset cleanup paths churn too.
                for _ in range(rng.randint(0, 3)):
                    abrupt_client(proxy_port, b"\x77" * rng.randint(1, 32 * 1024))
                    aborts_total += 1

                wait_for_fd_count_at_most(proxy, baseline_fds, timeout=10.0)
                rss_samples_kib.append(rss_kib(proxy))
                rounds += 1

            if rounds < 3:
                raise AssertionError(f"soak completed too few rounds to be meaningful: {rounds}")
            # Compare the plateau (post-warmup) rather than the first sample.
            plateau_start = rss_samples_kib[min(2, len(rss_samples_kib) - 1)]
            rss_growth_kib = rss_samples_kib[-1] - plateau_start
            if rss_limit_kib is not None and rss_growth_kib > rss_limit_kib:
                raise AssertionError(
                    f"RSS climbed {rss_growth_kib} KiB over the soak (limit {rss_limit_kib}): "
                    f"soak_seed={seed} samples={rss_samples_kib}"
                )
        finally:
            if proxy is not None:
                stop_process(proxy)
            stop_process(server)

    print(
        json.dumps(
            {
                "milestone": 5,
                "scenario": "soak",
                "soak_seed": seed,
                "soak_seconds": soak_seconds,
                "rounds": rounds,
                "connections_total": connections_total,
                "aborts_total": aborts_total,
                "rss_first_kib": rss_samples_kib[0],
                "rss_last_kib": rss_samples_kib[-1],
                "rss_limit_kib": rss_limit_kib,
                "rss_asserted": rss_limit_kib is not None,
                "status": "passed",
            }
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"milestone5 soak failure: {error}", file=sys.stderr)
        raise
