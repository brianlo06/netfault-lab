#!/usr/bin/env python3
"""Milestone 2 resource plateau: repeated connection creation/destruction must
return file descriptors to baseline and keep resident memory flat."""

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
    rss_kib,
    stop_process,
    wait_for_fd_count_at_most,
    wait_for_port,
    wait_for_stable_fd_count,
)

ROUNDS = 5
CONNECTIONS_PER_ROUND = 16
PAYLOAD_BYTES = 65_536
RSS_GROWTH_LIMIT_KIB = 16 * 1024  # generous because sanitizer allocators are noisy


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--client", required=True)
    args = parser.parse_args()

    server_port = free_port()
    proxy_port = free_port()
    environment = default_environment()

    with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
        server = subprocess.Popen(
            [args.server, "--listen", f"127.0.0.1:{server_port}", "--mode", "echo"],
            stdout=server_log,
            stderr=server_log,
            text=True,
            env=environment,
        )
        proxy = None
        rss_samples_kib = []
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
                    str(CONNECTIONS_PER_ROUND * 2),
                ],
                stdout=proxy_log,
                stderr=proxy_log,
                text=True,
                env=environment,
            )
            wait_for_port(proxy_port, proxy)
            baseline_fds = wait_for_stable_fd_count(proxy)

            for round_index in range(ROUNDS):
                client = subprocess.run(
                    [
                        args.client,
                        "--connect",
                        f"127.0.0.1:{proxy_port}",
                        "--connections",
                        str(CONNECTIONS_PER_ROUND),
                        "--payload-bytes",
                        str(PAYLOAD_BYTES),
                        "--seed",
                        str(1_000 + round_index),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=60,
                    env=environment,
                    check=False,
                )
                if client.returncode != 0:
                    raise RuntimeError(
                        f"round {round_index} client failed: "
                        f"stdout={client.stdout!r} stderr={client.stderr!r}"
                    )
                summary = json.loads(client.stdout)
                if summary["successful"] != CONNECTIONS_PER_ROUND:
                    raise AssertionError(f"round {round_index} had failures: {summary}")
                wait_for_fd_count_at_most(proxy, baseline_fds)
                time.sleep(0.05)
                rss_samples_kib.append(rss_kib(proxy))
        finally:
            if proxy is not None:
                stop_process(proxy)
            stop_process(server)

        events = read_events(proxy_log)
        closed = events_named(events, "connection_closed")
        expected_closes = ROUNDS * CONNECTIONS_PER_ROUND
        # Filter to workload connections; wait_for_port probes close with zero bytes.
        orderly = [
            event
            for event in closed
            if event["state"] == "fully_closed"
            and parse_detail(str(event.get("detail", ""))).get("client_read", 0) == PAYLOAD_BYTES
        ]
        if len(orderly) != expected_closes:
            raise AssertionError(
                f"expected {expected_closes} orderly workload closes, got {len(orderly)} of {len(closed)}"
            )

        rss_growth_kib = rss_samples_kib[-1] - rss_samples_kib[0]
        if rss_growth_kib > RSS_GROWTH_LIMIT_KIB:
            raise AssertionError(
                f"RSS grew {rss_growth_kib} KiB across rounds (limit {RSS_GROWTH_LIMIT_KIB}): "
                f"{rss_samples_kib}"
            )

    print(
        json.dumps(
            {
                "milestone": 2,
                "scenario": "connection_churn",
                "rounds": ROUNDS,
                "connections_per_round": CONNECTIONS_PER_ROUND,
                "rss_samples_kib": rss_samples_kib,
                "rss_growth_kib": rss_growth_kib,
                "status": "passed",
            }
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"milestone2 churn failure: {error}", file=sys.stderr)
        raise
