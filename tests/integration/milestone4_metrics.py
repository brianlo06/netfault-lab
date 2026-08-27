#!/usr/bin/env python3
"""Milestone 4 metrics export: SIGUSR1 and shutdown must atomically write a
parseable JSON document copied from event-loop-owned values."""

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import (  # noqa: E402
    default_environment,
    free_port,
    stop_process,
    wait_for_port,
)


def read_metrics(path: str, deadline_s: float = 5.0) -> dict:
    deadline = time.monotonic() + deadline_s
    last_error = None
    while time.monotonic() < deadline:
        try:
            with open(path, encoding="utf-8") as handle:
                return json.load(handle)
        except (FileNotFoundError, json.JSONDecodeError) as error:
            last_error = error
            time.sleep(0.05)
    raise AssertionError(f"metrics file was not readable: {last_error}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--client", required=True)
    args = parser.parse_args()

    payload_bytes = 1024 * 1024  # ~0.8 s through the slow reader, outlasting the sample point
    environment = default_environment()

    with tempfile.TemporaryDirectory() as workdir, tempfile.TemporaryFile(
        mode="w+"
    ) as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
        metrics_path = os.path.join(workdir, "metrics.json")
        server_port = free_port()
        proxy_port = free_port()
        server = subprocess.Popen(
            [
                args.server,
                "--listen",
                f"127.0.0.1:{server_port}",
                "--mode",
                "slow-reader",
                "--delay-ms",
                "3",
                "--chunk-bytes",
                "4096",
            ],
            stdout=server_log,
            stderr=server_log,
            text=True,
            env=environment,
        )
        proxy = None
        try:
            wait_for_port(server_port, server)
            proxy = subprocess.Popen(
                [
                    args.proxy,
                    "--listen",
                    f"127.0.0.1:{proxy_port}",
                    "--upstream",
                    f"127.0.0.1:{server_port}",
                    "--metrics-file",
                    metrics_path,
                ],
                stdout=proxy_log,
                stderr=proxy_log,
                text=True,
                env=environment,
            )
            wait_for_port(proxy_port, proxy)

            client = subprocess.Popen(
                [
                    args.client,
                    "--connect",
                    f"127.0.0.1:{proxy_port}",
                    "--connections",
                    "1",
                    "--payload-bytes",
                    str(payload_bytes),
                    "--seed",
                    "555",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=environment,
            )
            time.sleep(0.3)  # let the transfer engage before sampling
            os.kill(proxy.pid, signal.SIGUSR1)

            live = read_metrics(metrics_path)
            if live["active_connections"] != 1 or live["total_accepted"] < 1:
                raise AssertionError(f"unexpected live aggregate: {live}")
            if not isinstance(live["closes"], dict) or "timed_out" not in live["closes"]:
                raise AssertionError(f"closes breakdown missing: {live}")
            live_connections = [c for c in live["connections"] if c["client_bytes_read"] > 0]
            if len(live_connections) != 1:
                raise AssertionError(f"expected one live workload connection: {live['connections']}")
            snapshot = live_connections[0]
            for field in (
                "state",
                "duration_ms",
                "client_bytes_read",
                "upstream_bytes_written",
                "c2u_high_water",
                "c2u_pause_count",
                "u2c_delay_budget_us",
                "faults_applied",
            ):
                if field not in snapshot:
                    raise AssertionError(f"connection snapshot missing {field}: {snapshot}")
            if snapshot["client_bytes_read"] > payload_bytes:
                raise AssertionError(f"snapshot exceeds payload: {snapshot}")

            stdout, stderr = client.communicate(timeout=60)
            if client.returncode != 0 or json.loads(stdout)["successful"] != 1:
                raise RuntimeError(f"transfer failed: stdout={stdout!r} stderr={stderr!r}")
        finally:
            if proxy is not None:
                stop_process(proxy)
            stop_process(server)

        final = read_metrics(metrics_path)
        if final["active_connections"] != 0:
            raise AssertionError(f"shutdown export shows live connections: {final}")
        total_closes = sum(final["closes"].values())
        if total_closes < 1 or final["closes"]["fully_closed"] < 1:
            raise AssertionError(f"shutdown export missing close counts: {final}")
        if final["connections"] != []:
            raise AssertionError(f"shutdown export should list no connections: {final}")
        if final["total_accepted"] != total_closes:
            raise AssertionError(f"accepted/closed mismatch at shutdown: {final}")

    print(
        json.dumps(
            {
                "milestone": 4,
                "scenario": "metrics_export",
                "live_client_bytes_read": snapshot["client_bytes_read"],
                "final_closes": final["closes"],
                "status": "passed",
            }
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"milestone4 metrics failure: {error}", file=sys.stderr)
        raise
