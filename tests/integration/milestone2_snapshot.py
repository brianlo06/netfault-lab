#!/usr/bin/env python3
"""Milestone 2 metrics snapshot: SIGUSR1 must emit queryable counters for the
listener and every live connection without disturbing the transfer."""

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
    events_named,
    free_port,
    parse_detail,
    read_events,
    stop_process,
    wait_for_port,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--client", required=True)
    args = parser.parse_args()

    server_port = free_port()
    proxy_port = free_port()
    payload_bytes = 512 * 1024
    environment = default_environment()

    with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
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
                    "86420",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=environment,
            )
            time.sleep(0.5)  # let the transfer engage before sampling
            os.kill(proxy.pid, signal.SIGUSR1)

            deadline = time.monotonic() + 5
            snapshots = []
            while time.monotonic() < deadline:
                snapshots = events_named(read_events(proxy_log), "metrics_snapshot")
                if snapshots:
                    break
                time.sleep(0.05)
            if not snapshots:
                raise AssertionError("no metrics_snapshot event after SIGUSR1")

            stdout, stderr = client.communicate(timeout=60)
            if client.returncode != 0 or json.loads(stdout)["successful"] != 1:
                raise RuntimeError(f"transfer failed after snapshot: stdout={stdout!r} stderr={stderr!r}")
        finally:
            if proxy is not None:
                stop_process(proxy)
            stop_process(server)

        events = read_events(proxy_log)
        snapshot = parse_detail(str(events_named(events, "metrics_snapshot")[0]["detail"]))
        # total_accepted includes the zero-byte wait_for_port probe connection.
        if snapshot["active_connections"] != 1 or snapshot["total_accepted"] < 1:
            raise AssertionError(f"unexpected aggregate snapshot: {snapshot}")

        connection_snapshots = events_named(events, "connection_snapshot")
        if len(connection_snapshots) != 1:
            raise AssertionError(f"expected one connection snapshot, got {len(connection_snapshots)}")
        connection_metrics = parse_detail(str(connection_snapshots[0]["detail"]))
        if connection_metrics["snapshot"] != 1 or connection_metrics["client_read"] <= 0:
            raise AssertionError(f"snapshot missing live counters: {connection_metrics}")
        if connection_metrics["client_read"] > payload_bytes:
            raise AssertionError(f"snapshot counted more bytes than the payload: {connection_metrics}")

        closed = [
            event
            for event in events_named(events, "connection_closed")
            if parse_detail(str(event.get("detail", ""))).get("client_read", 0) != 0
        ]
        if len(closed) != 1 or closed[0]["state"] != "fully_closed":
            raise AssertionError(f"transfer must still close orderly after a snapshot: {closed}")
        final_metrics = parse_detail(str(closed[0]["detail"]))
        if final_metrics["client_read"] != payload_bytes:
            raise AssertionError(f"final byte accounting mismatch: {final_metrics}")

    print(
        json.dumps(
            {
                "milestone": 2,
                "scenario": "metrics_snapshot",
                "snapshot_client_read": connection_metrics["client_read"],
                "payload_bytes": payload_bytes,
                "status": "passed",
            }
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"milestone2 snapshot failure: {error}", file=sys.stderr)
        raise
