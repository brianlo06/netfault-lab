#!/usr/bin/env python3
"""Milestone 3 fault injection: latency, rate limiting, reset and half-close
injection, and reproducible fault application from the master seed."""

import argparse
import json
import os
import socket
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


def start_echo_server(args, server_log):
    server_port = free_port()
    server = subprocess.Popen(
        [args.server, "--listen", f"127.0.0.1:{server_port}", "--mode", "echo"],
        stdout=server_log,
        stderr=server_log,
        text=True,
        env=default_environment(),
    )
    wait_for_port(server_port, server)
    return server, server_port


def start_proxy(args, proxy_log, upstream_port: int, extra: list[str]):
    proxy_port = free_port()
    proxy = subprocess.Popen(
        [
            args.proxy,
            "--listen",
            f"127.0.0.1:{proxy_port}",
            "--upstream",
            f"127.0.0.1:{upstream_port}",
        ]
        + extra,
        stdout=proxy_log,
        stderr=proxy_log,
        text=True,
        env=default_environment(),
    )
    wait_for_port(proxy_port, proxy)
    return proxy, proxy_port


def run_client(args, proxy_port: int, payload_bytes: int, timeout: float = 60):
    client = subprocess.run(
        [
            args.client,
            "--connect",
            f"127.0.0.1:{proxy_port}",
            "--connections",
            "1",
            "--payload-bytes",
            str(payload_bytes),
            "--seed",
            "1234",
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=default_environment(),
        check=False,
    )
    return client


def workload_close(events, minimum_bytes: int):
    closes = [
        event
        for event in events_named(events, "connection_closed")
        if parse_detail(str(event.get("detail", ""))).get("client_read", 0) >= minimum_bytes
    ]
    if len(closes) != 1:
        raise AssertionError(f"expected one workload close, got {closes}")
    return closes[0]


def scenario_latency(args) -> dict:
    """200 ms of injected latency per direction must dominate a loopback echo."""
    payload_bytes = 64 * 1024
    with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
        server, server_port = start_echo_server(args, server_log)
        proxy = None
        try:
            proxy, proxy_port = start_proxy(
                args, proxy_log, server_port, ["--fault-latency-ms", "200", "--fault-seed", "7"]
            )
            client = run_client(args, proxy_port, payload_bytes)
            if client.returncode != 0:
                raise RuntimeError(f"client failed: stdout={client.stdout!r} stderr={client.stderr!r}")
            summary = json.loads(client.stdout)
            if summary["successful"] != 1:
                raise AssertionError(f"latency transfer failed: {summary}")
            # Both directions are delayed, so the echo round trip needs >= ~400 ms.
            if summary["max_duration_us"] < 380_000:
                raise AssertionError(f"latency was not observed: {summary}")
        finally:
            if proxy is not None:
                stop_process(proxy)
            stop_process(server)

        events = read_events(proxy_log)
        metrics = parse_detail(str(workload_close(events, payload_bytes)["detail"]))
        if metrics["faults_applied"] != 1 or metrics["c2u_delayed_segments"] < 1:
            raise AssertionError(f"fault accounting missing: {metrics}")
        if metrics["c2u_delay_budget_us"] < 200_000:
            raise AssertionError(f"delay budget below configured latency: {metrics}")
    return {
        "scenario": "latency",
        "duration_us": summary["max_duration_us"],
        "delayed_segments": metrics["c2u_delayed_segments"],
        "status": "passed",
    }


def scenario_rate_limit(args) -> dict:
    """A 100 KB/s client-to-upstream token bucket must pace a 150 KB transfer."""
    payload_bytes = 150_000
    with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
        server, server_port = start_echo_server(args, server_log)
        proxy = None
        try:
            proxy, proxy_port = start_proxy(
                args,
                proxy_log,
                server_port,
                [
                    "--fault-rate-bytes-per-sec",
                    "100000",
                    "--fault-burst-bytes",
                    "10000",
                    "--fault-direction",
                    "c2u",
                ],
            )
            started = time.monotonic()
            client = run_client(args, proxy_port, payload_bytes)
            elapsed = time.monotonic() - started
            if client.returncode != 0 or json.loads(client.stdout)["successful"] != 1:
                raise RuntimeError(f"client failed: stdout={client.stdout!r} stderr={client.stderr!r}")
            # 150 KB minus the 10 KB burst at 100 KB/s needs at least 1.4 s.
            if elapsed < 1.3:
                raise AssertionError(f"rate limit was not observed: {elapsed:.3f}s")
        finally:
            if proxy is not None:
                stop_process(proxy)
            stop_process(server)

        events = read_events(proxy_log)
        metrics = parse_detail(str(workload_close(events, payload_bytes)["detail"]))
        if metrics["faults_applied"] != 1:
            raise AssertionError(f"faults were not applied: {metrics}")
    return {"scenario": "rate_limit", "elapsed_s": round(elapsed, 3), "status": "passed"}


def scenario_reset_injection(args) -> dict:
    """The connection must reset after the configured forwarded-byte budget."""
    with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
        server, server_port = start_echo_server(args, server_log)
        proxy = None
        try:
            proxy, proxy_port = start_proxy(
                args, proxy_log, server_port, ["--fault-reset-after-bytes", "50000"]
            )
            with socket.create_connection(("127.0.0.1", proxy_port), timeout=10) as sock:
                sock.settimeout(10)
                error_seen = None
                try:
                    for _ in range(64):
                        sock.sendall(b"\x11" * 16_384)
                        try:
                            sock.recv(65_536)
                        except (ConnectionResetError, BrokenPipeError) as error:
                            error_seen = type(error).__name__
                            break
                except (ConnectionResetError, BrokenPipeError) as error:
                    error_seen = type(error).__name__
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if events_named(read_events(proxy_log), "fault_injected"):
                    break
                time.sleep(0.05)
        finally:
            if proxy is not None:
                stop_process(proxy)
            stop_process(server)

        events = read_events(proxy_log)
        injected = [
            event for event in events_named(events, "fault_injected") if "fault=reset" in str(event["detail"])
        ]
        if len(injected) != 1:
            raise AssertionError(f"expected one reset injection event, got {injected}")
        closed = [
            event
            for event in events_named(events, "connection_closed")
            if "fault_reset" in str(event.get("detail", ""))
        ]
        if len(closed) != 1 or closed[0]["state"] != "reset":
            raise AssertionError(f"expected one fault_reset close, got {closed}")
        metrics = parse_detail(str(closed[0]["detail"]))
        total_written = metrics["client_written"] + metrics["upstream_written"]
        if total_written < 50_000:
            raise AssertionError(f"reset fired before the byte budget: {metrics}")
    return {
        "scenario": "reset_injection",
        "client_error": error_seen or "eof",
        "total_written": total_written,
        "status": "passed",
    }


def scenario_half_close_injection(args) -> dict:
    """The proxy must half-close toward the client after N bytes reach it."""
    threshold = 30_000
    sent_bytes = 100_000
    with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
        server, server_port = start_echo_server(args, server_log)
        proxy = None
        try:
            proxy, proxy_port = start_proxy(
                args, proxy_log, server_port, ["--fault-half-close-after-bytes", str(threshold)]
            )
            received = 0
            with socket.create_connection(("127.0.0.1", proxy_port), timeout=10) as sock:
                sock.settimeout(10)
                sock.sendall(b"\x22" * sent_bytes)
                while True:
                    chunk = sock.recv(65_536)
                    if not chunk:
                        break
                    received += len(chunk)
            if received < threshold or received >= sent_bytes:
                raise AssertionError(
                    f"expected EOF between {threshold} and {sent_bytes} echoed bytes, got {received}"
                )
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if events_named(read_events(proxy_log), "fault_injected"):
                    break
                time.sleep(0.05)
        finally:
            if proxy is not None:
                stop_process(proxy)
            stop_process(server)

        events = read_events(proxy_log)
        injected = [
            event
            for event in events_named(events, "fault_injected")
            if "fault=half_close" in str(event["detail"])
        ]
        if len(injected) != 1:
            raise AssertionError(f"expected one half-close injection event, got {injected}")
    return {"scenario": "half_close_injection", "received": received, "status": "passed"}


def scenario_seeded_reproducibility(args) -> dict:
    """With probability 0.5, the set of fault-applied connections must be a pure
    function of the master seed and connection order."""

    def applied_bitmap(seed: int) -> str:
        with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
            server, server_port = start_echo_server(args, server_log)
            proxy = None
            try:
                proxy, proxy_port = start_proxy(
                    args,
                    proxy_log,
                    server_port,
                    [
                        "--fault-latency-ms",
                        "1",
                        "--fault-probability",
                        "0.5",
                        "--fault-seed",
                        str(seed),
                    ],
                )
                for _ in range(16):  # sequential, so connection ids are 1..16 in order
                    with socket.create_connection(("127.0.0.1", proxy_port), timeout=10) as sock:
                        sock.settimeout(10)
                        sock.sendall(b"\x33" * 512)
                        sock.shutdown(socket.SHUT_WR)
                        while sock.recv(4096):
                            pass
            finally:
                if proxy is not None:
                    stop_process(proxy)
                stop_process(server)
            events = events_named(read_events(proxy_log), "fault_config")
            per_connection = {
                event["connection_id"]: parse_detail(str(event["detail"]))["applied"] for event in events
            }
            # The port-readiness probe takes the first connection id(s); the 16
            # workload connections are always the highest ids in accept order.
            return "".join(str(per_connection[key]) for key in sorted(per_connection)[-16:])

    first = applied_bitmap(97)
    second = applied_bitmap(97)
    other = applied_bitmap(1097)
    if len(first) != 16 or first != second:
        raise AssertionError(f"same seed produced different fault application: {first} vs {second}")
    if first == other:
        raise AssertionError(f"different seeds produced identical 16-connection bitmaps: {first}")
    if "1" not in first or "0" not in first:
        raise AssertionError(f"probability 0.5 produced a degenerate bitmap: {first}")
    return {"scenario": "seeded_reproducibility", "bitmap": first, "status": "passed"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--client", required=True)
    args = parser.parse_args()

    results = [
        scenario_latency(args),
        scenario_rate_limit(args),
        scenario_reset_injection(args),
        scenario_half_close_injection(args),
        scenario_seeded_reproducibility(args),
    ]
    print(json.dumps({"milestone": 3, "scenarios": results, "status": "passed"}))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"milestone3 fault failure: {error}", file=sys.stderr)
        raise
