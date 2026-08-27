#!/usr/bin/env python3
"""Milestone 2 fault scenarios: both half-close directions, upstream refusal,
abrupt endpoint termination, and OS-level forced partial writes."""

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
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


def start_proxy(args, proxy_log, upstream_port: int, extra: list[str] | None = None):
    proxy_port = free_port()
    proxy = subprocess.Popen(
        [
            args.proxy,
            "--listen",
            f"127.0.0.1:{proxy_port}",
            "--upstream",
            f"127.0.0.1:{upstream_port}",
            "--max-connections",
            "8",
        ]
        + (extra or []),
        stdout=proxy_log,
        stderr=proxy_log,
        text=True,
        env=default_environment(),
    )
    wait_for_port(proxy_port, proxy)
    return proxy, proxy_port


def start_server(args, server_log, mode: str, extra: list[str] | None = None):
    server_port = free_port()
    server = subprocess.Popen(
        [args.server, "--listen", f"127.0.0.1:{server_port}", "--mode", mode] + (extra or []),
        stdout=server_log,
        stderr=server_log,
        text=True,
        env=default_environment(),
    )
    wait_for_port(server_port, server)
    return server, server_port


def close_events(events) -> list[dict]:
    return events_named(events, "connection_closed")


def workload_closes(events) -> list[dict]:
    """Close events with byte activity, excluding the zero-byte wait_for_port probe."""
    return [
        event
        for event in close_events(events)
        if any(
            parse_detail(str(event.get("detail", ""))).get(counter, 0) != 0
            for counter in ("client_read", "upstream_read", "client_written", "upstream_written")
        )
    ]


def connection_events(events, name: str, connection_id) -> list[dict]:
    return [event for event in events_named(events, name) if event.get("connection_id") == connection_id]


def scenario_client_half_close(args) -> dict:
    """Client sends, half-closes first; the read-until-eof server never echoes."""
    payload_bytes = 200_000
    with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
        server, server_port = start_server(args, server_log, "read-until-eof")
        proxy = None
        try:
            proxy, proxy_port = start_proxy(args, proxy_log, server_port)
            with socket.create_connection(("127.0.0.1", proxy_port), timeout=10) as sock:
                sock.settimeout(10)
                sock.sendall(b"\x5a" * payload_bytes)
                sock.shutdown(socket.SHUT_WR)
                trailing = sock.recv(4096)
                if trailing != b"":
                    raise AssertionError(f"expected EOF with no echoed bytes, got {len(trailing)} bytes")
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if workload_closes(read_events(proxy_log)):
                    break
                time.sleep(0.05)
        finally:
            if proxy is not None:
                stop_process(proxy)
            stop_process(server)

        events = read_events(proxy_log)
        closed = workload_closes(events)
        if len(closed) != 1 or closed[0]["state"] != "fully_closed":
            raise AssertionError(f"expected one orderly workload close, got {closed}")
        metrics = parse_detail(str(closed[0]["detail"]))
        if metrics["client_read"] != payload_bytes or metrics["upstream_written"] != payload_bytes:
            raise AssertionError(f"forwarded byte mismatch: {metrics}")
        if metrics["client_written"] != 0:
            raise AssertionError(f"read-until-eof server must not produce data: {metrics}")
        forwarded = [
            str(event["detail"])
            for event in connection_events(events, "half_close_forwarded", closed[0]["connection_id"])
        ]
        if "direction=upstream_write" not in forwarded or "direction=client_write" not in forwarded:
            raise AssertionError(f"expected both half-close directions, got {forwarded}")
        if forwarded.index("direction=upstream_write") > forwarded.index("direction=client_write"):
            raise AssertionError(f"client-initiated half-close must reach upstream first: {forwarded}")
    return {"scenario": "client_half_close", "payload_bytes": payload_bytes, "status": "passed"}


def scenario_upstream_half_close(args) -> dict:
    """Server sends, half-closes first, then drains what the client still sends."""
    send_bytes = 150_000
    reply_bytes = 5_000
    with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
        server, server_port = start_server(
            args, server_log, "send-then-half-close", ["--send-bytes", str(send_bytes)]
        )
        proxy = None
        try:
            proxy, proxy_port = start_proxy(args, proxy_log, server_port)
            with socket.create_connection(("127.0.0.1", proxy_port), timeout=10) as sock:
                sock.settimeout(10)
                received = 0
                while True:
                    chunk = sock.recv(65_536)
                    if not chunk:
                        break
                    received += len(chunk)
                if received != send_bytes:
                    raise AssertionError(f"expected {send_bytes} bytes before EOF, got {received}")
                sock.sendall(b"\xa5" * reply_bytes)
                sock.shutdown(socket.SHUT_WR)
            deadline = time.monotonic() + 5

            def reply_closes(events):
                # The send-then-half-close server also pushes bytes at the
                # wait_for_port probe, so identify the workload connection by
                # the reply bytes only the real client sends.
                return [
                    event
                    for event in close_events(events)
                    if parse_detail(str(event.get("detail", ""))).get("client_read", 0) == reply_bytes
                ]

            while time.monotonic() < deadline:
                if reply_closes(read_events(proxy_log)):
                    break
                time.sleep(0.05)
        finally:
            if proxy is not None:
                stop_process(proxy)
            stop_process(server)

        events = read_events(proxy_log)
        closed = reply_closes(events)
        if len(closed) != 1 or closed[0]["state"] != "fully_closed":
            raise AssertionError(f"expected one orderly workload close, got {closed}")
        metrics = parse_detail(str(closed[0]["detail"]))
        if metrics["upstream_read"] != send_bytes or metrics["client_written"] != send_bytes:
            raise AssertionError(f"server payload mismatch: {metrics}")
        if metrics["upstream_written"] != reply_bytes:
            raise AssertionError(f"post-half-close reply mismatch: {metrics}")
        forwarded = [
            str(event["detail"])
            for event in connection_events(events, "half_close_forwarded", closed[0]["connection_id"])
        ]
        if forwarded.index("direction=client_write") > forwarded.index("direction=upstream_write"):
            raise AssertionError(f"upstream-initiated half-close must reach the client first: {forwarded}")
    return {"scenario": "upstream_half_close", "send_bytes": send_bytes, "status": "passed"}


def scenario_upstream_refused(args) -> dict:
    """The upstream port has no listener; the client connection must fail fast."""
    refused_port = free_port()  # nothing is listening here
    with tempfile.TemporaryFile(mode="w+") as proxy_log:
        proxy, proxy_port = start_proxy(args, proxy_log, refused_port)
        try:
            with socket.create_connection(("127.0.0.1", proxy_port), timeout=10) as sock:
                sock.settimeout(5)
                try:
                    outcome = sock.recv(4096)
                    if outcome != b"":
                        raise AssertionError(f"expected EOF after refused upstream, got {outcome!r}")
                except ConnectionResetError:
                    pass
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                events = read_events(proxy_log)
                refused = events_named(events, "upstream_connect_failed") + [
                    event
                    for event in close_events(events)
                    if "upstream_connect=" in str(event.get("detail", ""))
                ]
                if refused:
                    break
                time.sleep(0.05)
        finally:
            stop_process(proxy)
        if not refused:
            raise AssertionError(f"no upstream-refusal evidence in proxy log: {read_events(proxy_log)}")
    return {"scenario": "upstream_refused", "status": "passed"}


def scenario_abrupt_termination(args) -> dict:
    """SIGKILL the server mid-transfer; the proxy must close with reset/failed."""
    payload = b"\x3c" * (2 * 1024 * 1024)
    with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
        server, server_port = start_server(
            args, server_log, "slow-reader", ["--delay-ms", "5", "--chunk-bytes", "4096"]
        )
        proxy = None
        client_error: list[str] = []
        try:
            proxy, proxy_port = start_proxy(args, proxy_log, server_port)

            def pump() -> None:
                try:
                    with socket.create_connection(("127.0.0.1", proxy_port), timeout=10) as sock:
                        sock.settimeout(10)
                        sock.sendall(payload)
                        while sock.recv(65_536):
                            pass
                except OSError as error:
                    client_error.append(type(error).__name__)

            client = threading.Thread(target=pump)
            client.start()
            time.sleep(0.3)
            server.kill()
            server.wait(timeout=5)
            client.join(timeout=15)
            if client.is_alive():
                raise AssertionError("client thread hung after server was killed")
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if workload_closes(read_events(proxy_log)):
                    break
                time.sleep(0.05)
        finally:
            if proxy is not None:
                stop_process(proxy)
            if server.poll() is None:
                server.kill()
                server.wait(timeout=5)

        closed = workload_closes(read_events(proxy_log))
        if len(closed) != 1 or closed[0]["state"] not in ("reset", "failed"):
            raise AssertionError(f"expected one reset/failed close after SIGKILL, got {closed}")
    return {
        "scenario": "abrupt_termination",
        "close_state": closed[0]["state"],
        "client_error": client_error[0] if client_error else "eof",
        "status": "passed",
    }


def scenario_forced_partial_writes(args) -> dict:
    """Small upstream SO_SNDBUF plus a slow reader forces short kernel writes.

    Loopback TCP copies up to its ~64 KiB size goal per send before checking
    memory, so the flushed span must exceed that for send() to return a
    positive short count. Only the upstream socket is constrained; client-side
    inflow at kernel defaults fills the 256 KiB proxy queue past the size goal.
    """
    payload_bytes = 1024 * 1024
    with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
        server, server_port = start_server(
            args, server_log, "slow-reader", ["--delay-ms", "2", "--chunk-bytes", "4096"]
        )
        proxy = None
        try:
            proxy, proxy_port = start_proxy(
                args,
                proxy_log,
                server_port,
                [
                    "--socket-buffer-bytes",
                    "4096",
                    "--buffer-bytes",
                    "262144",
                    "--high-water-bytes",
                    "262144",
                    "--low-water-bytes",
                    "65536",
                ],
            )
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
                    "97531",
                ],
                capture_output=True,
                text=True,
                timeout=60,
                env=default_environment(),
                check=False,
            )
            if client.returncode != 0 or json.loads(client.stdout)["successful"] != 1:
                raise RuntimeError(f"client failed: stdout={client.stdout!r} stderr={client.stderr!r}")
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if workload_closes(read_events(proxy_log)):
                    break
                time.sleep(0.05)
        finally:
            if proxy is not None:
                stop_process(proxy)
            stop_process(server)

        closed = workload_closes(read_events(proxy_log))
        if len(closed) != 1 or closed[0]["state"] != "fully_closed":
            raise AssertionError(f"expected one orderly workload close, got {closed}")
        metrics = parse_detail(str(closed[0]["detail"]))
        if metrics["client_read"] != payload_bytes or metrics["client_written"] != payload_bytes:
            raise AssertionError(f"round-trip byte mismatch: {metrics}")
        if metrics["partial_writes"] < 1:
            raise AssertionError(f"expected forced partial writes with a 4096-byte SO_SNDBUF: {metrics}")
        if metrics["eagain_events"] < 1:
            raise AssertionError(f"expected send EAGAIN under saturation: {metrics}")
    return {
        "scenario": "forced_partial_writes",
        "partial_writes": metrics["partial_writes"],
        "eagain_events": metrics["eagain_events"],
        "status": "passed",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--client", required=True)
    args = parser.parse_args()

    results = [
        scenario_client_half_close(args),
        scenario_upstream_half_close(args),
        scenario_upstream_refused(args),
        scenario_abrupt_termination(args),
        scenario_forced_partial_writes(args),
    ]
    print(json.dumps({"milestone": 2, "scenarios": results, "status": "passed"}))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"milestone2 fault-scenario failure: {error}", file=sys.stderr)
        raise
