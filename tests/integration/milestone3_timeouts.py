#!/usr/bin/env python3
"""Milestone 3 timeouts: idle connections and hung upstream connects must close
as timed_out via the timerfd path, never by blocking the event loop."""

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


def timed_out_closes(events, reason: str):
    return [
        event
        for event in events_named(events, "connection_closed")
        if event["state"] == "timed_out" and reason in str(event.get("detail", ""))
    ]


def scenario_idle_timeout(args) -> dict:
    """A connection that stops transferring must close as timed_out/idle_timeout."""
    with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
        server_port = free_port()
        server = subprocess.Popen(
            [args.server, "--listen", f"127.0.0.1:{server_port}", "--mode", "echo"],
            stdout=server_log,
            stderr=server_log,
            text=True,
            env=default_environment(),
        )
        proxy = None
        try:
            wait_for_port(server_port, server)
            proxy, proxy_port = start_proxy(args, proxy_log, server_port, ["--idle-timeout-ms", "400"])
            started = time.monotonic()
            with socket.create_connection(("127.0.0.1", proxy_port), timeout=10) as sock:
                sock.settimeout(10)
                sock.sendall(b"ping")
                if sock.recv(4096) != b"ping":
                    raise AssertionError("echo before idling failed")
                # Now go idle; the proxy must close us from its timer, and this
                # blocking read must observe EOF or reset rather than hang.
                try:
                    trailing = sock.recv(4096)
                    if trailing != b"":
                        raise AssertionError(f"unexpected data while idle: {trailing!r}")
                except ConnectionResetError:
                    pass
            elapsed = time.monotonic() - started
            if elapsed < 0.35 or elapsed > 8.0:
                raise AssertionError(f"idle close arrived at an implausible time: {elapsed:.3f}s")
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if timed_out_closes(read_events(proxy_log), "idle_timeout"):
                    break
                time.sleep(0.05)
        finally:
            if proxy is not None:
                stop_process(proxy)
            stop_process(server)

        events = read_events(proxy_log)
        closes = timed_out_closes(events, "idle_timeout")
        if len(closes) != 1:
            raise AssertionError(f"expected one idle_timeout close, got {closes}")
        metrics = parse_detail(str(closes[0]["detail"]))
        if metrics["client_read"] != 4:
            raise AssertionError(f"unexpected transfer accounting before idle close: {metrics}")
    return {"scenario": "idle_timeout", "elapsed_s": round(elapsed, 3), "status": "passed"}


def scenario_connect_timeout(args) -> dict:
    """A SYN-queue-saturated upstream must trip the connect timeout.

    The hang relies on the kernel dropping SYNs to a full accept queue. With
    net.ipv4.tcp_syncookies=1 a handshake can instead complete against the
    full queue (the server silently drops the child), in which case the proxy
    legitimately sees a connected upstream and no timeout can fire; the
    scenario detects that environment and reports itself skipped rather than
    failing. CI disables syncookies so the strict path runs there.
    """
    with tempfile.TemporaryFile(mode="w+") as proxy_log:
        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        fillers = []
        proxy = None
        try:
            blocker.bind(("127.0.0.1", 0))
            blocker.listen(0)
            upstream_port = blocker.getsockname()[1]
            for _ in range(4):
                filler = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                filler.setblocking(False)
                try:
                    filler.connect(("127.0.0.1", upstream_port))
                except BlockingIOError:
                    pass
                fillers.append(filler)
            time.sleep(0.2)  # let the backlog fill before the proxy tries

            proxy, proxy_port = start_proxy(args, proxy_log, upstream_port, ["--connect-timeout-ms", "400"])
            started = time.monotonic()
            with socket.create_connection(("127.0.0.1", proxy_port), timeout=10) as sock:
                sock.settimeout(8)

                # Watch the log for this connection's fate: a connect_timeout
                # close (strict path) or a completed upstream connect
                # (syncookie environment).
                outcome = None
                deadline = time.monotonic() + 6
                while time.monotonic() < deadline and outcome is None:
                    events = read_events(proxy_log)
                    accepted = events_named(events, "connection_accepted")
                    if accepted:
                        workload_id = max(event["connection_id"] for event in accepted)
                        if any(
                            event["connection_id"] == workload_id
                            for event in timed_out_closes(events, "connect_timeout")
                        ):
                            outcome = "timed_out"
                        elif any(
                            event["connection_id"] == workload_id
                            for event in events_named(events, "upstream_connected")
                        ):
                            outcome = "connected"
                    if events_named(events, "upstream_connect_failed"):
                        # The kernel refused (RST) against the full queue, e.g.
                        # tcp_abort_on_overflow=1 or a filler race; the connect
                        # never hangs, so the timeout cannot be exercised.
                        outcome = "refused"
                    if outcome is None:
                        time.sleep(0.05)

                if outcome in ("connected", "refused"):
                    return {
                        "scenario": "connect_timeout",
                        "status": "skipped_environment",
                        "reason": f"upstream connect did not hang ({outcome}) against a full accept queue",
                    }
                if outcome != "timed_out":
                    raise AssertionError(
                        "no connect outcome was observed; last events: "
                        + json.dumps(read_events(proxy_log)[-10:])
                    )

                try:
                    trailing = sock.recv(4096)
                    if trailing != b"":
                        raise AssertionError(f"expected EOF after connect timeout, got {trailing!r}")
                except ConnectionResetError:
                    pass
            elapsed = time.monotonic() - started
            if elapsed < 0.35 or elapsed > 8.0:
                raise AssertionError(f"connect timeout at an implausible time: {elapsed:.3f}s")
        finally:
            if proxy is not None:
                stop_process(proxy)
            for filler in fillers:
                filler.close()
            blocker.close()

        closes = timed_out_closes(read_events(proxy_log), "connect_timeout")
        if len(closes) < 1:
            raise AssertionError("no connect_timeout close was logged")
    return {"scenario": "connect_timeout", "elapsed_s": round(elapsed, 3), "status": "passed"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--client", required=True)
    args = parser.parse_args()

    results = [
        scenario_idle_timeout(args),
        scenario_connect_timeout(args),
    ]
    print(json.dumps({"milestone": 3, "scenarios": results, "status": "passed"}))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"milestone3 timeout failure: {error}", file=sys.stderr)
        raise
