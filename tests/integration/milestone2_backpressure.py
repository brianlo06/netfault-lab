#!/usr/bin/env python3

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from typing import TextIO


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_port(port: int, process: subprocess.Popen[str], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"process exited before port {port} was ready")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.02)
    raise TimeoutError(f"port {port} did not become ready")


def stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired as error:
        process.kill()
        process.wait(timeout=2)
        raise RuntimeError(f"process {process.args} did not stop cleanly") from error
    if process.returncode != 0:
        raise RuntimeError(f"process {process.args} exited with {process.returncode}")


def fd_count(process: subprocess.Popen[str]) -> int:
    return len(os.listdir(f"/proc/{process.pid}/fd"))


def wait_for_stable_fd_count(process: subprocess.Popen[str], timeout: float = 2.0) -> int:
    deadline = time.monotonic() + timeout
    previous = -1
    stable_samples = 0
    while time.monotonic() < deadline:
        current = fd_count(process)
        if current == previous:
            stable_samples += 1
            if stable_samples >= 3:
                return current
        else:
            previous = current
            stable_samples = 0
        time.sleep(0.02)
    raise TimeoutError("proxy file-descriptor count did not settle")


def wait_for_fd_count_at_most(process: subprocess.Popen[str], expected: int, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if fd_count(process) <= expected:
            return
        time.sleep(0.02)
    raise AssertionError(f"proxy fd count remained above {expected}: {fd_count(process)}")


def parse_detail(detail: str) -> dict[str, int | str]:
    result: dict[str, int | str] = {}
    for item in detail.split(","):
        if "=" not in item:
            result["reason"] = item
            continue
        key, value = item.split("=", 1)
        try:
            result[key] = int(value)
        except ValueError:
            result[key] = value
    return result


def read_events(log: TextIO) -> list[dict[str, object]]:
    log.flush()
    log.seek(0)
    return [json.loads(line) for line in log if line.strip()]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--client", required=True)
    args = parser.parse_args()

    server_port = free_port()
    proxy_port = free_port()
    payload_bytes = 4 * 1024 * 1024
    buffer_bytes = 8192
    low_water_bytes = 2048
    high_water_bytes = 8192
    environment = dict(os.environ)
    environment.setdefault("ASAN_OPTIONS", "detect_leaks=1:abort_on_error=1")

    with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
        server = subprocess.Popen(
            [
                args.server,
                "--listen",
                f"127.0.0.1:{server_port}",
                "--mode",
                "slow-reader",
                "--delay-ms",
                "2",
                "--chunk-bytes",
                "4096",
            ],
            stdout=server_log,
            stderr=server_log,
            text=True,
            env=environment,
        )
        proxy: subprocess.Popen[str] | None = None
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
                    "8",
                    "--buffer-bytes",
                    str(buffer_bytes),
                    "--low-water-bytes",
                    str(low_water_bytes),
                    "--high-water-bytes",
                    str(high_water_bytes),
                ],
                stdout=proxy_log,
                stderr=proxy_log,
                text=True,
                env=environment,
            )
            wait_for_port(proxy_port, proxy)
            baseline_fds = wait_for_stable_fd_count(proxy)

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
                    "24680",
                ],
                capture_output=True,
                text=True,
                timeout=20,
                env=environment,
                check=False,
            )
            if client.returncode != 0:
                raise RuntimeError(f"client failed: stdout={client.stdout!r} stderr={client.stderr!r}")
            summary = json.loads(client.stdout)
            if summary["successful"] != 1:
                raise AssertionError(f"unexpected client summary: {summary}")
            wait_for_fd_count_at_most(proxy, baseline_fds)
        finally:
            if proxy is not None:
                stop_process(proxy)
            stop_process(server)

        events = read_events(proxy_log)
        closed = [
            event
            for event in events
            if event.get("event") == "connection_closed"
            and f"upstream_written={payload_bytes}" in str(event.get("detail", ""))
        ]
        if len(closed) != 1:
            raise AssertionError(f"expected one workload close event, found {len(closed)}")
        metrics = parse_detail(str(closed[0]["detail"]))
        if metrics["client_read"] != payload_bytes or metrics["upstream_written"] != payload_bytes:
            raise AssertionError(f"client-to-upstream byte accounting mismatch: {metrics}")
        if metrics["upstream_read"] != payload_bytes or metrics["client_written"] != payload_bytes:
            raise AssertionError(f"upstream-to-client byte accounting mismatch: {metrics}")
        if not high_water_bytes <= metrics["c2u_high_water"] <= buffer_bytes:
            raise AssertionError(f"queue did not reach its configured high-water mark: {metrics}")
        if metrics["c2u_pause_count"] < 1 or metrics["c2u_resume_count"] < 1:
            raise AssertionError(f"backpressure pause/resume was not observed: {metrics}")
        if metrics["c2u_saturation_count"] < 1 or metrics["c2u_paused_us"] <= 0:
            raise AssertionError(f"saturation duration was not recorded: {metrics}")
        if metrics["rejected_bytes"] != 0:
            raise AssertionError(f"pause policy must not reject bytes: {metrics}")

        slow_writer_port = free_port()
        slow_writer_server = subprocess.Popen(
            [
                args.server,
                "--listen",
                f"127.0.0.1:{slow_writer_port}",
                "--mode",
                "slow-writer",
                "--delay-ms",
                "1",
                "--chunk-bytes",
                "4096",
            ],
            stdout=server_log,
            stderr=server_log,
            text=True,
            env=environment,
        )
        try:
            wait_for_port(slow_writer_port, slow_writer_server)
            slow_writer_client = subprocess.run(
                [
                    args.client,
                    "--connect",
                    f"127.0.0.1:{slow_writer_port}",
                    "--connections",
                    "1",
                    "--payload-bytes",
                    str(64 * 1024),
                    "--seed",
                    "13579",
                ],
                capture_output=True,
                text=True,
                timeout=10,
                env=environment,
                check=False,
            )
            if slow_writer_client.returncode != 0:
                raise RuntimeError(
                    "slow-writer client failed: "
                    f"stdout={slow_writer_client.stdout!r} stderr={slow_writer_client.stderr!r}"
                )
            if json.loads(slow_writer_client.stdout)["successful"] != 1:
                raise AssertionError(f"slow-writer response failed: {slow_writer_client.stdout}")
        finally:
            stop_process(slow_writer_server)

    print(
        json.dumps(
            {
                "milestone": 2,
                "scenario": "slow-reader-backpressure",
                "payload_bytes": payload_bytes,
                "buffer_bytes": buffer_bytes,
                "c2u_high_water": metrics["c2u_high_water"],
                "c2u_pause_count": metrics["c2u_pause_count"],
                "c2u_paused_us": metrics["c2u_paused_us"],
                "eagain_events": metrics["eagain_events"],
                "slow_writer_mode": "passed",
                "status": "passed",
            }
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"milestone2 integration failure: {error}", file=sys.stderr)
        raise
