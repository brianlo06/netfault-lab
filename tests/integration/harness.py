"""Shared helpers for NetFault Lab integration tests."""

import json
import os
import signal
import socket
import subprocess
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
    raise TimeoutError("file-descriptor count did not settle")


def wait_for_fd_count_at_most(process: subprocess.Popen[str], expected: int, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if fd_count(process) <= expected:
            return
        time.sleep(0.02)
    raise AssertionError(f"fd count remained above {expected}: {fd_count(process)}")


def rss_kib(process: subprocess.Popen[str]) -> int:
    with open(f"/proc/{process.pid}/status", encoding="ascii") as status:
        for line in status:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    raise RuntimeError("VmRSS not found")


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
    """Parse JSON Lines events, skipping a trailing line still being written.

    Tests poll the log while the proxy is running, so the reader can observe a
    partially-completed write(). Once the process has exited every line parses.
    """
    log.flush()
    log.seek(0)
    events = []
    for line in log:
        if not line.strip():
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def events_named(events: list[dict[str, object]], name: str) -> list[dict[str, object]]:
    return [event for event in events if event.get("event") == name]


def default_environment() -> dict[str, str]:
    environment = dict(os.environ)
    environment.setdefault("ASAN_OPTIONS", "detect_leaks=1:abort_on_error=1")
    return environment
