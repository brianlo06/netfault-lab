#!/usr/bin/env python3

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_port(port: int, process: subprocess.Popen[str], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            raise RuntimeError(f"process exited before readiness: stdout={stdout!r} stderr={stderr!r}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.02)
    raise TimeoutError(f"port {port} did not become ready")


def terminate(process: subprocess.Popen[str]) -> tuple[str, str]:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
    try:
        return process.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        stdout, stderr = process.communicate(timeout=2)
        raise RuntimeError(f"process did not stop cleanly: stdout={stdout!r} stderr={stderr!r}")


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
            stable_samples = 0
            previous = current
        time.sleep(0.02)
    raise TimeoutError("proxy file-descriptor count did not settle")


def wait_for_fd_count_at_most(process: subprocess.Popen[str], expected: int, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if fd_count(process) <= expected:
            return
        time.sleep(0.02)
    raise AssertionError(f"proxy file descriptors did not return to baseline {expected}; current={fd_count(process)}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--client", required=True)
    args = parser.parse_args()

    server_port = free_port()
    proxy_port = free_port()
    environment = dict(os.environ)
    environment.setdefault("ASAN_OPTIONS", "detect_leaks=1:abort_on_error=1")

    server: subprocess.Popen[str] | None = subprocess.Popen(
        [args.server, "--listen", f"127.0.0.1:{server_port}", "--mode", "echo"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
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
                "--max-connections",
                "64",
                "--buffer-bytes",
                "65536",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=environment,
        )
        wait_for_port(proxy_port, proxy)
        baseline_proxy_fds = wait_for_stable_fd_count(proxy)

        completed = subprocess.run(
            [
                args.client,
                "--connect",
                f"127.0.0.1:{proxy_port}",
                "--connections",
                "8",
                "--payload-bytes",
                str(1024 * 1024),
                "--seed",
                "12345",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            env=environment,
            check=False,
        )
        if completed.returncode != 0:
            proxy_stdout, proxy_stderr = terminate(proxy)
            server_stdout, server_stderr = terminate(server)
            proxy = None
            server = None
            raise RuntimeError(
                f"client failed: stdout={completed.stdout!r} stderr={completed.stderr!r}; "
                f"proxy_stdout={proxy_stdout!r} proxy_stderr={proxy_stderr!r}; "
                f"server_stdout={server_stdout!r} server_stderr={server_stderr!r}"
            )
        summary = json.loads(completed.stdout)
        if summary["connections"] != 8 or summary["successful"] != 8:
            raise AssertionError(f"unexpected client summary: {summary}")
        wait_for_fd_count_at_most(proxy, baseline_proxy_fds)
    finally:
        if proxy is not None:
            proxy_stdout, proxy_stderr = terminate(proxy)
            if proxy.returncode != 0:
                raise RuntimeError(
                    f"proxy exit={proxy.returncode}: stdout={proxy_stdout!r} stderr={proxy_stderr!r}"
                )
        if server is not None:
            server_stdout, server_stderr = terminate(server)
            if server.returncode != 0:
                raise RuntimeError(
                    f"server exit={server.returncode}: stdout={server_stdout!r} stderr={server_stderr!r}"
                )

    print(
        json.dumps(
            {
                "milestone": 1,
                "connections": 8,
                "payload_bytes_each": 1024 * 1024,
                "proxy_fd_baseline": baseline_proxy_fds,
                "status": "passed",
            }
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # Integration harness must surface all diagnostics.
        print(f"milestone1 integration failure: {error}", file=sys.stderr)
        raise
