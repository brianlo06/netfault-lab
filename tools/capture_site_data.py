#!/usr/bin/env python3
"""Capture real NetFault Lab runs as JSON documents for the project website.

Every scenario runs the actual proxy, server, and client binaries. For each
run this records:

  * the proxy's complete JSON Lines event log,
  * a time series of metrics documents sampled by sending SIGUSR1 on an
    interval and reading the atomically written metrics file,
  * the client's machine-readable summary.

Nothing is synthesized. Re-running this script regenerates the site data.

Usage:
  python3 tools/capture_site_data.py --build-dir build/release --out site/data
"""

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tests", "integration"))
from harness import free_port, read_events, wait_for_port  # noqa: E402

SAMPLE_INTERVAL_S = 0.02

# Log events produced by the capture apparatus itself (the SIGUSR1 sampling
# loop). They are kept in the captured document for full fidelity and tagged
# so the site can hide them by default without discarding anything.
SAMPLING_ARTIFACT_EVENTS = {"metrics_snapshot", "connection_snapshot", "metrics_file_written"}


def wait_for_proxy_listening(process: subprocess.Popen, log_file, timeout: float = 10.0) -> None:
    """Wait for the proxy's own 'listening' event.

    Deliberately does not connect: any probe connection would appear in the
    capture as a spurious zero-byte connection alongside the real workload.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("proxy exited before it began listening")
        if any(event.get("event") == "proxy_started" for event in read_events(log_file)):
            return
        time.sleep(0.02)
    raise TimeoutError("proxy did not report that it was listening")


class MetricsSampler:
    """Polls the proxy for metrics snapshots while a workload runs.

    Sends SIGUSR1, then reads the metrics file. Writes are atomic (temp file
    plus rename), so a read never observes partial JSON; documents are
    deduplicated on the proxy's own timestamp so a re-read of an unchanged
    file is not recorded twice.
    """

    def __init__(self, pid: int, metrics_path: str):
        self._pid = pid
        self._path = metrics_path
        self._samples: list[dict] = []
        self._seen_timestamps: set[int] = set()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self) -> "MetricsSampler":
        self._thread.start()
        return self

    def __exit__(self, *_exc) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                os.kill(self._pid, signal.SIGUSR1)
            except ProcessLookupError:
                return
            time.sleep(0.004)  # let the event loop service the signal and write
            try:
                with open(self._path, encoding="utf-8") as handle:
                    document = json.load(handle)
            except (FileNotFoundError, json.JSONDecodeError):
                time.sleep(SAMPLE_INTERVAL_S)
                continue
            stamp = document.get("timestamp_ms")
            if stamp is not None and stamp not in self._seen_timestamps:
                self._seen_timestamps.add(stamp)
                self._samples.append(document)
            time.sleep(SAMPLE_INTERVAL_S)

    @property
    def samples(self) -> list[dict]:
        return self._samples


def resolve_queue_config(proxy_args: list[str]) -> dict:
    """Mirror the proxy CLI's watermark defaults so the site can draw the queue.

    Kept in step with src/proxy/main.cpp: an explicit buffer size raises the
    high-water mark with it, and either of those sets the low-water mark to
    half the high-water mark unless it was given explicitly.
    """
    values = {}
    for index, argument in enumerate(proxy_args):
        if argument.startswith("--") and index + 1 < len(proxy_args):
            values[argument] = proxy_args[index + 1]
    capacity = int(values.get("--buffer-bytes", 65_536))
    high = int(values["--high-water-bytes"]) if "--high-water-bytes" in values else capacity
    if "--low-water-bytes" in values:
        low = int(values["--low-water-bytes"])
    elif "--buffer-bytes" in values or "--high-water-bytes" in values:
        low = high // 2
    else:
        low = 32_768
    return {"queue_capacity_bytes": capacity, "low_water_bytes": low, "high_water_bytes": high}


def start_process(command: list[str], log_file) -> subprocess.Popen:
    return subprocess.Popen(command, stdout=log_file, stderr=log_file, text=True, env=os.environ.copy())


def stop_process(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def capture(
    binaries: dict,
    out_dir: str,
    key: str,
    title: str,
    summary: str,
    server_args: list[str],
    proxy_args: list[str],
    client_args: list[str],
    raw_client=None,
) -> dict:
    """Run one scenario end to end and write its captured document."""
    server_port = free_port()
    proxy_port = free_port()
    metrics_path = os.path.join(out_dir, f".{key}-metrics.json")
    server_log_path = os.path.join(out_dir, f".{key}-server.log")
    proxy_log_path = os.path.join(out_dir, f".{key}-proxy.log")

    with open(server_log_path, "w+") as server_log, open(proxy_log_path, "w+") as proxy_log:
        server = start_process(
            [binaries["server"], "--listen", f"127.0.0.1:{server_port}"] + server_args, server_log
        )
        proxy = None
        client_summary = None
        try:
            wait_for_port(server_port, server)
            proxy = start_process(
                [
                    binaries["proxy"],
                    "--listen",
                    f"127.0.0.1:{proxy_port}",
                    "--upstream",
                    f"127.0.0.1:{server_port}",
                    "--metrics-file",
                    metrics_path,
                ]
                + proxy_args,
                proxy_log,
            )
            wait_for_proxy_listening(proxy, proxy_log)

            with MetricsSampler(proxy.pid, metrics_path) as sampler:
                started_ms = int(time.time() * 1000)
                if raw_client is not None:
                    raw_client(proxy_port)
                else:
                    completed = subprocess.run(
                        [binaries["client"], "--connect", f"127.0.0.1:{proxy_port}"] + client_args,
                        capture_output=True,
                        text=True,
                        timeout=120,
                        check=False,
                    )
                    if completed.stdout.strip():
                        client_summary = json.loads(completed.stdout)
                time.sleep(0.25)  # capture the tail of the run
                samples = list(sampler.samples)
        finally:
            if proxy is not None:
                stop_process(proxy)
            stop_process(server)

        events = read_events(proxy_log)

    for path in (metrics_path, server_log_path, proxy_log_path):
        if os.path.exists(path):
            os.remove(path)

    for event in events:
        if event.get("event") in SAMPLING_ARTIFACT_EVENTS:
            event["capture_artifact"] = True

    document = {
        "key": key,
        "title": title,
        "summary": summary,
        "captured_at_ms": started_ms,
        "config": resolve_queue_config(proxy_args),
        "command": {
            "server": " ".join(["netfault-server", "--listen", "127.0.0.1:9000"] + server_args),
            "proxy": " ".join(
                ["netfault-proxy", "--listen", "127.0.0.1:8080", "--upstream", "127.0.0.1:9000"] + proxy_args
            ),
            "client": (
                " ".join(["netfault-client", "--connect", "127.0.0.1:8080"] + client_args)
                if raw_client is None
                else "a scripted socket workload (see tools/capture_site_data.py)"
            ),
        },
        "events": events,
        "metrics_samples": samples,
        "client_summary": client_summary,
    }
    out_path = os.path.join(out_dir, f"{key}.json")
    with open(out_path, "w", encoding="utf-8") as handle:
        json.dump(document, handle, separators=(",", ":"))
    print(
        f"  {key}: {len(events)} events, {len(samples)} metrics samples "
        f"-> {out_path} ({os.path.getsize(out_path) // 1024} KiB)"
    )
    return document


def reset_workload(port: int) -> None:
    """Push until the proxy injects its configured reset."""
    with socket.create_connection(("127.0.0.1", port), timeout=10) as sock:
        sock.settimeout(10)
        try:
            for _ in range(64):
                sock.sendall(b"\x41" * 16_384)
                sock.recv(65_536)
        except (ConnectionResetError, BrokenPipeError):
            pass


def idle_workload(port: int) -> None:
    """Exchange once, then go quiet and wait for the idle timer to close us."""
    with socket.create_connection(("127.0.0.1", port), timeout=10) as sock:
        sock.settimeout(10)
        sock.sendall(b"hello netfault")
        sock.recv(4096)
        try:
            sock.recv(4096)  # returns empty at the proxy's idle close
        except (ConnectionResetError, TimeoutError):
            pass


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build-dir", default="build/release")
    parser.add_argument("--out", default="site/data")
    args = parser.parse_args()

    binaries = {
        "proxy": os.path.abspath(os.path.join(args.build_dir, "netfault-proxy")),
        "server": os.path.abspath(os.path.join(args.build_dir, "netfault-server")),
        "client": os.path.abspath(os.path.join(args.build_dir, "netfault-client")),
    }
    for name, path in binaries.items():
        if not os.path.exists(path):
            raise SystemExit(f"missing {name} binary at {path}; build the release preset first")
    os.makedirs(args.out, exist_ok=True)

    print("capturing scenarios:")
    manifest = []

    manifest.append(
        capture(
            binaries,
            args.out,
            key="backpressure",
            title="Backpressure under a slow reader",
            summary=(
                "The upstream reads deliberately slowly and its socket buffer is capped, so the kernel "
                "cannot absorb the backlog. The proxy's fixed-capacity queue fills to its high-water mark, "
                "reads pause, the queue drains to the low-water mark, and reads resume — for the whole "
                "transfer, with no unbounded buffering and no dropped bytes."
            ),
            server_args=["--mode", "slow-reader", "--delay-ms", "5", "--chunk-bytes", "4096"],
            proxy_args=[
                "--buffer-bytes", "16384",
                "--low-water-bytes", "4096",
                "--high-water-bytes", "16384",
                "--socket-buffer-bytes", "4096",
            ],
            client_args=["--connections", "1", "--payload-bytes", "1048576", "--seed", "101"],
        )
    )

    manifest.append(
        capture(
            binaries,
            args.out,
            key="latency",
            title="Deterministic latency and jitter injection",
            summary=(
                "Every forwarded chunk is held 40 ms ± 10 ms of seeded jitter in each direction. Bytes stay "
                "in the same bounded queue with an eligibility time, so delay never creates hidden storage, "
                "and the event loop never sleeps — a timerfd wakes it when bytes come due."
            ),
            server_args=["--mode", "echo"],
            proxy_args=[
                "--fault-latency-ms", "40",
                "--fault-jitter-ms", "10",
                "--fault-seed", "42",
            ],
            client_args=[
                "--mode", "request-response",
                "--connections", "1",
                "--requests", "16",
                "--warmup", "2",
                "--request-bytes", "1024",
                "--seed", "7",
            ],
        )
    )

    manifest.append(
        capture(
            binaries,
            args.out,
            key="rate-limit",
            title="Token-bucket rate limiting",
            summary=(
                "A 100 KB/s token bucket with a 10 KB burst paces the client-to-upstream direction. Refill "
                "uses integer arithmetic that carries sub-byte remainders, so long-run throughput equals the "
                "configured rate exactly."
            ),
            server_args=["--mode", "echo"],
            proxy_args=[
                "--fault-rate-bytes-per-sec", "100000",
                "--fault-burst-bytes", "10000",
                "--fault-direction", "c2u",
            ],
            client_args=["--connections", "1", "--payload-bytes", "200000", "--seed", "31"],
        )
    )

    manifest.append(
        capture(
            binaries,
            args.out,
            key="reset",
            title="Injected connection reset",
            summary=(
                "The proxy is configured to reset the connection once it has forwarded 50,000 bytes. The "
                "fault fires at the byte budget and the connection closes in a distinct reset state — the "
                "packet-capture test in the repository confirms a real RST reaches the wire."
            ),
            server_args=["--mode", "echo"],
            proxy_args=["--fault-reset-after-bytes", "50000"],
            client_args=[],
            raw_client=reset_workload,
        )
    )

    manifest.append(
        capture(
            binaries,
            args.out,
            key="idle-timeout",
            title="Idle timeout via timerfd",
            summary=(
                "After one exchange the client goes quiet. A deadline held in a min-heap and armed on a "
                "single timerfd expires, and the proxy closes the connection in a distinct timed-out state "
                "without ever blocking the event loop."
            ),
            server_args=["--mode", "echo"],
            proxy_args=["--idle-timeout-ms", "600"],
            client_args=[],
            raw_client=idle_workload,
        )
    )

    index = [
        {"key": item["key"], "title": item["title"], "summary": item["summary"]} for item in manifest
    ]
    index_path = os.path.join(args.out, "index.json")
    with open(index_path, "w", encoding="utf-8") as handle:
        json.dump({"scenarios": index}, handle, indent=2)
        handle.write("\n")
    print(f"wrote {index_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
