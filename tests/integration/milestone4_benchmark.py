#!/usr/bin/env python3
"""Milestone 4 benchmark client: request-response latency distributions must be
recorded with full configuration, and must move by the configured fault
latency between a baseline and a fault run."""

import argparse
import json
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from harness import (  # noqa: E402
    default_environment,
    free_port,
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


def run_benchmark(args, port: int, seed: int = 4242) -> dict:
    client = subprocess.run(
        [
            args.client,
            "--connect",
            f"127.0.0.1:{port}",
            "--mode",
            "request-response",
            "--connections",
            "2",
            "--requests",
            "20",
            "--warmup",
            "3",
            "--request-bytes",
            "1024",
            "--seed",
            str(seed),
        ],
        capture_output=True,
        text=True,
        timeout=120,
        env=default_environment(),
        check=False,
    )
    if client.returncode != 0:
        raise RuntimeError(f"benchmark client failed: stdout={client.stdout!r} stderr={client.stderr!r}")
    return json.loads(client.stdout)


def check_summary_shape(summary: dict) -> None:
    if summary["mode"] != "request-response" or summary["successful"] != 2:
        raise AssertionError(f"benchmark run failed: {summary}")
    for field in ("requests_per_connection", "warmup_per_connection", "request_bytes", "seed", "environment"):
        if field not in summary:
            raise AssertionError(f"summary missing reproducibility field {field}: {summary}")
    latency = summary["latency_us"]
    if latency["count"] != 40:  # 2 connections x 20 recorded requests
        raise AssertionError(f"expected 40 pooled samples: {latency}")
    if not latency["min"] <= latency["p50"] <= latency["p90"] <= latency["p99"] <= latency["max"]:
        raise AssertionError(f"percentiles are not monotone: {latency}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--client", required=True)
    args = parser.parse_args()

    with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
        server, server_port = start_echo_server(args, server_log)
        baseline_proxy = None
        fault_proxy = None
        try:
            baseline_port = free_port()
            baseline_proxy = subprocess.Popen(
                [
                    args.proxy,
                    "--listen",
                    f"127.0.0.1:{baseline_port}",
                    "--upstream",
                    f"127.0.0.1:{server_port}",
                ],
                stdout=proxy_log,
                stderr=proxy_log,
                text=True,
                env=default_environment(),
            )
            wait_for_port(baseline_port, baseline_proxy)
            baseline = run_benchmark(args, baseline_port)
            check_summary_shape(baseline)

            fault_port = free_port()
            fault_proxy = subprocess.Popen(
                [
                    args.proxy,
                    "--listen",
                    f"127.0.0.1:{fault_port}",
                    "--upstream",
                    f"127.0.0.1:{server_port}",
                    "--fault-latency-ms",
                    "50",
                    "--fault-seed",
                    "9",
                ],
                stdout=proxy_log,
                stderr=proxy_log,
                text=True,
                env=default_environment(),
            )
            wait_for_port(fault_port, fault_proxy)
            faulted = run_benchmark(args, fault_port)
            check_summary_shape(faulted)
        finally:
            if baseline_proxy is not None:
                stop_process(baseline_proxy)
            if fault_proxy is not None:
                stop_process(fault_proxy)
            stop_process(server)

    baseline_p50 = baseline["latency_us"]["p50"]
    faulted_p50 = faulted["latency_us"]["p50"]
    # 50 ms is injected in each direction, so a request round trip gains ~100 ms.
    if faulted_p50 - baseline_p50 < 90_000:
        raise AssertionError(
            f"fault latency not visible in the distribution: baseline p50={baseline_p50}us "
            f"faulted p50={faulted_p50}us"
        )
    if baseline["seed"] != faulted["seed"] or baseline["request_bytes"] != faulted["request_bytes"]:
        raise AssertionError("run configurations diverged; results are not comparable")

    print(
        json.dumps(
            {
                "milestone": 4,
                "scenario": "benchmark_latency_shift",
                "baseline_p50_us": baseline_p50,
                "faulted_p50_us": faulted_p50,
                "status": "passed",
            }
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"milestone4 benchmark failure: {error}", file=sys.stderr)
        raise
