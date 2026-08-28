#!/usr/bin/env python3
"""Milestone 5 payload sweep: byte-exact forwarding across payload sizes
around socket and queue boundaries plus seeded random sizes. The seed is
always printed so any failing combination can be replayed exactly."""

import argparse
import json
import os
import random
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

# Off-by-one bands around the client chunk (16 KiB), server chunk (16 KiB),
# proxy queue/watermark defaults (32/64 KiB), and kernel-ish page sizes.
BOUNDARY_SIZES = [
    1,
    2,
    4_095,
    4_096,
    4_097,
    16_383,
    16_384,
    16_385,
    32_767,
    32_768,
    32_769,
    65_535,
    65_536,
    65_537,
    131_071,
    131_072,
    131_073,
]
RANDOM_SIZES = 12
MAX_RANDOM_SIZE = 512 * 1024


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--client", required=True)
    args = parser.parse_args()

    seed = int(os.environ.get("NETFAULT_SWEEP_SEED", "20260828"))
    rng = random.Random(seed)
    sizes = BOUNDARY_SIZES + [rng.randint(1, MAX_RANDOM_SIZE) for _ in range(RANDOM_SIZES)]
    environment = default_environment()

    with tempfile.TemporaryFile(mode="w+") as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
        server_port = free_port()
        proxy_port = free_port()
        server = subprocess.Popen(
            [args.server, "--listen", f"127.0.0.1:{server_port}", "--mode", "echo"],
            stdout=server_log,
            stderr=server_log,
            text=True,
            env=environment,
        )
        proxy = None
        completed = 0
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
                    "16",
                ],
                stdout=proxy_log,
                stderr=proxy_log,
                text=True,
                env=environment,
            )
            wait_for_port(proxy_port, proxy)

            for index, size in enumerate(sizes):
                client_seed = seed + index
                client = subprocess.run(
                    [
                        args.client,
                        "--connect",
                        f"127.0.0.1:{proxy_port}",
                        "--connections",
                        "2",
                        "--payload-bytes",
                        str(size),
                        "--seed",
                        str(client_seed),
                    ],
                    capture_output=True,
                    text=True,
                    timeout=60,
                    env=environment,
                    check=False,
                )
                summary = json.loads(client.stdout) if client.returncode == 0 else None
                if summary is None or summary["successful"] != 2:
                    raise AssertionError(
                        f"sweep failed: sweep_seed={seed} payload_bytes={size} client_seed={client_seed} "
                        f"stdout={client.stdout!r} stderr={client.stderr!r}"
                    )
                completed += 1
        finally:
            if proxy is not None:
                stop_process(proxy)
            stop_process(server)

    print(
        json.dumps(
            {
                "milestone": 5,
                "scenario": "payload_sweep",
                "sweep_seed": seed,
                "boundary_sizes": len(BOUNDARY_SIZES),
                "random_sizes": RANDOM_SIZES,
                "transfers_completed": completed,
                "status": "passed",
            }
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"milestone5 sweep failure: {error}", file=sys.stderr)
        raise
