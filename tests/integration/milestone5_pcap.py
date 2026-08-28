#!/usr/bin/env python3
"""Milestone 5 packet-capture comparison: capture a reset-injection run on
loopback and corroborate the proxy's logged story against the wire — two TCP
handshakes (client-proxy and proxy-upstream), forwarded payload, and a real
RST after the logged fault. Skips itself when packet capture is unavailable
(unprivileged environments)."""

import argparse
import json
import os
import shutil
import socket
import struct
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

TCP_FIN = 0x01
TCP_SYN = 0x02
TCP_RST = 0x04


def parse_pcap(path: str) -> list[dict]:
    """Minimal classic-pcap parser for Ethernet-framed IPv4 TCP packets.

    Returns one record per TCP packet: src/dst ports, flags, payload length.
    Raises ValueError for formats this parser does not understand.
    """
    with open(path, "rb") as handle:
        data = handle.read()
    if len(data) < 24:
        raise ValueError("pcap too short for a global header")
    magic = data[:4]
    if magic == b"\xd4\xc3\xb2\xa1" or magic == b"\x4d\x3c\xb2\xa1":
        endian = "<"
    elif magic == b"\xa1\xb2\xc3\xd4" or magic == b"\xa1\xb2\x3c\x4d":
        endian = ">"
    else:
        raise ValueError(f"unknown pcap magic: {magic!r}")
    link_type = struct.unpack(endian + "I", data[20:24])[0]
    if link_type != 1:  # LINKTYPE_ETHERNET, what tcpdump uses on Linux lo
        raise ValueError(f"unsupported link type: {link_type}")

    packets = []
    offset = 24
    while offset + 16 <= len(data):
        incl_len = struct.unpack(endian + "I", data[offset + 8 : offset + 12])[0]
        frame = data[offset + 16 : offset + 16 + incl_len]
        offset += 16 + incl_len
        if len(frame) < 14 + 20 + 20:
            continue
        ethertype = struct.unpack("!H", frame[12:14])[0]
        if ethertype != 0x0800:  # IPv4
            continue
        ip = frame[14:]
        ip_header_len = (ip[0] & 0x0F) * 4
        total_len = struct.unpack("!H", ip[2:4])[0]
        if ip[9] != 6 or len(ip) < ip_header_len + 20:  # TCP
            continue
        tcp = ip[ip_header_len:]
        src_port, dst_port = struct.unpack("!HH", tcp[:4])
        tcp_header_len = (tcp[12] >> 4) * 4
        flags = tcp[13]
        payload_len = max(0, total_len - ip_header_len - tcp_header_len)
        packets.append(
            {"src": src_port, "dst": dst_port, "flags": flags, "payload_len": payload_len}
        )
    return packets


def start_capture(pcap_path: str, ports: list[int]):
    """Start tcpdump on loopback for the given ports; returns (proc, cmd) or None."""
    if shutil.which("tcpdump") is None:
        return None
    port_filter = " or ".join(f"port {port}" for port in ports)
    base_cmd = ["tcpdump", "-i", "lo", "-U", "-w", pcap_path, f"tcp and ({port_filter})"]
    for cmd in (base_cmd, ["sudo", "-n"] + base_cmd):
        probe = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        time.sleep(0.6)  # tcpdump either sets up the capture or exits with a permission error
        if probe.poll() is None:
            return probe
        probe.wait()
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--server", required=True)
    parser.add_argument("--client", required=True)
    args = parser.parse_args()

    environment = default_environment()
    reset_after = 50_000

    with tempfile.TemporaryDirectory() as workdir, tempfile.TemporaryFile(
        mode="w+"
    ) as server_log, tempfile.TemporaryFile(mode="w+") as proxy_log:
        pcap_path = os.path.join(workdir, "reset.pcap")
        server_port = free_port()
        proxy_port = free_port()

        capture = start_capture(pcap_path, [server_port, proxy_port])
        if capture is None:
            print(
                json.dumps(
                    {
                        "milestone": 5,
                        "scenario": "pcap_comparison",
                        "status": "skipped_environment",
                        "reason": "packet capture unavailable (no tcpdump or no privilege)",
                    }
                )
            )
            return 0

        server = subprocess.Popen(
            [args.server, "--listen", f"127.0.0.1:{server_port}", "--mode", "echo"],
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
                    "--fault-reset-after-bytes",
                    str(reset_after),
                ],
                stdout=proxy_log,
                stderr=proxy_log,
                text=True,
                env=environment,
            )
            wait_for_port(proxy_port, proxy)

            with socket.create_connection(("127.0.0.1", proxy_port), timeout=10) as sock:
                sock.settimeout(10)
                try:
                    for _ in range(64):
                        sock.sendall(b"\x44" * 16_384)
                        sock.recv(65_536)
                except (ConnectionResetError, BrokenPipeError):
                    pass

            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if events_named(read_events(proxy_log), "fault_injected"):
                    break
                time.sleep(0.05)
        finally:
            if proxy is not None:
                stop_process(proxy)
            stop_process(server)
            time.sleep(0.5)  # let tcpdump flush its final packets
            capture.terminate()
            capture.wait(timeout=10)

        events = read_events(proxy_log)
        injected = [
            event for event in events_named(events, "fault_injected") if "fault=reset" in str(event["detail"])
        ]
        reset_closes = [
            event
            for event in events_named(events, "connection_closed")
            if event["state"] == "reset" and "fault_reset" in str(event.get("detail", ""))
        ]
        if len(injected) != 1 or len(reset_closes) != 1:
            raise AssertionError(f"log story incomplete: injected={injected} closes={reset_closes}")

        packets = parse_pcap(pcap_path)
        if not packets:
            raise AssertionError("capture produced no parseable TCP packets")

        client_hop_syns = [
            p for p in packets if p["flags"] & TCP_SYN and not p["flags"] & TCP_RST and p["dst"] == proxy_port
        ]
        upstream_hop_syns = [
            p for p in packets if p["flags"] & TCP_SYN and not p["flags"] & TCP_RST and p["dst"] == server_port
        ]
        if not client_hop_syns or not upstream_hop_syns:
            raise AssertionError(
                f"expected handshakes on both hops: client={len(client_hop_syns)} "
                f"upstream={len(upstream_hop_syns)}"
            )

        # The fault budget counts proxy-written bytes in both directions:
        # toward the upstream server plus toward the client. The wire can show
        # slightly less than the log because bytes handed to the kernel but
        # still queued in a socket buffer are discarded by the RST; allow one
        # client-chunk of slack per direction.
        close_metrics = parse_detail(str(reset_closes[0]["detail"]))
        logged_written = close_metrics["client_written"] + close_metrics["upstream_written"]
        toward_upstream = sum(p["payload_len"] for p in packets if p["dst"] == server_port)
        toward_client = sum(p["payload_len"] for p in packets if p["src"] == proxy_port)
        forwarded_payload = toward_upstream + toward_client
        discard_allowance = 2 * 16_384
        if logged_written < reset_after:
            raise AssertionError(f"log's written total is below the fault budget: {close_metrics}")
        if forwarded_payload < logged_written - discard_allowance:
            raise AssertionError(
                f"wire shows {forwarded_payload} forwarded payload bytes "
                f"({toward_upstream} toward upstream, {toward_client} toward client), more than "
                f"{discard_allowance} below the log's {logged_written}-byte written total"
            )

        resets = [p for p in packets if p["flags"] & TCP_RST]
        if not resets:
            raise AssertionError("log claims a fault reset but no RST appears on the wire")

    print(
        json.dumps(
            {
                "milestone": 5,
                "scenario": "pcap_comparison",
                "packets": len(packets),
                "forwarded_payload_bytes": forwarded_payload,
                "rst_packets": len(resets),
                "status": "passed",
            }
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"milestone5 pcap failure: {error}", file=sys.stderr)
        raise
