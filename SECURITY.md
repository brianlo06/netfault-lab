# Security and Safety Policy

NetFault Lab is for isolated, authorized testing only. It is not a transparent interception tool, public relay, firewall manager, or traffic-hiding utility.

## Safe defaults

- Proxy listener and upstream default to loopback.
- Separate explicit flags are required for non-loopback listener and upstream addresses, and warnings are printed.
- The bundled server and client are loopback-only in Milestone 1.
- Payload contents are neither logged nor captured.
- Connection count and per-direction memory are bounded.
- No root privileges, raw sockets, firewall modification, or automatic `tc` commands are used.

## Reporting a vulnerability

Do not include payloads, credentials, private packet captures, or target details in a public issue. Until a dedicated private reporting channel is configured, open a minimal GitHub issue requesting a private contact path.

## Out of scope

Do not use this project to intercept traffic without authorization, bypass access controls, expose an open proxy, conceal malicious traffic, test public services, or degrade third-party systems.
