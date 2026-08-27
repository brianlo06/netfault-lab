# Contributing

The project is currently milestone-driven. Open an issue before a large change and keep each pull request focused on one invariant or behavior.

Requirements:

- Preserve loopback-safe defaults.
- Add tests for lifecycle, buffer, timer, or fault-policy changes.
- Run the ASan+UBSan preset and report actual output.
- Never commit payload captures, credentials, raw private traffic, or benchmark claims without reproducible data.
- Document new configuration keys, resource limits, and failure semantics.
