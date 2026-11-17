<p align="center">
  <img src="assets/banner.png" alt="nimbus" width="480">
</p>

# nimbus documentation

nimbus is a resumable, parallel, compressed file sync tool that works over SSH.
These pages go into more detail than the [project README](../README.md).

## Getting started

- **[Installation](installation.md)**: release binaries, verifying checksums,
  building from source.
- **[Usage](usage.md)**: how source and destination paths behave, the full
  option reference, exclude patterns, exit codes and common recipes.

## Guides

- **[SSH](ssh.md)**: the built-in client, when nimbus switches to the system
  `ssh`, authentication, `known_hosts` and `~/.ssh/config` support.
- **[Performance](performance.md)**: the benchmark, when parallel streams help,
  and how to tune `--jobs`, `--block` and compression.

## Reference

- **[How it works](architecture.md)**: listing and diffing, batches, the
  pack → transfer → verify → extract pipeline, retries, resume and staging.
- **[Compatibility](compatibility.md)**: what the remote host needs, which
  platforms are tested, and known limitations.

## Project

- [Contributing](../CONTRIBUTING.md)
- [Changelog](../CHANGELOG.md)
- [License (GPL-3.0-only)](../LICENSE)
