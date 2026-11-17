# Changelog

All notable changes to nimbus are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- GPL-3.0-only license.
- Documentation in `docs/`: installation, usage, SSH, architecture,
  performance and compatibility guides.
- `CONTRIBUTING.md` and this changelog.
- CI workflow (`.forgejo/workflows/ci.yml`): gofmt, `go vet`, `go mod tidy`
  check, unit tests with the race detector, cross-compilation for every
  release target, and the end-to-end suite.
- Release notes now include the changelog section for the tag, install
  instructions and the SHA-256 checksum of every binary.

### Fixed

- The single-connection warning for the system ssh now suggests `--conns N`
  (it said `-c N`, which is `--checksum`).

### Changed

- Re-running the release workflow for an existing tag updates the release in
  place (notes and checksums rewritten, all assets replaced) instead of
  failing.
- The README was rewritten around a new banner, with the details moved to
  `docs/`.

## [0.2.0] - 2026-11-12

nimbus is now a Go program. It replaces the earlier bash script.

### Added

- Portable POSIX shell agent that does all endpoint work on GNU, BusyBox and
  macOS/BSD userlands, so the remote needs nothing installed.
- rsync-compatible CLI: `[user@]host:path` locations, trailing-slash
  semantics, skipping unchanged files, `-c/--checksum`, `--delete`,
  `-n/--dry-run`. Common rsync flags (`-avzhP`, …) are accepted as no-ops.
- `--exclude` / `--exclude-from` with rsync-style patterns. Excluded
  destination entries are protected from `--delete`.
- rsync's single-file rule: a single-file `SRC` is written as `DST` when `DST`
  is not a directory.
- Independent batches (small files tarred together, large files split by
  offset) in a pipeline of pack, transfer, verify and extract.
- Built-in SSH client with N parallel connections and a single authentication
  prompt, or the system ssh via `-e/--ssh-cmd`. Hosts using `ProxyJump` or
  `ProxyCommand` switch to the system ssh automatically.
- `-F/--ssh-config` for both SSH clients.
- sha256 verification of every batch before extraction.
- Retries with exponential backoff and full jitter, stall detection
  (`--timeout`) and automatic reconnects.
- Resume per batch. Remote packers run detached and survive dropped
  connections.
- Live terminal UI and a plain, log-friendly mode.
- Exit code `23` when some entries could not be updated, as in rsync.
- Release binaries for linux, macOS and FreeBSD built by Forgejo Actions.
- Unit tests and an end-to-end suite with a throwaway sshd and a
  rate-limiting proxy for benchmarks.

### Changed

- Only connection errors, stalls and checksum mismatches are retried. Any
  other error aborts the run straight away with the underlying message.
- Connection failures exit with `1`. `2` is reserved for usage errors.

### Fixed

- Several resume edge cases (stale part markers, single-file transfers,
  staged data that vanished, dead packers) that could re-send data or leave
  holes in files.
- `--delete` only removes entries inside the synced tree and keeps
  directories that contain excluded entries.
- Bounded SSH session open/close on frozen connections.
- macOS tar no longer adds AppleDouble `._*` entries to archives.

### Removed

- The bash implementation (`nimbus.sh`) and its GitHub workflow.

[Unreleased]: https://git.fferrando.cc/rdfilippo/nimbus/compare/v0.2.0...HEAD
[0.2.0]: https://git.fferrando.cc/rdfilippo/nimbus/releases/tag/v0.2.0
