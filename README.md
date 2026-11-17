<p align="center">
  <img src="docs/assets/banner.png" alt="nimbus" width="720">
</p>

<p align="center">
  <strong>Resumable, parallel, compressed file sync over SSH.</strong><br>
  The <code>rsync -a</code> you reach for when the link is slow, flaky or throttled.
</p>

<p align="center">
  <a href="https://git.fferrando.cc/rdfilippo/nimbus/actions?workflow=ci.yml"><img alt="CI" src="https://git.fferrando.cc/rdfilippo/nimbus/actions/workflows/ci.yml/badge.svg?label=ci"></a>
  <a href="https://git.fferrando.cc/rdfilippo/nimbus/releases"><img alt="Release" src="https://img.shields.io/gitea/v/release/rdfilippo/nimbus?gitea_url=https%3A%2F%2Fgit.fferrando.cc&label=release&color=2ea043"></a>
  <a href="go.mod"><img alt="Go" src="https://img.shields.io/badge/go-1.27-00ADD8?logo=go&logoColor=white"></a>
  <img alt="Platforms" src="https://img.shields.io/badge/platforms-linux%20%7C%20macOS%20%7C%20FreeBSD-1d2840">
  <a href="LICENSE"><img alt="License: GPL-3.0-only" src="https://img.shields.io/badge/license-GPL--3.0--only-blue"></a>
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> ·
  <a href="docs/installation.md">Install</a> ·
  <a href="docs/usage.md">Usage</a> ·
  <a href="docs/architecture.md">How it works</a> ·
  <a href="docs/README.md">Documentation</a> ·
  <a href="CONTRIBUTING.md">Contributing</a>
</p>

---

nimbus copies directory trees between your machine and a remote host over SSH.
It speaks rsync's language (`[user@]host:path`, trailing-slash rules,
`--exclude`, `--delete`, `-n`) but moves data differently: changes are split
into independent, checksummed batches that are packed, compressed, sent and
extracted in parallel over several SSH connections. If anything breaks, you run
the same command again and it picks up where it stopped.

```console
$ nimbus -j 8 ./photos/ backup@nas:/srv/photos
$ nimbus --delete me@server:/var/www/site ./mirror
$ nimbus -n --delete src/ host:dst        # dry run: show what would change
```

## Why nimbus?

|                          | rsync                       | nimbus                                                    |
| ------------------------ | --------------------------- | --------------------------------------------------------- |
| TCP streams per transfer | 1                           | `--jobs` (default 4), each with its own cipher            |
| Interrupted transfer     | partial file discarded unless `--partial` | resumes per batch; staged data is reused                  |
| Dropped connection       | run fails                   | reconnects, retries with backoff, no new password prompt  |
| Integrity                | per-file checksum after writing | sha256 per batch, verified before anything is extracted   |
| Remote requirements      | `rsync` installed           | only `sh`, `tar`, `find`/`stat`, a sha256 tool            |

With every TCP connection capped at 20 MB/s, `nimbus -j8` finishes a 510 MB
tree in **3.9 s** against rsync's **26.6 s**
([benchmark](docs/performance.md)). On a fast, unthrottled link it is about as
fast as rsync.

## Features

- **rsync semantics.** `src` vs `src/`, size+mtime or sha256 (`-c`)
  comparison, `--exclude`, `--delete`, `--dry-run`. Habitual flags like
  `-avzhP` are accepted.
- **Parallel and pipelined.** Small files are tarred together, large files are
  split into chunks, and packing, transfer and extraction run concurrently.
- **Compression.** Multi-threaded zstd when both ends have it, otherwise
  pigz/gzip, or none.
- **Verified.** Every batch is sha256-checked on the destination before it is
  extracted.
- **Resilient.** Connection failures, stalls and checksum mismatches are
  retried with exponential backoff and full jitter. Permanent errors such as
  permission denied or a full disk stop the run straight away.
- **Resumable.** Ctrl-C, network loss or a reboot costs at most the batches
  that were in flight. Remote packers run detached and survive a dropped
  connection.
- **Nothing to install remotely.** A small embedded shell agent does the
  remote work with standard tools (GNU, BusyBox, macOS/BSD).
- **One password prompt.** The built-in SSH client authenticates once and
  reuses it for every connection. `known_hosts` is checked strictly.
  `ProxyJump`/`ProxyCommand` hosts switch to the system `ssh` automatically.
- **Live progress.** Overall bar, throughput, wire rate, compression ratio, ETA
  and per-job lines on a terminal. Plain log lines when piped.

## Quick start

Download a binary for your platform from the
[releases page](https://git.fferrando.cc/rdfilippo/nimbus/releases) and put it
on your `PATH`:

```sh
curl -fLo nimbus https://git.fferrando.cc/rdfilippo/nimbus/releases/download/v0.2.0/nimbus-v0.2.0-linux-amd64
chmod +x nimbus && sudo mv nimbus /usr/local/bin/
```

Or build from source (Go 1.27+):

```sh
go install git.fferrando.cc/rdfilippo/nimbus@latest
```

Then sync:

```sh
nimbus ./data/ user@host:/backup/data     # push the contents of ./data
nimbus user@host:/var/log ./logs           # pull the directory itself: ./logs/log/...
```

Interrupted? Run the same command again. See
[installation](docs/installation.md) for checksums and other platforms.

## Documentation

| Guide                                          | What's inside                                                         |
| ---------------------------------------------- | --------------------------------------------------------------------- |
| [Installation](docs/installation.md)           | Release binaries, checksums, building from source                     |
| [Usage](docs/usage.md)                         | Path rules, every option, exclude patterns, exit codes, examples      |
| [SSH](docs/ssh.md)                             | Built-in client vs system ssh, config, authentication, host keys      |
| [How it works](docs/architecture.md)           | Batches, the pipeline, verification, retries, resume, staging         |
| [Performance](docs/performance.md)             | Benchmark, when nimbus wins, tuning `--jobs`, `--block`, compression  |
| [Compatibility](docs/compatibility.md)         | Remote requirements, tested platforms, known limitations              |

## Contributing

Bug reports and pull requests are welcome. Read
[CONTRIBUTING.md](CONTRIBUTING.md) for the development setup and the test
suites. Notable changes are listed in [CHANGELOG.md](CHANGELOG.md).

## License

nimbus is free software: you can redistribute it and/or modify it under the
terms of the [GNU General Public License, version 3](LICENSE) only
(`SPDX-License-Identifier: GPL-3.0-only`).
