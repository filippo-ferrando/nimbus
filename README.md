<p align="center">
  <img src="logo.png" width="400">
</p>

# nimbus

Resumable, parallel, compressed file synchronization over SSH: a drop-in
for the common `rsync -a` use cases, built for big transfers over slow or
unreliable links.

```
nimbus -j 8 ./photos/ backup@nas:/srv/photos
nimbus --delete me@server:/var/www/site ./mirror
nimbus -n --delete src/ host:dst        # dry run: show what would change
```

## Features

- **rsync semantics**: `[user@]host:path` locations, `src` vs `src/`
  (directory itself vs its contents), only new/changed files are sent
  (size + mtime, or sha256 with `-c`), optional `--delete`, `--dry-run`.
  Common rsync flags (`-avzhP`, …) are accepted.
- **Parallel and pipelined**: changes are grouped into independent batches
  (small files are tarred together, large files are split into chunks). Packing,
  transfer and extraction run concurrently over several SSH connections, each
  with its own TCP stream and cipher.
- **Compression**: zstd (multi-threaded) when both ends have it, otherwise
  pigz/gzip, or none. Use `--compress none` for already-compressed data.
- **Verified**: every batch is sha256-checked on the destination before it is
  extracted; nothing is extracted or cleaned up after a failed check.
- **Resilient**: every operation retries with exponential backoff and full
  jitter; dropped connections are re-established without prompting again;
  stalled transfers are detected (`--timeout`) and retried.
- **Resumable**: interrupt it (Ctrl-C, network loss, reboot) and run the same
  command again; finished batches and staged data are reused. Packing on a
  remote source runs detached, so it survives a dropped connection.
- **Nothing to install remotely**: the remote side only needs `sh`, `tar`,
  `find`/`stat` and a sha256 tool. GNU/Linux, BusyBox and macOS/BSD userlands
  are supported.
- **One password prompt**: the built-in SSH client authenticates once (agent,
  keys, passphrase, password or keyboard-interactive) and opens all
  connections with it. `known_hosts` is verified strictly.
- **Live progress** on terminals (overall bar, throughput, wire rate,
  compression ratio, ETA, per-job lines), plain log lines when piped or with
  `--plain`.

## Install

Download a binary from the releases page (linux, macOS, FreeBSD), or build:

```
go build -o nimbus .
```

## Usage

```
nimbus [options] SRC DST
```

`DST` is always a directory and is created if missing. Remote to remote is not
supported: run nimbus on one of the hosts.

| Option | Default | |
|---|---|---|
| `-b, --block SIZE` | `64M` | batch / chunk size (multiple of 1M). Larger means less overhead; smaller means finer resume granularity |
| `-j, --jobs N` | `4` | parallel pack / transfer / extract jobs |
| `--conns N` | jobs | SSH connections |
| `--compress MODE` | `auto` | `auto`, `zstd`, `gzip`, `none` (`auto` = none for local→local) |
| `--level N` | zstd 3, gzip 6 | compression level |
| `-c, --checksum` | | compare contents by sha256 instead of size+mtime |
| `--delete` | | delete destination entries missing from the source (only inside the synced tree; refused if the source is empty) |
| `-n, --dry-run` | | print the plan, change nothing |
| `--retries N` | `10` | attempts per operation |
| `--backoff DUR`, `--backoff-max DUR` | `1s`, `1m` | exponential backoff with full jitter |
| `--timeout DUR` | `60s` | retry a transfer that made no progress for this long |
| `-e, --ssh-cmd CMD` | | use the system ssh (e.g. `"ssh -J bastion"`) instead of the built-in client |
| `--port N`, `--identity FILE` | | SSH port / key |
| `--staging DIR` | `/var/tmp` | staging directory on both endpoints |
| `--keep-staging`, `--fresh` | | keep staged data after success / ignore data from interrupted runs |
| `--plain`, `--no-color`, `-q`, `-v` | | output control (`NO_COLOR` is honoured) |

Exit codes: `0` ok, `1` transfer failed (rerun to resume), `2` usage error,
`23` some entries could not be updated, `130` interrupted.

### SSH

The built-in client reads `~/.ssh/config` (`HostName`, `User`, `Port`,
`IdentityFile`, `UserKnownHostsFile`, `HostKeyAlias`), uses `ssh-agent`, and
asks before trusting an unknown host key. Hosts that need `ProxyJump` or
`ProxyCommand` must use `--ssh-cmd ssh`, which drives your ssh binary with one
ControlMaster per connection. With `--ssh-cmd`, if key authentication is not
available, nimbus falls back to a single connection so you are prompted only
once (override with `--conns`). `NIMBUS_SSH_CONFIG` points the built-in client
at a different ssh config file.

## How it works

1. Both trees are listed (natively for local paths; with a small embedded
   shell agent uploaded to the staging directory for remote ones) and compared.
2. Files to send are grouped into batches: small files into tar archives of
   about `--block` bytes, large files into `--block`-sized byte ranges. Each
   batch has a content key, so it is reused across runs.
3. For each batch: the source packs and compresses it in staging and records
   its sha256. The data is streamed to the destination, which verifies the
   checksum, extracts it (chunks are written in place into
   `.name.nimbus-part`, then renamed atomically) and drops the staged copy.
4. After every batch succeeded: attribute fixes, `--delete`, directory
   mtimes/permissions, staging cleanup.

Staging needs at most about `2 × jobs × block` of free space on each side.

## Limitations

- Ownership is preserved only when extracting as root; hard links, ACLs,
  xattrs and special files (devices, FIFOs, sockets) are not transferred.
- On BusyBox/BSD remotes (no GNU `find`/`tar --null`), file names containing
  newlines are rejected.
- Timestamps are compared and set with one-second precision.
