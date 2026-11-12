<p align="center">
  <img src="logo.png" width="400">
</p>

# nimbus

Resumable, parallel, compressed file synchronization over SSH. It covers the
common `rsync -a` use cases and is built for big transfers over slow or
unreliable links.

```
nimbus -j 8 ./photos/ backup@nas:/srv/photos
nimbus --delete me@server:/var/www/site ./mirror
nimbus -n --delete src/ host:dst        # dry run: show what would change
```

## Features

- **rsync semantics**: `[user@]host:path` locations, `src` vs `src/`
  (directory itself vs its contents), only new/changed files are sent
  (size + mtime, or sha256 with `-c`), `--exclude`, optional `--delete`,
  `--dry-run`. Common rsync flags (`-avzhP`, …) are accepted.
- **Parallel and pipelined**: changes are grouped into independent batches
  (small files are tarred together, large files are split into chunks). Packing,
  transfer and extraction run concurrently over several SSH connections, each
  with its own TCP stream and cipher.
- **Compression**: zstd (multi-threaded) when both ends have it, otherwise
  pigz/gzip, or none. Use `--compress none` for already-compressed data.
- **Verified**: every batch is sha256-checked on the destination before it is
  extracted; nothing is extracted or cleaned up after a failed check.
- **Resilient**: connection failures, stalls (`--timeout`) and checksum
  mismatches are retried with exponential backoff and full jitter; dead
  connections are detected and re-established without prompting again. Other
  errors (permission denied, disk full, …) abort the run immediately.
- **Resumable**: interrupt it (Ctrl-C, network loss, reboot) and run the same
  command again; finished batches and staged data are reused. Packing on a
  remote source runs detached, so it survives a dropped connection.
- **Nothing to install remotely**: the remote side only needs `sh`, `tar`,
  `find`/`stat` and a sha256 tool (see [Compatibility](#compatibility)).
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

`DST` is a directory, created if missing. As in rsync, when `SRC` is a single
file and `DST` has no trailing slash and is not an existing directory, `DST` is
the new file name (`nimbus disk.img host:/backup/disk-2024.img`). Remote to
remote is not supported: run nimbus on one of the hosts.

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
| `--exclude PATTERN`, `--exclude-from FILE` | | skip matching entries (repeatable, see below); excluded destination entries are never deleted |
| `--retries N` | `10` | attempts per operation |
| `--backoff DUR`, `--backoff-max DUR` | `1s`, `1m` | exponential backoff with full jitter |
| `--timeout DUR` | `60s` | retry a transfer that made no progress for this long |
| `-e, --ssh-cmd CMD` | | use the system ssh (e.g. `"ssh -J bastion"`) instead of the built-in client |
| `-F, --ssh-config FILE` | `~/.ssh/config` | ssh config for both clients |
| `--port N`, `--identity FILE` | | SSH port / key |
| `--staging DIR` | `/var/tmp` | staging directory on both endpoints |
| `--keep-staging`, `--fresh` | | keep staged data after success / ignore data from interrupted runs |
| `--plain`, `--no-color`, `-q`, `-v` | | output control (`NO_COLOR` is honoured) |

Exclude patterns follow rsync: `name` matches at any depth, `dir/` matches
directories only, `/top` is anchored at the transfer root, `a/b` matches the
end of a path, `*`/`?` do not cross `/`, `**` does.

Exit codes: `0` ok, `1` transfer failed (rerun to resume), `2` usage error,
`23` some entries could not be updated, `130` interrupted.

### SSH

The built-in client reads `~/.ssh/config` (`HostName`, `User`, `Port`,
`IdentityFile`, `UserKnownHostsFile`, `HostKeyAlias`; `Match` blocks are
ignored), uses `ssh-agent`, verifies `known_hosts` strictly and asks before
trusting an unknown host key. Hosts configured with `ProxyJump` or
`ProxyCommand` automatically use the system ssh; for `Match` blocks or other
advanced options pass `--ssh-cmd ssh`. The system ssh is driven with one
ControlMaster per connection; if key authentication is not available, nimbus
uses a single connection so you are prompted only once (override with
`--conns`). `NIMBUS_SSH_CONFIG` is read when `-F` is not given.

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

## Performance

On a single fast link with no per-connection limit, nimbus is about as fast as
rsync (it stages data on disk, so it is slightly slower on loopback). It wins
when each TCP connection is limited: high latency, per-flow shaping, VPNs,
lossy links. Measured with a proxy limiting every connection to 20 MB/s
(510 MB: one 200 MB file and 2,000 small files):

| | time |
|---|---|
| `rsync -a` | 26.6 s |
| `nimbus -j1` | 26.6 s |
| `nimbus -j4` | 9.5 s |
| `nimbus -j8` | 3.9 s |

If the link's total bandwidth is the bottleneck, more jobs do not help.

## Compatibility

| | status |
|---|---|
| GNU/Linux (GNU tar, find, coreutils) | tested |
| bsdtar instead of GNU tar | tested |
| BusyBox (no GNU `find -printf` / `tar --null`) | code path tested with GNU tools in compatibility mode, not on real BusyBox |
| macOS / FreeBSD (`stat -f`, bsdtar) | implemented, not yet tested on real systems |
| Built-in SSH: keys, ssh-agent, known_hosts (strict, prompt, key-type negotiation) | tested |
| Built-in SSH: password, key passphrase, keyboard-interactive | implemented, not yet tested |

## Testing

```
go test ./...          # unit tests
test/e2e.sh            # end-to-end: starts a throwaway sshd on 127.0.0.1
test/e2e.sh -b         # ... plus the rsync benchmark above
test/e2e.sh resume     # only some groups (local push pull sshcmd delete rename
                       # compat checksum errors resume network bench)
```

The end-to-end suite builds nimbus, generates test trees (awkward names, long
paths, symlinks, permissions), and covers interrupt/resume, killed and frozen
connections, `--delete`/`--exclude`, bsdtar interoperability and host key
checks. It needs `sshd`, about 4 GB in `$NIMBUS_E2E_DIR` (default `/var/tmp`),
and removes everything when done.

## Limitations

- Ownership is preserved only when extracting as root; hard links, ACLs,
  xattrs and special files (devices, FIFOs, sockets) are not transferred.
- On BusyBox/BSD remotes (no GNU `find`/`tar --null`), file names containing
  newlines are rejected.
- Timestamps are compared and set with one-second precision.
