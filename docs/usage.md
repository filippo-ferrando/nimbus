# Usage

```
nimbus [options] SRC DST
```

`SRC` and `DST` are local paths or remote locations written as
`[user@]host:path`, as in rsync. One side at most can be remote: for a
remote-to-remote copy, run nimbus on one of the two hosts.

- [Paths](#paths)
- [Options](#options)
- [Exclude patterns](#exclude-patterns)
- [Deleting extraneous files](#deleting-extraneous-files)
- [Output](#output)
- [Exit codes](#exit-codes)
- [Recipes](#recipes)

## Paths

### Remote locations

A colon before the first slash makes a location remote:

| Argument               | Meaning                                        |
| ---------------------- | ---------------------------------------------- |
| `host:/srv/data`       | `/srv/data` on `host`                          |
| `user@host:data`       | `data` relative to the remote home directory   |
| `host:~/data`          | same, written explicitly                       |
| `[2001:db8::1]:/data`  | IPv6 address                                   |
| `./a:b`                | a local file whose name contains a colon       |

`host` can be an alias from `~/.ssh/config` (see [SSH](ssh.md)).

### Trailing slash on the source

The same rule as rsync:

| Command                    | Result                              |
| -------------------------- | ----------------------------------- |
| `nimbus photos  host:/bak` | `/bak/photos/...` (the directory itself) |
| `nimbus photos/ host:/bak` | `/bak/...` (its contents)           |

### Destination

`DST` is a directory and is created if it does not exist. There is one
exception, again from rsync: if `SRC` is a single file, and `DST` has no
trailing slash and is not an existing directory, then `DST` is the new file
name:

```sh
nimbus disk.img host:/backup/disk-2024.img   # writes /backup/disk-2024.img
nimbus disk.img host:/backup/                # writes /backup/disk.img
```

nimbus refuses to run when a local source and a local destination overlap.

### What gets sent

A file is sent when it is missing on the destination or when its size or
modification time is different. With `-c` / `--checksum`, files of the same
size are compared by sha256 instead, which is slower but catches content
changes that kept the mtime.

nimbus always behaves like `rsync -a`: it copies recursively and keeps symlinks,
permissions and modification times. Ownership is kept only when the
destination side runs as root.

## Options

### Transfer

| Option              | Default  | Description |
| ------------------- | -------- | ----------- |
| `-b, --block SIZE`  | `64M`    | Batch and chunk size. Takes `K`/`M`/`G` suffixes and must be a multiple of `1M`. Larger blocks mean less overhead, smaller blocks mean finer resume granularity. |
| `-j, --jobs N`      | `4`      | Parallel pack, transfer and extract jobs (1–256). |
| `--conns N`         | = jobs   | Number of SSH connections. |
| `--compress MODE`   | `auto`   | `auto`, `zstd`, `gzip` or `none`. `auto` picks zstd if both ends have it, then gzip (pigz where installed), then none. It always picks none for local→local. An explicit mode fails if either end lacks the tool. |
| `--level N`         | zstd 3, gzip 6 | Compression level. |

### Selection

| Option                  | Description |
| ----------------------- | ----------- |
| `-c, --checksum`        | Compare contents by sha256 instead of size + mtime. |
| `--exclude PATTERN`     | Skip matching entries. Can be repeated. See [Exclude patterns](#exclude-patterns). |
| `--exclude-from FILE`   | Read patterns from `FILE`, one per line. Can be repeated. |
| `--delete`              | Delete destination entries that are not in the source. See [below](#deleting-extraneous-files). |
| `-n, --dry-run`         | Print the plan and change nothing. |

### Reliability

| Option              | Default | Description |
| ------------------- | ------- | ----------- |
| `--retries N`       | `10`    | Attempts per operation, counting the first one. |
| `--backoff DUR`     | `1s`    | First retry delay. It doubles on every attempt, with full jitter. |
| `--backoff-max DUR` | `1m`    | Longest retry delay. |
| `--timeout DUR`     | `60s`   | Retry a transfer that made no progress for this long. |

Durations use Go syntax: `500ms`, `30s`, `2m`.

### SSH

| Option                  | Description |
| ----------------------- | ----------- |
| `-e, --ssh-cmd CMD`     | Use the system ssh instead of the built-in client, e.g. `"ssh -J bastion"`. |
| `-F, --ssh-config FILE` | ssh config file for both clients. The default is `~/.ssh/config`. `$NIMBUS_SSH_CONFIG` sets it for the built-in client only. |
| `--port N`              | SSH port. |
| `--identity FILE`       | Private key file. |

See [SSH](ssh.md) for details.

### Staging

| Option           | Default    | Description |
| ---------------- | ---------- | ----------- |
| `--staging DIR`  | `/var/tmp` | Staging directory on both ends. Must be an absolute path. |
| `--keep-staging` |            | Keep staged data after a successful run. |
| `--fresh`        |            | Ignore staged data left by an interrupted run. |

Staging needs up to about `2 × jobs × block` of free space on each side (512 MB
with the defaults). See [How it works](architecture.md#staging).

### Output

| Option           | Description |
| ---------------- | ----------- |
| `--plain`        | Plain log lines instead of the live view. |
| `--no-color`     | Disable colors. `NO_COLOR` is honoured too. |
| `-q, --quiet`    | Print errors only. |
| `-v, --verbose`  | Print more detail. |
| `-V, --version`  | Print the version and exit. |
| `--help`         | Print the option summary. |

### Accepted rsync flags

`-a -r -l -t -p -g -o -D -P -h -z` are accepted and ignored, so commands like
`nimbus -avzP src/ host:dst` work unchanged. nimbus always acts like `-a`, and
compression is controlled by `--compress`.

## Exclude patterns

Patterns follow rsync's `--exclude` rules:

| Pattern       | Matches |
| ------------- | ------- |
| `name`        | a file or directory called `name` at any depth |
| `dir/`        | directories only |
| `/top`        | `top` at the root of the transfer only |
| `a/b`         | paths that end in `a/b` |
| `*`, `?`      | any characters except `/` |
| `**`          | any characters including `/` |
| `[...]`       | a character class |

Excluding a directory excludes everything below it. In an `--exclude-from`
file, blank lines and lines starting with `#` or `;` are ignored.

```sh
nimbus --exclude .git/ --exclude '*.tmp' --exclude /build ./project/ host:/srv/project
```

## Deleting extraneous files

`--delete` removes destination entries that are missing from the source.
It is deliberately conservative:

- It only works inside the synced tree. For `nimbus photos host:/bak` that
  means `/bak/photos`, never other entries in `/bak`.
- It runs only after every batch has transferred successfully.
- Excluded entries on the destination are never deleted.
- It is refused when the source is empty, so a wrong path or an unmounted disk
  cannot wipe the destination.

Run it with `-n` first to see what would be removed:

```sh
nimbus -n --delete ./site/ web@host:/var/www/site
```

## Output

On a terminal nimbus draws a live view with the overall progress, throughput,
wire rate, compression ratio, ETA and one line per job. When output is piped or
`--plain` is given, it prints one log line per event, which works well with
cron, CI and `tee`.

## Exit codes

| Code  | Meaning |
| ----- | ------- |
| `0`   | Success. |
| `1`   | The transfer failed. Run the same command again to resume. |
| `2`   | Usage error (bad option or path). |
| `23`  | Some entries could not be updated (rsync's "partial transfer"). |
| `130` | Interrupted (Ctrl-C / SIGTERM). Run again to resume. |

## Recipes

**Back up a home directory to a NAS, ignoring caches:**

```sh
nimbus -j 8 --exclude .cache/ --exclude node_modules/ ~/ backup@nas:/srv/backup/home
```

**Mirror a remote site locally, deleting what was removed upstream:**

```sh
nimbus --delete me@server:/var/www/site/ ./mirror
```

**Copy one large file and give it a new name:**

```sh
nimbus vm.qcow2 host:/images/vm-2026-11.qcow2
```

**Already-compressed data (video, archives):** skip compression to save CPU.

```sh
nimbus --compress none ./videos/ host:/media/videos
```

**Through a bastion host:**

```sh
nimbus -e "ssh -J bastion" ./data/ internal-host:/data
```

**Unattended, retrying for a long time on a flaky link:**

```sh
nimbus --plain --retries 50 --backoff-max 5m ./data/ host:/data
```

**Small staging area:** a smaller block and fewer jobs need less space.

```sh
nimbus -b 16M -j 2 --staging /mnt/scratch ./data/ host:/data
```
